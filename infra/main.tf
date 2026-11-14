# Phase 1 infrastructure: one bucket, one least-privilege service account,
# one always-free e2-micro VM running two collectors under systemd:
#   basmy-collector          raw positions -> Cloud Storage (data platform)
#   basmy-supabase-collector realtime/collector.py -> Supabase (live map),
#                            replacing the throttled GitHub Actions cron

resource "google_project_service" "apis" {
  for_each           = toset(["compute.googleapis.com", "storage.googleapis.com", "iam.googleapis.com", "secretmanager.googleapis.com", "bigquery.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}

# --- bucket ----------------------------------------------------------------

resource "google_storage_bucket" "data" {
  name     = var.bucket_name
  location = upper(var.region) # single region: required for the free 5 GB
  # STANDARD class: Nearline/Coldline are not part of the free tier.
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Raw data is append-only and never deleted by the collector, so soft
  # delete would only add billed storage. Disable it.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  # Protect against an accidental `terraform destroy` wiping collected history.
  force_destroy = false

  depends_on = [google_project_service.apis]
}

# --- service account ---------------------------------------------------------

resource "google_service_account" "collector" {
  account_id   = "basmy-collector"
  display_name = "bas.my raw collector (create objects in the data bucket only)"
  depends_on   = [google_project_service.apis]
}

# objectCreator = storage.objects.create only. The collector cannot read,
# list, overwrite or delete anything, and has no project-level roles at all.
resource "google_storage_bucket_iam_member" "collector_create_only" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.collector.email}"
}

# --- Supabase connection string (for the live-map collector) -----------------
#
# Terraform creates only the empty secret. You add the value yourself, so the
# password never appears in Terraform state or in this repo:
#   gcloud secrets versions add basmy-supabase-database-url --data-file=-
# Always free: 6 active secret versions and 10,000 access operations a month;
# the VM reads it once per boot.

resource "google_secret_manager_secret" "supabase_database_url" {
  secret_id = "basmy-supabase-database-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

# Read access to this one secret only.
resource "google_secret_manager_secret_iam_member" "collector_reads_db_url" {
  secret_id = google_secret_manager_secret.supabase_database_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.collector.email}"
}

# --- VM --------------------------------------------------------------------

resource "google_compute_instance" "collector" {
  name         = var.vm_name
  machine_type = "e2-micro" # always free: one per billing account in the regions above
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12" # ships Python 3.11
      size  = 30                       # always free: 30 GB standard persistent disk
      type  = "pd-standard"
    }
  }

  network_interface {
    network = "default"
    # Ephemeral public IP so the VM can reach api.data.gov.my. Check current
    # pricing for in-use external IPv4 addresses before applying.
    access_config {}
  }

  service_account {
    email = google_service_account.collector.email
    # cloud-platform is needed for Secret Manager. Scopes are only an upper
    # bound: the IAM bindings above are what limit this account to creating
    # objects in one bucket and reading one secret. It has no project roles.
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  # The startup script lives in the metadata map (not metadata_startup_script),
  # so editing it later updates the VM in place instead of recreating it.
  metadata = {
    enable-oslogin = "TRUE"
    # replace() strips Windows line endings: a CRLF "#!/bin/bash" cannot run.
    startup-script = replace(templatefile("${path.module}/startup.sh.tftpl", {
      bucket   = google_storage_bucket.data.name
      repo_url = var.repo_url
      repo_ref = var.repo_ref
      secret   = google_secret_manager_secret.supabase_database_url.secret_id
    }), "\r", "")
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  allow_stopping_for_update = true

  depends_on = [
    google_storage_bucket_iam_member.collector_create_only,
    google_secret_manager_secret_iam_member.collector_reads_db_url,
  ]
}

# --- BigQuery (Phase 3 warehouse) ---------------------------------------------
#
# Real and synthetic data never share a dataset. *_raw holds the tables
# warehouse/load.py loads; the dataset without suffix holds the dbt models.
# Same location as the bucket (load jobs from gs:// require it). Always free:
# 10 GB storage and 1 TB of queries a month; load jobs are free.

locals {
  bq_datasets = {
    basmy_real          = "dbt models built from REAL collected data"
    basmy_real_raw      = "Raw tables loaded from REAL data (arrivals, GTFS, weather)"
    basmy_synthetic     = "dbt models built from SYNTHETIC test data - never report as real"
    basmy_synthetic_raw = "Raw tables loaded from SYNTHETIC test data - never report as real"
  }
}

resource "google_bigquery_dataset" "warehouse" {
  for_each                   = local.bq_datasets
  dataset_id                 = each.key
  description                = each.value
  location                   = var.region
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.apis]
}
