# Phase 1 infrastructure: one bucket, one least-privilege service account,
# one always-free e2-micro VM running the collector under systemd.

resource "google_project_service" "apis" {
  for_each           = toset(["compute.googleapis.com", "storage.googleapis.com", "iam.googleapis.com"])
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
    # The scope just allows Cloud Storage API calls; the IAM binding above is
    # what actually limits the account to creating objects in one bucket.
    scopes = ["https://www.googleapis.com/auth/devstorage.read_write"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh.tftpl", {
    bucket   = google_storage_bucket.data.name
    repo_url = var.repo_url
    repo_ref = var.repo_ref
  })

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  allow_stopping_for_update = true

  depends_on = [google_storage_bucket_iam_member.collector_create_only]
}
