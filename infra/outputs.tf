output "bucket" {
  value = google_storage_bucket.data.name
}

output "raw_prefix" {
  value = "gs://${google_storage_bucket.data.name}/raw/vehicle_positions/"
}

output "service_account" {
  value = google_service_account.collector.email
}

output "vm_name" {
  value = google_compute_instance.collector.name
}

output "ssh" {
  value = "gcloud compute ssh ${google_compute_instance.collector.name} --zone ${var.zone} --project ${var.project_id}"
}
