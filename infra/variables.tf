variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Must be us-central1, us-east1 or us-west1 for the always-free e2-micro and 5 GB of regional Standard storage."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "Free tier only covers us-central1, us-east1 and us-west1."
  }
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "bucket_name" {
  description = "Globally unique bucket name for the raw/processed/models data."
  type        = string
}

variable "repo_url" {
  description = "HTTPS git URL the VM clones the collector from. Must be readable without credentials (public repo)."
  type        = string
  default     = "https://github.com/WanHafizuddin/KB-MyBasTracker-SimulatedLive.git"
}

variable "repo_ref" {
  description = "Branch or tag the VM checks out."
  type        = string
  default     = "main"
}

variable "vm_name" {
  type    = string
  default = "basmy-collector"
}
