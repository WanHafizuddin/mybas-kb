terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # State is local (infra/terraform.tfstate) and gitignored. Never commit it.
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}
