terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.35.0"
    }
  }
}

provider "google" {
  project = "ny-taxi-data-engr"
  region  = "us-central1"
  credentials = "../google_cred/google-credentials.json"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "ny-taxi-data-engr-demo-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
} 

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = "demo_id"
  project = "ny-taxi-data-engr"
  location = "US"
}