terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      # Version 6.34.0 or greater is required for manual scaling on a Cloud Run
      # service support.
      version = ">= 6.34.0"
    } 
  }
}

# Input variables.
variable "cloud_run_project_id" {
  type = string
}
variable "region" {
  type = string
}
variable "artifact_registry_repo" {
  type = string
}
variable "pubsub_subscription_project_id" {
  type = string
}
variable "pubsub_subscription_id" {
  type = string
}

provider "google" {
  region = var.region
  project = var.cloud_run_project_id
}

data "google_project" "cloud_run_project" {
  project_id = var.cloud_run_project_id
}

# Service Accounts
#
# One service account for each Cloud Run deployment:
# - Worker SA
# - Scaler SA
resource "google_service_account" "worker_sa" {
  account_id = "worker"
  display_name = "Worker Service Account"
}

resource "google_service_account" "scaler_sa" {
  account_id = "scaler"
  display_name = "Scaler Service Account"
}

# Pub/Sub resources

# Assume the Pub/Sub subscription already exists.
data "google_pubsub_subscription" "subscription" {
  name = var.pubsub_subscription_id
  project = var.pubsub_subscription_project_id
}

# Pub/Sub IAM policies
#
# Allow the Worker SA to subscribe to the topic.
resource "google_pubsub_subscription_iam_member" "subscription_policy" {
  subscription = data.google_pubsub_subscription.subscription.name
  role = "roles/pubsub.subscriber"
  member = "serviceAccount:${google_service_account.worker_sa.email}"
}

# Cloud Run resources

# Scaler Cloud Run service.
#
# Uses the Scaler image. Sets flags to configure the Scaler options.
resource "google_cloud_run_v2_service" "scaler_service" {
  name = "scaler"
  location = var.region

  template {
    containers {
      image = "${var.artifact_registry_repo}/scaler"
      args = [
        "--target_utilization=0.8",
        "--min_instances=1",
        "--resource_name=projects/${var.cloud_run_project_id}/locations/${var.region}/services/worker",
        "--cycle_frequency=10s"
      ]
      resources {
        cpu_idle = false
      }
    }
    service_account = google_service_account.scaler_sa.email
    max_instance_request_concurrency = 1000
    scaling {
      # Set max instances to 1 to ensure all worker load report requests go to
      # the same instance.
      max_instance_count = 1
    }
  }
}

# Scaler Invoker IAM policy.
#
# Allow the Worker SA to invoke the Scaler service.
data "google_iam_policy" "scaler_policy_config" {
  binding {
    role = "roles/run.invoker"
    members = [
      "serviceAccount:${google_service_account.worker_sa.email}",
    ]
  }
}

resource "google_cloud_run_v2_service_iam_policy" "scaler_policy" {
  location = var.region
  name = google_cloud_run_v2_service.scaler_service.name
  policy_data = data.google_iam_policy.scaler_policy_config.policy_data
}

# Worker Cloud Run service.
#
# Uses the Worker image. Sets flags to configure the Worker options.
resource "google_cloud_run_v2_service" "worker_service" {
  name = "worker"
  location = var.region
  launch_stage = "BETA"

  template {
    containers {
      name = "puller"
      image = "${var.artifact_registry_repo}/worker"
      args = [
        "--subscription_project_id=${var.pubsub_subscription_project_id}",
        "--subscription_id=${var.pubsub_subscription_id}",
        "--reporting_url=https://scaler-${data.google_project.cloud_run_project.number}.${var.region}.run.app/reportLoad",
        "--reporting_audience=https://scaler-${data.google_project.cloud_run_project.number}.${var.region}.run.app/",
        # Set to configure the maximum number of messages a single worker will pull
        # from Pub/Sub at once.
        "--max_outstanding_messages=20",
        # The duration over which average active messages is averaged. A higher value
        # gives more stable scaling. A lower value gives more responsive scaling.
        "--metric_window_length=10s",
        "--local_push_url=http://localhost:8080",
      ]
      resources {
        cpu_idle = false
      }
    }
    containers {
      name = "consumer"
      image = "${var.artifact_registry_repo}/exampleconsumer"
      args = [
        "--sleep_duration=1s",
        "--nack_percent=0",
        "-v=3",
        "--logtostderr",
      ]
      ports {
        container_port = 8080
      }
      resources {
        cpu_idle = false
      }
    }
    service_account = google_service_account.worker_sa.email
  }

  scaling {
    scaling_mode = "MANUAL"
    manual_instance_count = 1
  }
}

# Worker IAM policies.
#
# Allow the Scaler SA to act as the Worker SA to pass deployment permission
# checks to allow updating the Worker's instance count.
data "google_iam_policy" "worker_sa_policy_config" {
  binding {
    role = "roles/iam.serviceAccountUser"
    members = [
      "serviceAccount:${google_service_account.scaler_sa.email}",
    ]
  }
}

resource "google_service_account_iam_policy" "worker_sa_policy" {
  service_account_id = google_service_account.worker_sa.name
  policy_data = data.google_iam_policy.worker_sa_policy_config.policy_data
}

# roles/run.admin for the Scaler SA. Must be set at the project level because
# the Operations API for checking operation status does not support IAM
# permissions at the service level.
resource "google_project_iam_member" "scaler_run_admin" {
  project = var.cloud_run_project_id
  role = "roles/run.admin"
  member = "serviceAccount:${google_service_account.scaler_sa.email}"
}

