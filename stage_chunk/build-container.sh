#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${GCP_PROJECT:-}" ]; then
  echo "GCP_PROJECT is unset or empty. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "${SERVICE_ACCOUNT_EMAIL:-}" ]; then
  echo "SERVICE_ACCOUNT_EMAIL is unset or empty. Please set it to the service account email."
  exit 1
fi

if [ -z "${GCP_REGION:-}" ]; then
  GCP_REGION="us-central1"
  echo "GCP_REGION is unset or empty. Defaulting to ${GCP_REGION}."
fi

# Build the Docker image
gcloud builds submit \
  --config=cloudbuild.yaml \
  --region="${GCP_REGION}" \
  --service-account="projects/${GCP_PROJECT}/serviceAccounts/${SERVICE_ACCOUNT_EMAIL}"

# Verify the image was built and pushed to Artifact Registry
gcloud artifacts docker images list "${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/ppdb-docker-repo"
