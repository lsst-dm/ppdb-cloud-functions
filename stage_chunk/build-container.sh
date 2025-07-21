#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is unset or empty. Please set it to your service account key file."
  exit 1
fi

if [ -z "${GCP_PROJECT:-}" ]; then
  echo "GCP_PROJECT is unset or empty. Please set it to your Google Cloud project ID."
  exit 1
fi

# Build the Docker image
gcloud builds submit \
  --config=cloudbuild.yaml \
  --region=us-central1 \
  --service-account="projects/${GCP_PROJECT}/serviceAccounts/${SERVICE_ACCOUNT_EMAIL}"

# Verify the image was built and pushed to Artifact Registry
gcloud artifacts docker images list us-central1-docker.pkg.dev/${GCP_PROJECT}/ppdb-docker-repo
