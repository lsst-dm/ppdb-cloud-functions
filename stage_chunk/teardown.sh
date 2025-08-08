#!/usr/bin/env bash

set -uxo pipefail

if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set. Please set it to your service account key file."
  exit 1
fi

if [ -z "${GCP_PROJECT:-}" ]; then
  echo "GCP_PROJECT is not set. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "${GCP_REGION:-}" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

if [ -z "${GCS_BUCKET:-}" ]; then
  echo "GCS_BUCKET is not set. Please set it to your Google Cloud Storage bucket name."
  exit 1
fi

echo "Teardown started..."
echo "Project ID: ${GCP_PROJECT}"
echo "Bucket: ${GCS_BUCKET}"
echo "Region: ${GCP_REGION}"

# Delete Cloud Function
gcloud functions delete trigger_stage_chunk --region=${RGCP_REGION} --quiet

# Delete Flex Template JSON
gsutil rm -f gs://${GCS_BUCKET}/templates/stage_chunk_flex_template.json

# Delete Container Image (optional)
gcloud artifacts docker images delete "us-central1-docker.pkg.dev/${GCP_PROJECT}/ppdb-docker-repo/stage-chunk-image:latest" --quiet

echo "Teardown complete."
