#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${GCP_PROJECT:-}" ]; then
  echo "GCP_PROJECT is unset or empty. Please set it to your Google Cloud project ID."
  exit 1
fi

if [ -z "${GCS_BUCKET:-}" ]; then
  echo "GCS_BUCKET is unset or empty. Please set it to your Google Cloud Storage bucket name."
  exit 1
fi

echo "Creating Dataflow Flex Template..."
gcloud dataflow flex-template build "gs://${GCS_BUCKET}/templates/stage_chunk_flex_template.json" \
  --image "us-central1-docker.pkg.dev/${GCP_PROJECT}/ppdb-docker-repo/stage-chunk-image:latest" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json"

echo "Flex Template created and pushed to gs://${GCS_BUCKET}/templates/stage_chunk_flex_template.json"
