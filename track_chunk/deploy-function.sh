#!/usr/bin/env bash

set -euxo pipefail

# Deploy the Cloud Function
gcloud functions deploy track-chunk \
  --runtime=python313 \
  --region=${GCP_REGION} \
  --source=. \
  --entry-point=track_chunk \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --trigger-topic=track-chunk-topic \
  --set-env-vars "PPDB_CONFIG_URI=${PPDB_CONFIG_URI},PPDB_USE_SECRET_MANAGER=true" \
  --gen2 \
  --memory=4Gi \
  --quiet
