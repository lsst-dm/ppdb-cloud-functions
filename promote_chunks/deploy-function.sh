#!/usr/bin/env bash

set -euxo pipefail

LOG_LEVEL=${LOG_LEVEL:-INFO}

gcloud functions deploy promote-chunks \
  --gen2 \
  --region=us-central1 \
  --runtime=python311 \
  --source=. \
  --entry-point=promote_chunks \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="${SERVICE_ACCOUNT_EMAIL}" \
  --memory=4Gi \
  --timeout=900s \
  --vpc-connector=ppdb-vpc-connector \
  --egress-settings=all \
  --set-env-vars "REGION=${GCP_REGION},PROJECT_ID=${GCP_PROJECT},DATASET_ID=${DATASET_ID},DB_HOST=${PPDB_DB_HOST_INTERNAL},DB_USER=${PPDB_DB_USER},DB_NAME=${PPDB_DB_NAME},DB_SCHEMA=${PPDB_SCHEMA_NAME},LOG_LEVEL=${LOG_LEVEL}"
