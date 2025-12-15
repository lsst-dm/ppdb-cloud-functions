#!/usr/bin/env bash

set -euxo pipefail

# Deploy the Cloud Function
gcloud functions deploy track-chunk \
  --runtime=python311 \
  --region=${GCP_REGION} \
  --source=. \
  --entry-point=track_chunk \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --trigger-topic=track-chunk-topic \
  --set-env-vars "PROJECT_ID=${GCP_PROJECT},DB_HOST=${PPDB_DB_HOST_EXTERNAL},DB_USER=${PPDB_DB_USER},DB_NAME=${PPDB_DB_NAME},DB_SCHEMA=${PPDB_SCHEMA_NAME}" \
  --gen2

#  --vpc-connector=ppdb-vpc-connector \
#  --egress-settings=all \
