#!/usr/bin/env bash

set -euxo pipefail

# Set Dataflow template path and temp location by convention
DATAFLOW_TEMPLATE_PATH="gs://${GCS_BUCKET}/templates/stage_chunk_flex_template.json"
TEMP_LOCATION="gs://${GCS_BUCKET}/dataflow/temp"

# Set the topic name for updating chunk statuses
TOPIC_NAME="track-chunk-topic"

LOG_LEVEL=${LOG_LEVEL:-DEBUG}

# Deploy the Cloud Function
gcloud functions deploy trigger_stage_chunk \
  --runtime=python311 \
  --region=${GCP_REGION} \
  --source=. \
  --entry-point=trigger_stage_chunk \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --trigger-topic=stage-chunk-topic \
  --set-env-vars "PROJECT_ID=${GCP_PROJECT},REGION=${GCP_REGION},SERVICE_ACCOUNT_EMAIL=${SERVICE_ACCOUNT_EMAIL},TEMP_LOCATION=${TEMP_LOCATION},DATAFLOW_TEMPLATE_PATH=${DATAFLOW_TEMPLATE_PATH},TOPIC_NAME=${TOPIC_NAME},LOG_LEVEL=${LOG_LEVEL}" \
  --gen2
