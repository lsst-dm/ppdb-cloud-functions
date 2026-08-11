#!/usr/bin/env bash

set -euo pipefail

REGION=${REGION:-us-central1}

# Function URL
FUNCTION_URL=$(gcloud functions describe promote-chunks \
  --gen2 --region=${REGION} \
  --format='value(serviceConfig.uri)')

# Delete the existing Cloud Scheduler job if it exists
gcloud scheduler jobs delete promote-chunks-daily \
  --quiet \
  --location=${REGION} || true

# Daily at 12:00 pm Chilean time
gcloud scheduler jobs create http promote-chunks-daily \
  --quiet \
  --location=${REGION} \
  --schedule="0 12 * * *" \
  --time-zone="${TIME_ZONE:-America/Santiago}" \
  --http-method=POST \
  --uri="${FUNCTION_URL}" \
  --oidc-service-account-email="cloudrun-promote-chunks@${PROJECT_ID}.iam.gserviceaccount.com" \
  --oidc-token-audience="${FUNCTION_URL}"
