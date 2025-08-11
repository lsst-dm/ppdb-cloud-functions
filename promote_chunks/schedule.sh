#!/usr/bin/env bash

# Function URL
FUNCTION_URL=$(gcloud functions describe promote-chunks \
  --gen2 --region=us-central1 \
  --format='value(serviceConfig.uri)')

# Daily at 12:00 Chile time
gcloud scheduler jobs create http promote-chunks-daily \
  --location=us-central1 \
  --schedule="0 12 * * *" \
  --time-zone="America/Santiago" \
  --http-method=POST \
  --uri="${FUNCTION_URL}" \
  --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
  --oidc-token-audience="${FUNCTION_URL}"
