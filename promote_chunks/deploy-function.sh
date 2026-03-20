#!/usr/bin/env bash

set -euxo pipefail

LOG_LEVEL=${LOG_LEVEL:-INFO}

# Deploy function into Python 3.13 environment with 15 minute timeout
gcloud functions deploy promote-chunks \
  --quiet \
  --gen2 \
  --region=us-central1 \
  --runtime=python313 \
  --source=. \
  --entry-point=promote_chunks \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="${SERVICE_ACCOUNT_EMAIL}" \
  --memory=4Gi \
  --timeout=900s \
  --set-env-vars "PPDB_CONFIG_URI=${PPDB_CONFIG_URI},PPDB_USE_SECRET_MANAGER=true"
