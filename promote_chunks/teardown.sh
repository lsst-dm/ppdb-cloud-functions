#!/usr/bin/env bash

set -uxo pipefail

if [ -z "${GCP_REGION:-}" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

# Delete the promote chunks function
gcloud functions delete promote-chunks \
  --gen2 \
  --region=$GCP_REGION

# Deleted scheduled run
gcloud scheduler jobs delete promote-chunks-daily \
  --location=us-central1
