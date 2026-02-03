#!/usr/bin/env bash

set -ux

if [ -z "${GCP_REGION:-}" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

gcloud run services delete promote-chunks \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT}" \
  --quiet

# Deleted scheduled run
gcloud scheduler jobs delete promote-chunks-daily \
  --quiet \
  --location="${GCP_REGION}"
