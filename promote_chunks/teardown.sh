#!/usr/bin/env bash

set -uxo pipefail

if [ -z "${GCP_REGION:-}" ]; then
  echo "REGION is not set. Please set it to your Google Cloud region."
  exit 1
fi

gcloud functions delete promote-chunks \
  --gen2 \
  --region=$GCP_REGION
