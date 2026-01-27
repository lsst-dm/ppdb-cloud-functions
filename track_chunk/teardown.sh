#!/usr/bin/env bash

set -euxo pipefail

# Delete Cloud Function
gcloud run services delete track-chunk \
  --region="${GCP_REGION}" \
  --project="${GCP_PROJECT}" \
  --quiet
