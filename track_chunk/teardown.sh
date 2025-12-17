#!/usr/bin/env bash

set -ux

# Delete Cloud Function
gcloud functions delete track-chunk \
  --quiet \
  --region=${GCP_REGION}

echo "Teardown complete."
