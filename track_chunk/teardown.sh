#!/usr/bin/env bash

set -ux

# Delete Cloud Function
gcloud functions delete track-chunk --region=${GCP_REGION} --quiet

echo "Teardown complete."
