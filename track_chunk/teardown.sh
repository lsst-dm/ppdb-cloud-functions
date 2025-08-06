#!/usr/bin/env bash

set -uxo pipefail

# Delete Cloud Function
gcloud functions delete track_chunk --region=${GCP_REGION} --quiet

echo "Teardown complete."
