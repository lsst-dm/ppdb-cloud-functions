#!/usr/bin/env bash
set -euxo pipefail

FUNC="promote-chunks"

# Get the function's URL
URL="$(gcloud functions describe "$FUNC" --gen2 --region="$GCP_REGION" \
  --format='value(serviceConfig.uri)')"

# Set the environment variable for the dev token
TOKEN="$(gcloud auth print-identity-token --audiences="$URL")"

# Invoke the function with the dev token
curl -X POST "$URL" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" 
