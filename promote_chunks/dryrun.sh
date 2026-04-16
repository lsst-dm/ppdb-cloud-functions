#!/usr/bin/env bash
set -euxo pipefail

URL=$(gcloud functions describe promote-chunks \
  --region=us-central1 \
  --gen2 \
  --format="value(serviceConfig.uri)")

echo "Calling promote-chunks function at: $URL"

curl -X POST "$URL?dry_run=true" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"
