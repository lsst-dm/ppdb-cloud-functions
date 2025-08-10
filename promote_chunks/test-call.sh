#!/usr/bin/env bash
set -euxo pipefail

FUNC="promote-chunks"

# 1. Get the function's URL
URL="$(gcloud functions describe "$FUNC" --gen2 --region="$GCP_REGION" \
  --format='value(serviceConfig.uri)')"

# 2. Login as the SA
gcloud config set account ${SERVICE_ACCOUNT_EMAIL}

# 3. Set the environment variable for the dev token
TOKEN="$(gcloud auth print-identity-token --audiences="$URL")"

# 4. Invoke the function with the dev token
curl -X POST "$URL" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" 

# 5. Restore the user account after testing
gcloud config set account ${USER_EMAIL}
