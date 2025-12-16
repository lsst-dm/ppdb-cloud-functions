#!/bin/bash

set -euxo pipefail

gcloud functions describe trigger-stage-chunk \
  --quiet \
  --gen2 \
  --region="$GCP_REGION" \
  --format='json(serviceConfig.environmentVariables)'
