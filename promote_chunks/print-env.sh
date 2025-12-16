#!/bin/bash

set -euxo pipefail

gcloud functions describe promote-chunks \
  --gen2 --region="$GCP_REGION" --format='json(serviceConfig.environmentVariables)'
