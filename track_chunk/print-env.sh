#!/bin/bash

set -euxo pipefail

gcloud functions describe track-chunk \
  --gen2 --region="$GCP_REGION" --format='json(serviceConfig.environmentVariables)'
