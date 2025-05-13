# PPDB Cloud Functions

This repository contains [Google Cloud Run](https://cloud.google.com/functions) functions for the Rubin Observatory's Prompt Products Database (PPDB) along with related scripts and configuration files.

There is currently a single function for triggering a [Dataflow](https://cloud.google.com/products/dataflow) job which loads table data into BigQuery from Parquet files in Google Cloud Storage (GCS). The implementation of this function is contained in the [stage_chunk](./stage_chunk) directory with the following files:

- `build-container.sh` - Builds the Docker container for the Dataflow job
- `build-flex-template.sh` - Deploys the [flex template](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) for the Dataflow job
- `deploy-function.sh` - Deploys the Cloud Function to listen for events on the `stage-chunk-topic` Pub/Sub topic
- `Dockerfile` - Dockerfile for the Dataflow job which launches the [Apache Beam](https://beam.apache.org/) script
- `main.py` - Implementation of the Cloud Function which triggers the Dataflow job. The function accepts the name of a GCS bucket and prefix containing the Parquet files for a replica chunk, e.g., `gs://rubin-ppdb-test-bucket-1/data/tmp/2025/04/23/1737056400`.
- `Makefile` - Makefile with helpful targets for deploying and tearing down the Cloud Function. Typing `make` will print all available targets.
- `metadata.json` - Required metadata for the Dataflow job
- `requirements.txt` - Python dependencies for the Dataflow job
- `stage_chunk_beam_job.py` - [Apache Beam](https://beam.apache.org/) script for loading the data into BigQuery from Parquet files in GCS.
- `teardown.sh` - Script to teardown the Cloud Function and Dataflow configuration, including deleting the Docker image, removing the flex template, etc.
