# PPDB Cloud Functions

This repository contains [Google Cloud Run](https://cloud.google.com/functions) functions for the Rubin Observatory's Prompt Products Database (PPDB) along with related scripts and configuration files.

## GitHub Actions deployments

To deploy all workloads, select **Dev - Deploy All Workloads**, **Int - Deploy All Workloads**, or **Prod - Deploy All Workloads** in the Actions tab and choose **Run workflow** with the desired branch or tag.

Each workflow runs all of the workload deployment jobs in parallel, subject to runner availability and the existing GitHub environment approval rules. A failed deployment does not cancel the other deployments; check every job before retrying any failed workloads individually.

The [stage_chunk](./stage_chunk) function stages a replica chunk in BigQuery when a message for it arrives on the `stage-chunk-topic` Pub/Sub topic. It uses `ChunkStager` from [dax_ppdb](https://github.com/lsst/dax_ppdb), which loads the chunk's Parquet files from Google Cloud Storage (GCS) with BigQuery load jobs and copies them into the staging tables in one transaction.
