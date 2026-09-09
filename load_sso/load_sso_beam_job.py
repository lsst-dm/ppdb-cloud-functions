# This file is part of ppdb-cloud-functions.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse
import logging
import posixpath
import uuid
from typing import Any

import apache_beam
from apache_beam import PCollection
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
)
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud import logging as cloud_logging

# Configure Google Cloud logging
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)
_LOG = logging.getLogger(__name__)


class BeamSuppressUpdateDestinationSchemaWarning(logging.Filter):
    """Suppresses the UpdateDestinationSchema warning from Apache Beam."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Suppress the UpdateDestinationSchema warning."""
        if record.name == "apache_beam.transforms.core":
            message = str(record.getMessage())
            if "No iterator is returned by the process method" in message:
                return False
        return True


logging.getLogger("apache_beam.transforms.core").addFilter(
    BeamSuppressUpdateDestinationSchemaWarning()
)


def log_event(level: int, message: str, event_name: str, **fields: Any) -> None:
    """Emit a structured log entry under Cloud Logging ``json_fields``."""
    _LOG.log(level, message, extra={"json_fields": {"event": event_name, **fields}})


class CustomOptions(PipelineOptions):
    """Custom options for the pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        """Add custom arguments to the parser."""
        parser.add_argument(
            "--bucket",
            required=True,
            help="GCS bucket containing the SSO Parquet files",
        )
        parser.add_argument(
            "--object_prefix",
            required=True,
            help="GCS object prefix containing the SSO Parquet files",
        )
        parser.add_argument(
            "--tables",
            required=True,
            help="Comma-separated list of SSO table names to load",
        )
        parser.add_argument(
            "--staging_dataset_id",
            required=True,
            help="BigQuery dataset ID for staging tables",
        )
        parser.add_argument(
            "--internal_dataset_id",
            required=True,
            help="BigQuery dataset ID for internal tables",
        )


def read_parquet(
    pipeline: apache_beam.Pipeline, bucket: str, object_prefix: str, table_name: str
) -> PCollection:
    """Read a Parquet file from Google Cloud Storage."""
    parquet_path = (
        f"gs://{posixpath.join(bucket, object_prefix, f'{table_name}.parquet')}"
    )
    log_event(
        logging.INFO,
        "Reading Parquet file",
        "reading_parquet_file",
        table_name=table_name,
        parquet_path=parquet_path,
    )
    return pipeline | f"Read{table_name}" >> ReadFromParquet(parquet_path)


def write_to_bigquery(
    pcoll: apache_beam.PCollection,
    table_fqn: str,
    temp_location: str,
) -> PCollection:
    """Write PCollection to a target BigQuery table."""
    log_event(
        logging.INFO,
        "Writing to BigQuery table",
        "writing_to_bigquery",
        table_fqn=table_fqn,
    )
    return pcoll | f"Write{table_fqn}" >> WriteToBigQuery(
        table=table_fqn,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        custom_gcs_temp_location=temp_location,
    )


def swap_internal_table(
    bq_client: bigquery.Client, staging_ref: str, internal_ref: str, table_name: str
) -> None:
    """Copy a staging table over its target table in the internal dataset."""
    log_event(
        logging.INFO,
        "Swapping staging table into internal dataset",
        "swapping_staging_to_internal",
        table_name=table_name,
        staging_ref=staging_ref,
        internal_ref=internal_ref,
    )

    # This job should fail if the schemas of the staging and internal tables
    # don't match.
    copy_job_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq_client.copy_table(staging_ref, internal_ref, job_config=copy_job_config)
    job.result()

    log_event(
        logging.INFO,
        "Swapped staging table into internal dataset",
        "swapped_staging_to_internal",
        table_name=table_name,
        internal_ref=internal_ref,
    )


def delete_staging_table(
    bq_client: bigquery.Client, staging_ref: str, table_name: str
) -> None:
    """Delete a staging table, logging (rather than raising) on failure."""
    try:
        bq_client.delete_table(staging_ref, not_found_ok=True)
    except GoogleAPICallError as e:
        log_event(
            logging.ERROR,
            "Failed to delete staging table",
            "delete_staging_table_failed",
            table_name=table_name,
            staging_ref=staging_ref,
            error=str(e),
        )
        return

    log_event(
        logging.INFO,
        "Deleted staging table",
        "deleted_staging_table",
        table_name=table_name,
        staging_ref=staging_ref,
    )


def run(argv: list[str] | None = None) -> None:
    """Run the pipeline."""
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)

    gcp_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    temp_location = gcp_options.temp_location
    if not temp_location:
        raise ValueError("GCP temp_location must be set in pipeline options.")

    project_id = gcp_options.project
    staging_dataset_id = custom_options.staging_dataset_id
    internal_dataset_id = custom_options.internal_dataset_id
    bucket = custom_options.bucket
    object_prefix = custom_options.object_prefix
    tables = custom_options.tables.split(",")

    log_event(
        logging.INFO,
        "Loading SSO tables",
        "load_sso_tables_started",
        tables=tables,
        bucket=bucket,
        object_prefix=object_prefix,
        staging_dataset_id=staging_dataset_id,
        internal_dataset_id=internal_dataset_id,
    )

    # Generate a unique suffix for each temporary staging table to avoid name
    # collisions.
    token = uuid.uuid4().hex
    staging_table_names = {table_name: f"{table_name}_{token}" for table_name in tables}

    # Write the data from each parquet file to its temporary staging table.
    with apache_beam.Pipeline(options=options) as pipeline:
        for table_name in tables:
            data = read_parquet(pipeline, bucket, object_prefix, table_name)

            staging_table_fqn = (
                f"{project_id}:{staging_dataset_id}.{staging_table_names[table_name]}"
            )

            write_to_bigquery(
                data,
                staging_table_fqn,
                temp_location,
            )

    bq_client = bigquery.Client(project=project_id)
    try:
        # Copy each temporary staging table to the internal table in BigQuery.
        for table_name in tables:
            staging_ref = (
                f"{project_id}.{staging_dataset_id}.{staging_table_names[table_name]}"
            )
            internal_ref = f"{project_id}.{internal_dataset_id}.{table_name}"
            swap_internal_table(bq_client, staging_ref, internal_ref, table_name)
    finally:
        # Delete each temporary staging table in BigQuery.
        for table_name in tables:
            staging_ref = (
                f"{project_id}.{staging_dataset_id}.{staging_table_names[table_name]}"
            )
            delete_staging_table(bq_client, staging_ref, table_name)


if __name__ == "__main__":
    run()
