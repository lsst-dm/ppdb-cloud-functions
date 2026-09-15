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
import datetime
import logging
import math
import posixpath
import uuid
from collections.abc import Iterator
from typing import Any

import apache_beam
import pyarrow.parquet
from apache_beam import PCollection
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.bigquery import (
    BigQueryDisposition,
    WriteResult,
    WriteToBigQuery,
)
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
)
from apache_beam.transforms.util import WaitOn
from google.cloud import bigquery
from google.cloud import logging as cloud_logging

# Configure Google Cloud logging.
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)
_LOG = logging.getLogger(__name__)

_STAGING_TABLE_TTL = datetime.timedelta(hours=1)

# table name, fully qualified staging table, fully qualified internal table
TableSpec = tuple[str, str, str]


class BeamSuppressUpdateDestinationSchemaWarning(logging.Filter):
    """Suppress the unhelpful Beam process-method iterator warning."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Return false for the warning that should be suppressed."""
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


def non_mask_columns(parquet_path: str) -> list[str]:
    """Return data columns, excluding masks and pandas index artifacts."""
    with FileSystems.open(parquet_path) as file_handle:
        schema = pyarrow.parquet.read_schema(file_handle)
    return [
        name
        for name in schema.names
        if not name.endswith(".mask") and not name.startswith("__index_level_")
    ]


def read_parquet(
    pipeline: apache_beam.Pipeline,
    bucket: str,
    object_prefix: str,
    table_name: str,
) -> PCollection:
    """Read one Parquet file from Google Cloud Storage."""
    parquet_path = (
        f"gs://{posixpath.join(bucket, object_prefix, f'{table_name}.parquet')}"
    )
    columns = non_mask_columns(parquet_path)
    log_event(
        logging.INFO,
        "Reading Parquet file",
        "reading_parquet_file",
        table_name=table_name,
        parquet_path=parquet_path,
    )
    return pipeline | f"Read{table_name}" >> ReadFromParquet(
        parquet_path,
        columns=columns,
    )


def sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Replace non-JSON-compliant NaN and infinity values with ``None``."""
    return {
        key: None if isinstance(value, float) and not math.isfinite(value) else value
        for key, value in row.items()
    }


def write_to_bigquery(
    pcoll: PCollection,
    table_name: str,
    table_fqn: str,
    temp_location: str,
) -> WriteResult:
    """Load a bounded PCollection into an existing BigQuery table."""
    log_event(
        logging.INFO,
        "Writing to BigQuery table",
        "writing_to_bigquery",
        table_name=table_name,
        table_fqn=table_fqn,
    )
    return (
        pcoll
        | f"Sanitize{table_name}" >> apache_beam.Map(sanitize_row)
        | f"Write{table_name}ToStaging"
        >> WriteToBigQuery(
            table=table_fqn,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location=temp_location,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )
    )


def create_staging_table(
    bq_client: bigquery.Client,
    staging_ref: str,
    internal_ref: str,
    table_name: str,
) -> None:
    """Create an expiring staging table with the internal table's schema."""
    log_event(
        logging.INFO,
        "Creating staging table",
        "creating_staging_table",
        table_name=table_name,
        staging_ref=staging_ref,
        internal_ref=internal_ref,
    )

    internal_table = bq_client.get_table(internal_ref)
    staging_table = bigquery.Table(staging_ref, schema=internal_table.schema)
    staging_table.expires = (
        datetime.datetime.now(datetime.timezone.utc) + _STAGING_TABLE_TTL
    )

    # A Beam DoFn may be retried after the table has already been created.
    bq_client.create_table(staging_table, exists_ok=True)

    log_event(
        logging.INFO,
        "Created staging table",
        "created_staging_table",
        table_name=table_name,
        staging_ref=staging_ref,
        expires=staging_table.expires.isoformat(),
    )


def copy_staging_to_internal(
    bq_client: bigquery.Client,
    staging_ref: str,
    internal_ref: str,
    table_name: str,
) -> None:
    """Copy a staging table over its target table in the internal dataset."""
    log_event(
        logging.INFO,
        "Copying staging table to internal table",
        "copying_staging_to_internal",
        table_name=table_name,
        staging_ref=staging_ref,
        internal_ref=internal_ref,
    )

    copy_job_config = bigquery.CopyJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq_client.copy_table(
        staging_ref,
        internal_ref,
        job_config=copy_job_config,
    )
    job.result()

    log_event(
        logging.INFO,
        "Copied staging table to internal table",
        "copied_staging_to_internal",
        table_name=table_name,
        internal_ref=internal_ref,
    )


class CreateStagingTable(apache_beam.DoFn):
    """Create staging tables on a Dataflow worker."""

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.bq_client: bigquery.Client | None = None

    def setup(self) -> None:
        """Create the BigQuery client once per worker instance."""
        self.bq_client = bigquery.Client(project=self.project_id)

    def process(self, table_spec: TableSpec) -> Iterator[str]:
        """Create one table and emit its name as a completion signal."""
        if self.bq_client is None:
            raise RuntimeError("BigQuery client was not initialized")

        table_name, staging_ref, internal_ref = table_spec
        create_staging_table(
            self.bq_client,
            staging_ref,
            internal_ref,
            table_name,
        )
        yield table_name


class PromoteTables(apache_beam.DoFn):
    """Copy all staging tables to their internal destinations."""

    def __init__(self, project_id: str, table_specs: list[TableSpec]) -> None:
        self.project_id = project_id
        self.table_specs = table_specs
        self.bq_client: bigquery.Client | None = None

    def setup(self) -> None:
        """Create the BigQuery client once per worker instance."""
        self.bq_client = bigquery.Client(project=self.project_id)

    def process(self, unused_element: None) -> Iterator[str]:
        """Promote every table after all staging loads have completed."""
        del unused_element
        if self.bq_client is None:
            raise RuntimeError("BigQuery client was not initialized")

        for table_name, staging_ref, internal_ref in self.table_specs:
            copy_staging_to_internal(
                self.bq_client,
                staging_ref,
                internal_ref,
                table_name,
            )

        yield "promoted"


def run(argv: list[str] | None = None) -> None:
    """Build and submit the SSO staging-and-promotion pipeline."""
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)

    gcp_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    temp_location = gcp_options.temp_location
    if not temp_location:
        raise ValueError("GCP temp_location must be set in pipeline options.")

    project_id = gcp_options.project
    if not project_id:
        raise ValueError("GCP project must be set in pipeline options.")

    staging_dataset_id = custom_options.staging_dataset_id
    internal_dataset_id = custom_options.internal_dataset_id
    bucket = custom_options.bucket
    object_prefix = custom_options.object_prefix
    tables = [name.strip() for name in custom_options.tables.split(",") if name.strip()]
    if not tables:
        raise ValueError("At least one table must be supplied with --tables.")

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

    token = uuid.uuid4().hex
    table_specs: list[TableSpec] = [
        (
            table_name,
            f"{project_id}.{staging_dataset_id}.{table_name}_{token}",
            f"{project_id}.{internal_dataset_id}.{table_name}",
        )
        for table_name in tables
    ]

    try:
        with apache_beam.Pipeline(options=options) as pipeline:
            staging_tables_ready = (
                pipeline
                | "CreateStagingTableSpecs" >> apache_beam.Create(table_specs)
                | "CreateStagingTables"
                >> apache_beam.ParDo(CreateStagingTable(project_id))
            )

            write_signals: list[PCollection] = []

            for table_name, staging_ref, _ in table_specs:
                data = read_parquet(
                    pipeline,
                    bucket,
                    object_prefix,
                    table_name,
                )
                data_after_table_creation = data | (
                    f"WaitFor{table_name}StagingTable"
                    >> WaitOn(staging_tables_ready)
                )

                write_result = write_to_bigquery(
                    data_after_table_creation,
                    table_name,
                    staging_ref.replace(".", ":", 1),
                    temp_location,
                )

                # FILE_LOADS can use both load jobs and follow-up copy jobs.
                # Waiting for both output collections covers either path.
                write_signals.extend(
                    [
                        write_result.destination_load_jobid_pairs,
                        write_result.destination_copy_jobid_pairs,
                    ]
                )

            (
                pipeline
                | "CreatePromotionTrigger" >> apache_beam.Create([None])
                | "WaitForAllStagingLoads" >> WaitOn(*write_signals)
                | "PromoteStagingTables"
                >> apache_beam.ParDo(PromoteTables(project_id, table_specs))
            )
    except Exception as error:
        log_event(
            logging.ERROR,
            "Error building or submitting the SSO table pipeline",
            "load_sso_tables_submission_failed",
            error=str(error),
        )
        raise


if __name__ == "__main__":
    run()
