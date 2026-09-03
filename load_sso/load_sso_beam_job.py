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
from google.cloud import logging as cloud_logging

# Configure Google Cloud logging
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)
_LOG = logging.getLogger(__name__)


class BeamSuppressUpdateDestinationSchemaWarning(logging.Filter):
    """Suppresses the UpdateDestinationSchema warning from Apache Beam."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Suppress the UpdateDestinationSchema warning.

        Parameters
        ----------
        record : `logging.LogRecord`
            The log record to filter.
        """
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
        """Add custom arguments to the parser.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            The argument parser to add arguments to.
        """
        parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID")
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


def read_parquet(
    pipeline: apache_beam.Pipeline, bucket: str, object_prefix: str, table_name: str
) -> PCollection:
    """Read a Parquet file from GCS.

    Parameters
    ----------
    pipeline : `apache_beam.Pipeline`
        The Apache Beam pipeline.
    bucket : `str`
        The GCS bucket containing the Parquet file.
    object_prefix : `str`
        The GCS object prefix containing the Parquet file.
    table_name : `str`
        The name of the SSO table to read.

    Returns
    -------
    transform: `apache_beam.PTransform`
        The transform to read the Parquet file.
    """
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
    """Write PCollection to BigQuery.

    Parameters
    ----------
    pcoll : `apache_beam.PCollection`
        The PCollection to write to BigQuery.
    table_fqn : `str`
        The fully qualified name of the BigQuery table in the format `project_id:dataset_id.table_name`.
    temp_location : `str`
        The GCS path for temporary files.

    Returns
    -------
    transform: `apache_beam.PTransform`
        The transform to write the PCollection to BigQuery.
    """
    log_event(
        logging.INFO,
        "Writing to BigQuery table",
        "writing_to_bigquery",
        table_fqn=table_fqn,
    )
    # SSO tables are fully replaced on every run, not incrementally appended.
    return pcoll | f"Write{table_fqn}" >> WriteToBigQuery(
        table=table_fqn,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        custom_gcs_temp_location=temp_location,
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

    dataset_id = custom_options.dataset_id
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
        dataset_id=dataset_id,
    )

    if ":" in dataset_id:
        project_id, dataset_id = dataset_id.split(":", 1)
    else:
        project_id = gcp_options.project

    with apache_beam.Pipeline(options=options) as pipeline:
        for table_name in tables:
            data = read_parquet(pipeline, bucket, object_prefix, table_name)

            table_fqn = f"{project_id}:{dataset_id}.{table_name}"

            write_to_bigquery(
                data,
                table_fqn,
                temp_location,
            )


if __name__ == "__main__":
    run()
