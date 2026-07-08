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
import json
import logging
import posixpath
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

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
from google.cloud import pubsub_v1, storage
from lsst.dax.ppdb.bigquery import Manifest
from lsst.dax.ppdb.bigquery.updates import UpdateRecords

# Configure Google Cloud logging
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)


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
            "--folder",
            required=True,
            help="GCS folder containing Parquet files",
        )
        parser.add_argument(
            "--chunk_id",
            required=True,
            help="ID of the chunk being processed",
        )
        parser.add_argument(
            "--topic_name",
            required=True,
            help="Name of the Pub/Sub topic to publish chunk status updates",
        )


def read_parquet(pipeline: apache_beam.Pipeline, folder: str, name: str) -> PCollection:
    """Read Parquet files from GCS.

    Parameters
    ----------
    pipeline : `apache_beam.Pipeline`
        The Apache Beam pipeline.
    folder : `str`
        The GCS folder containing Parquet files.
    name : `str`
        The name of the Parquet file to read (without extension).

    Returns
    -------
    transform: `apache_beam.PTransform`
        The transform to read the Parquet file.
    """
    parquet_path = posixpath.join(folder.rstrip("/"), name.lstrip("/"))
    logging.info("Reading Parquet file: %s", parquet_path)
    return pipeline | f"Read{name}" >> ReadFromParquet(f"{parquet_path}")


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
    logging.info("Writing to BigQuery table %s", table_fqn)
    return pcoll | f"Write{table_fqn}" >> WriteToBigQuery(
        table=table_fqn,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location=temp_location,
    )


def parse_folder(folder: str) -> tuple[str, str]:
    """Parse the input URL to extract the bucket name and object path.

    Parameters
    ----------
    folder : `str`
        The GCS URL to the prefix containing the manifest.json file.

    Returns
    -------
    bucket_and_prefix: tuple[str, str]
        A tuple containing the bucket name and object prefix.
    """
    parsed_url = urlparse(folder)
    if parsed_url.scheme != "gs":
        raise ValueError("Folder must start with 'gs://'")
    bucket_name = parsed_url.netloc
    object_path = parsed_url.path.strip("/")
    if not bucket_name or not object_path:
        raise ValueError(f"Invalid GCS folder: {folder}")
    return bucket_name, object_path


def read_manifest(chunk_id: int, folder: str) -> Manifest:
    """Read the chunk's manifest from its GCS location.

    Parameters
    ----------
    chunk_id : `int`
        The ID of the chunk being processed.
    folder : `str`
        The GCS prefix containing the manifest file.

    Returns
    -------
    manifest: `Manifest`
        The contents of the manifest.json file as a Manifest object.
    """
    # Parse the bucket name and object path from the folder.
    bucket_name, object_prefix = parse_folder(folder)

    # Initialize the GCS client.
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    manifest_path = posixpath.join(object_prefix, Manifest.FILE_NAME)
    logging.info(f"Reading manifest from: gs://{bucket_name}/{manifest_path}")

    # Download the manifest file and return its data.
    blob = bucket.blob(manifest_path)
    manifest_content = blob.download_as_text()
    return Manifest.from_json_str(manifest_content)


def update_chunk_status(project_id: str, topic_name: str, chunk_id: int) -> None:
    """Update the status of the chunk in the tracking database.

    Parameters
    ----------
    project_id : `str`
        The GCP project ID.
    topic_name : `str`
        The name of the Pub/Sub topic to publish the status update.
    chunk_id : `int`
        The ID of the chunk being processed.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    message = {
        "operation": "update",
        "apdb_replica_chunk": chunk_id,
        "values": {
            "status": "staged",
        },
    }
    try:
        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        future.result()  # Wait for the publish to complete
        logging.info("Published message to topic %s: %s", topic_path, message)
    except Exception:
        logging.exception(
            "Failed to publish message to topic %s: %s", topic_path, message
        )
        raise

    logging.info(f"Published chunk status update for chunk {chunk_id}")


def run(argv: Optional[list[str]] = None) -> None:
    """Run the pipeline."""
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)

    gcp_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    temp_location = gcp_options.temp_location
    if not temp_location:
        raise ValueError("GCP temp_location must be set in pipeline options.")

    dataset_id = custom_options.dataset_id
    folder = custom_options.folder
    chunk_id = int(custom_options.chunk_id)
    topic_name = custom_options.topic_name

    logging.info(
        f"Staging chunk {chunk_id} with folder {folder} into dataset `{dataset_id}` and topic `{topic_name}`."
    )

    if ":" in dataset_id:
        project_id, dataset_id = dataset_id.split(":", 1)
    else:
        project_id = gcp_options.project

    try:
        manifest = read_manifest(chunk_id, folder)
        logging.info(
            "Read manifest contents: %s",
            manifest.model_dump_json(),
        )
    except Exception:
        logging.exception("Failed to read manifest file from GCS")
        raise

    if manifest.is_empty_chunk:
        logging.info("Chunk %d is empty. No data to stage.", chunk_id)
        return

    logging.info("Loading table files: %s", list(manifest.files))

    # Filter out the update records file from the list of parquet files to be
    # staged.
    parquet_files = [
        name
        for name in manifest.files.keys()
        if name != UpdateRecords.PARQUET_FILE_NAME
    ]

    with apache_beam.Pipeline(options=options) as pipeline:
        for file_name in parquet_files:
            data = read_parquet(pipeline, folder, file_name)

            table_fqn = f"{project_id}:{dataset_id}.{Path(file_name).stem}"

            write_to_bigquery(
                data,
                table_fqn,
                temp_location,
            )

    # Update the chunk status in the tracking database.
    update_chunk_status(project_id, topic_name, chunk_id)


if __name__ == "__main__":
    run()
