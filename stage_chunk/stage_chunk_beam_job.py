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
from lsst.dax.ppdb.bigquery import ChunkStatus, Manifest
from lsst.dax.ppdb.gcp import CloudEventLogger

# Configure Google Cloud logging
cloud_logging.Client().setup_logging()
logging.getLogger().setLevel(logging.INFO)


class SuppressNoIteratorWarning(logging.Filter):
    """Suppress the unhelpful Beam process-method iterator warning.

    Note
    ----
    Duplicated across Dataflow jobs to avoid a shared dependency.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Return false for the warning that should be suppressed."""
        if record.name == "apache_beam.transforms.core":
            message = str(record.getMessage())
            if "No iterator is returned by the process method" in message:
                return False
        return True


logging.getLogger("apache_beam.transforms.core").addFilter(SuppressNoIteratorWarning())


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

    # Download the manifest file and return its data.
    blob = bucket.blob(manifest_path)
    manifest_content = blob.download_as_text()
    return Manifest.from_json_str(manifest_content)


def publish_chunk_status(
    logger: CloudEventLogger,
    project_id: str,
    topic_name: str,
    chunk_id: int,
    status: ChunkStatus,
) -> None:
    """Publish a status update for the chunk to the tracking topic.

    Parameters
    ----------
    logger : `CloudEventLogger`
        Logger used to report the outcome of the publish.
    project_id : `str`
        The GCP project ID.
    topic_name : `str`
        The name of the Pub/Sub topic to publish the status update.
    chunk_id : `int`
        The ID of the chunk being processed.
    status : `ChunkStatus`
        The new status of the chunk.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    message = {
        "operation": "update",
        "apdb_replica_chunk": chunk_id,
        "values": {
            "status": status.value,
        },
    }
    try:
        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        future.result()  # Wait for the publish to complete
    except Exception as e:
        logger.log_event(
            logging.ERROR,
            "Failed to publish chunk status update",
            "chunk_status_publish_failed",
            error=e,
            topic_path=topic_path,
            pubsub_message=message,
        )
        raise

    logger.log_event(
        logging.INFO,
        "Published chunk status update",
        "chunk_status_published",
        topic_path=topic_path,
        pubsub_message=message,
    )


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

    logger = CloudEventLogger(
        logging.getLogger("stage_chunk_beam_job"),
        log_fields={"chunk_id": chunk_id, "dataset": dataset_id, "gcs_folder": folder},
    )

    logger.log_event(
        logging.INFO,
        "Staging chunk",
        "chunk_staging_started",
        topic_name=topic_name,
    )

    if ":" in dataset_id:
        project_id, dataset_id = dataset_id.split(":", 1)
    else:
        project_id = gcp_options.project

    try:
        logger.log_event(logging.INFO, "Reading manifest", "manifest_reading")
        manifest = read_manifest(chunk_id, folder)
        logger.log_event(
            logging.INFO,
            "Read manifest",
            "manifest_read",
            manifest=manifest.model_dump(mode="json"),
        )

        if manifest.is_empty_chunk:
            logger.log_event(
                logging.INFO,
                "Chunk is empty, no data to stage",
                "empty_chunk",
            )
            return

        logger.log_event(
            logging.INFO,
            "Loading table files",
            "table_files_loading",
            files=list(manifest.files),
        )

        with apache_beam.Pipeline(options=options) as pipeline:
            for file_name in manifest.files:
                table_fqn = f"{project_id}:{dataset_id}.{Path(file_name).stem}"
                logger.log_event(
                    logging.INFO,
                    "Loading parquet file into BigQuery table",
                    "table_file_loading",
                    file_name=file_name,
                    table_fqn=table_fqn,
                )

                data = read_parquet(pipeline, folder, file_name)

                write_to_bigquery(
                    data,
                    table_fqn,
                    temp_location,
                )
    except Exception as e:
        logger.log_event(
            logging.ERROR,
            "Failed to stage chunk",
            "chunk_staging_failed",
            error=e,
        )
        publish_chunk_status(
            logger, project_id, topic_name, chunk_id, ChunkStatus.FAILED
        )
        raise

    publish_chunk_status(logger, project_id, topic_name, chunk_id, ChunkStatus.STAGED)


if __name__ == "__main__":
    run()
