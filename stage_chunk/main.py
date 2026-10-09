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

import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
import google.auth
import sqlalchemy.exc
from cloudevents.http import CloudEvent
from googleapiclient.discovery import build
from lsst.dax.ppdb.bigquery import ChunkStatus, PpdbBigQuery
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    DecodeMessageDataError,
    decode_message_data,
    handle_request_error,
    setup_cloud_logging,
)

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("stage_chunk")


# Read required environment variables.
PROJECT_ID = os.environ["PROJECT_ID"]
DATAFLOW_TEMPLATE_PATH = os.environ["DATAFLOW_TEMPLATE_PATH"]
REGION = os.environ["REGION"]
SERVICE_ACCOUNT_EMAIL = os.environ["SERVICE_ACCOUNT_EMAIL"]
TEMP_LOCATION = os.environ["TEMP_LOCATION"]
TOPIC_NAME = os.environ["TOPIC_NAME"]
GOOGLE_CLOUD_SUBNETWORK = os.environ.get("GOOGLE_CLOUD_SUBNETWORK")
NUM_RETRIES = int(os.environ["NUM_RETRIES"])

_credentials, _ = google.auth.default()

_dataflow_client = build(
    "dataflow",
    "v1b3",
    credentials=_credentials,
    cache_discovery=False,
)

# Setup PPDB BigQuery interface from environment variable configuration
PPDB = PpdbBigQuery.from_env()


@functions_framework.cloud_event
def trigger_stage_chunk(event: CloudEvent) -> None:
    """Cloud Function that launches a Dataflow job to stage chunks of PPDB
    data.

    Parameters
    ----------
    cloud_event
        The CloudEvent delivered by the Pub/Sub trigger. The ``dataset``,
        ``chunk_id`` and ``folder`` fields should be included in the message
        data.
    """
    # Updatable fields attached to every log entry.
    log_fields: dict[str, Any] = {}

    logger = CloudEventLogger(_LOG, event["id"], log_fields)

    try:
        data = decode_message_data(logger, event)
    except DecodeMessageDataError:
        return

    try:
        dataset_id = data["dataset"]
        chunk_id_raw = data["chunk_id"]
        folder = data["folder"]
    except KeyError as e:
        # Non-retryable error, just log and return.
        logger.log_event(
            logging.WARNING,
            "Missing required key in Pub/Sub message",
            "missing_key_in_pubsub_message",
            error=e,
            missing_keys=[
                key for key in ["dataset", "chunk_id", "folder"] if key not in data
            ],
            pubsub_message=data,
        )
        return

    # Convert the raw chunk ID to an integer.
    try:
        chunk_id = int(chunk_id_raw)
    except (ValueError, TypeError) as e:
        # Non-retryable error, just log and return.
        logger.log_event(
            logging.WARNING,
            "Invalid chunk_id in Pub/Sub message",
            "invalid_chunk_id",
            error=e,
            chunk_id=chunk_id_raw,
        )
        return

    # Attach chunk ID and dataset ID to subsequent log entries.
    log_fields.update(chunk_id=chunk_id, dataset=dataset_id)

    logger.log_event(
        logging.INFO,
        "Received stage chunk request",
        "stage_chunk_request_received",
        gcs_folder=folder,
    )

    # Get the chunk's metadata from the PPDB Postgres database.
    try:
        chunk = PPDB.find_chunk_by_id(chunk_id)
    except sqlalchemy.exc.OperationalError as e:
        # Transient database errors, e.g. dropped connections, statement
        # timeouts, and deadlocks. Raising triggers a Pub/Sub retry.
        logger.log_event(
            logging.WARNING,
            "Retryable database error while retrieving chunk",
            "retryable_database_error",
            error=e,
        )
        raise
    except sqlalchemy.exc.SQLAlchemyError as e:
        # Other database errors, e.g. integrity or programming errors, are
        # not retryable.
        logger.log_event(
            logging.ERROR,
            "Error retrieving chunk from PPDB",
            "ppdb_chunk_retrieval_error",
            error=e,
        )
        return

    # Check if the chunk was found in the database.
    if chunk is None:
        logger.log_event(
            logging.WARNING,
            "Chunk not found in PPDB",
            "ppdb_chunk_not_found",
        )
        return

    # Check if the chunk has a valid status for staging. UPLOADED are chunks
    # that have been successfully uploaded to GCS and are ready for staging.
    # FAILED are chunks where the previous ingestion attempt failed and can be
    # retried.
    if chunk.status not in (ChunkStatus.UPLOADED, ChunkStatus.FAILED):
        logger.log_event(
            logging.WARNING,
            "Chunk is not in a valid state for staging",
            "invalid_chunk_status",
            chunk_status=chunk.status,
        )
        return

    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
    job_name = f"stage-chunk-{chunk_id}-{timestamp}"

    launch_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": DATAFLOW_TEMPLATE_PATH,
            "parameters": {
                "dataset_id": dataset_id,
                "chunk_id": chunk_id,
                "folder": folder,
                "topic_name": TOPIC_NAME,
            },
            "environment": {
                "serviceAccountEmail": SERVICE_ACCOUNT_EMAIL,
                "tempLocation": TEMP_LOCATION,
                "subnetwork": GOOGLE_CLOUD_SUBNETWORK,
            },
        }
    }

    logger.log_event(
        logging.INFO,
        "Launching Dataflow job",
        "dataflow_job_launching",
        dataflow_job_name=job_name,
        launch_parameters=launch_body["launchParameter"]["parameters"],
        environment=launch_body["launchParameter"]["environment"],
        container_spec_gcs_path=launch_body["launchParameter"]["containerSpecGcsPath"],
    )

    try:
        request = (
            _dataflow_client.projects()
            .locations()
            .flexTemplates()
            .launch(projectId=PROJECT_ID, location=REGION, body=launch_body)
        )
        response = request.execute(num_retries=NUM_RETRIES)

        if "job" not in response:
            logger.log_event(
                logging.ERROR,
                "Dataflow API response missing 'job' field",
                "dataflow_response_missing_job",
                dataflow_response=response,
            )
            return

        job_id = response.get("job", {}).get("id", "unknown")

        logger.log_event(
            logging.INFO,
            "Dataflow job launched successfully",
            "dataflow_job_launched",
            dataflow_job_id=job_id,
            dataflow_job_name=job_name,
        )
    except Exception as e:
        retryable = handle_request_error(logger, e)
        if retryable:
            # Raising the exception should trigger a retry.
            raise
        else:
            # Non-retryable error, just return.
            return

    return
