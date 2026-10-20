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
    cloud_run_initialize_sentry,
    decode_message_data,
    flush_sentry,
    handle_database_error,
    handle_request_error,
    setup_cloud_logging,
)


cloud_run_initialize_sentry()

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

# Chunks with any other status are not staged.
_STAGEABLE_STATUSES = {ChunkStatus.UPLOADED, ChunkStatus.FAILED}

_credentials, _ = google.auth.default()

_dataflow_client = build(
    "dataflow",
    "v1b3",
    credentials=_credentials,
    cache_discovery=False,
)


@functions_framework.cloud_event
@flush_sentry
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
        chunk_id = data["chunk_id"]
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

    # Attach chunk ID and dataset ID to subsequent log entries.
    log_fields.update(chunk_id=chunk_id, dataset=dataset_id)

    logger.log_event(
        logging.INFO,
        "Received stage chunk request",
        "stage_chunk_request_received",
        gcs_folder=folder,
    )

    try:
        with PpdbBigQuery.from_env() as ppdb:
            chunk = ppdb.find_chunk_by_id(int(chunk_id))
    except sqlalchemy.exc.SQLAlchemyError as e:
        if handle_database_error(logger, e):
            # Raising the exception should trigger a retry.
            raise
        else:
            # Non-retryable error, just return.
            return
    except Exception as e:
        # Other errors are not retryable.
        logger.log_event(
            logging.ERROR,
            "Error while reading replica chunk",
            "chunk_read_error",
            error=e,
        )
        return

    if chunk is None:
        logger.log_event(
            logging.WARNING,
            "Replica chunk not found",
            "chunk_not_found",
        )
        return

    if chunk.status not in _STAGEABLE_STATUSES:
        logger.log_event(
            logging.WARNING,
            "Replica chunk has invalid status for staging",
            "invalid_chunk_status",
            status=str(chunk.status),
            allowed_statuses=sorted(str(s) for s in _STAGEABLE_STATUSES),
        )
        return

    # No timestamp in the job name, so that Dataflow rejects a second active
    # job for the same chunk.
    job_name = f"stage-chunk-{chunk_id}"

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
