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

import base64
import json
import logging
import os

import functions_framework
import google.auth
from cloudevents.http import CloudEvent
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    handle_request_error,
    setup_cloud_logging,
)

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("load_sso")

# Read required environment variables.
PROJECT_ID = os.environ["PROJECT_ID"]
DATAFLOW_TEMPLATE_PATH = os.environ["DATAFLOW_TEMPLATE_PATH"]
REGION = os.environ["REGION"]
SERVICE_ACCOUNT_EMAIL = os.environ["SERVICE_ACCOUNT_EMAIL"]
TEMP_LOCATION = os.environ["TEMP_LOCATION"]
GOOGLE_CLOUD_SUBNETWORK = os.environ["GOOGLE_CLOUD_SUBNETWORK"]
STAGING_DATASET_ID = os.environ["STAGING_DATASET_ID"]
INTERNAL_DATASET_ID = os.environ["INTERNAL_DATASET_ID"]
DATAFLOW_MACHINE_TYPE = os.environ["DATAFLOW_MACHINE_TYPE"]

_credentials, _ = google.auth.default()
_dataflow_client = build(
    "dataflow",
    "v1b3",
    credentials=_credentials,
    cache_discovery=False,
)


@functions_framework.cloud_event
def load_sso(event: CloudEvent) -> None:
    """Cloud Function to launch a Dataflow job to load SSO data."""
    logger = CloudEventLogger(_LOG, event["id"])

    try:
        message = base64.b64decode(event.data["message"]["data"]).decode("utf-8")
    except (KeyError, TypeError, ValueError) as e:
        logger.log_event(
            logging.ERROR,
            "Malformed or missing Pub/Sub data payload",
            "malformed_pubsub_payload",
            error=e,
            pubsub_event=event.data,
        )
        return

    try:
        data = json.loads(message)
    except json.JSONDecodeError as e:
        logger.log_event(
            logging.ERROR,
            "Failed to decode JSON from Pub/Sub message",
            "json_decode_error",
            error=e,
            pubsub_message=message,
        )
        return

    if not isinstance(data, dict):
        logger.log_event(
            logging.ERROR,
            "Pub/Sub message is not a JSON object",
            "invalid_payload_type",
            pubsub_message=data,
        )
        return

    try:
        bucket = data["bucket"]
        object_prefix = data["object_prefix"]
        uploaded_tables = data["uploaded_tables"]
    except KeyError as e:
        logger.log_event(
            logging.WARNING,
            "Missing required key in Pub/Sub message",
            "missing_key_in_pubsub_message",
            error=e,
            missing_keys=[
                key
                for key in [
                    "bucket",
                    "object_prefix",
                    "uploaded_tables",
                ]
                if key not in data
            ],
            pubsub_message=data,
        )
        return

    logger.log_event(
        logging.INFO,
        "Received load SSO request",
        "load_sso_request_received",
        gcs_bucket=bucket,
        gcs_object_prefix=object_prefix,
        uploaded_tables=uploaded_tables,
    )

    # A fixed job name is used to ensure that only one Load SSO job may be
    # active at once. Dataflow will reject launches if a job with the same name
    # is already running. This is used as a simple concurrency control
    # mechanism.
    job_name = "load-sso"

    launch_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": DATAFLOW_TEMPLATE_PATH,
            "parameters": {
                "bucket": bucket,
                "object_prefix": object_prefix,
                "tables": ",".join(uploaded_tables),
                "staging_dataset_id": STAGING_DATASET_ID,
                "internal_dataset_id": INTERNAL_DATASET_ID,
            },
            "environment": {
                "serviceAccountEmail": SERVICE_ACCOUNT_EMAIL,
                "tempLocation": TEMP_LOCATION,
                "subnetwork": GOOGLE_CLOUD_SUBNETWORK,
                "machineType": DATAFLOW_MACHINE_TYPE,
            },
        }
    }

    logger.log_event(
        logging.INFO,
        "Launching Dataflow job",
        "dataflow_job_launching",
        launch_parameter=launch_body["launchParameter"],
    )

    try:
        request = (
            _dataflow_client.projects()
            .locations()
            .flexTemplates()
            .launch(projectId=PROJECT_ID, location=REGION, body=launch_body)
        )
        response = request.execute()

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
        if isinstance(e, HttpError) and e.resp.status == 409:
            # Dataflow rejects launches while a job with the same name is
            # already active; this is expected and non-retryable.
            logger.log_event(
                logging.INFO,
                "Dataflow job already active",
                "dataflow_job_already_active",
                error=e,
                http_status=e.resp.status,
                dataflow_job_name=job_name,
            )
            return

        if handle_request_error(logger, e):
            raise  # Will trigger retry
        return  # Acknowledge message
