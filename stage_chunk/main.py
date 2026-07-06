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
from datetime import datetime, timezone
from typing import Any

import google.auth
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import logging as cloud_logging
from google.cloud.functions_v1.context import Context
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configure cloud logging.
client = cloud_logging.Client()
client.setup_logging()  # Redirects standard logging to Cloud Logging
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.getLogger().setLevel(log_level)

# Silence noisy warnings from
logging.getLogger("google_auth_httplib2").setLevel(logging.ERROR)

# Read required environment variables.
PROJECT_ID = os.environ["PROJECT_ID"]
DATAFLOW_TEMPLATE_PATH = os.environ["DATAFLOW_TEMPLATE_PATH"]
REGION = os.environ["REGION"]
SERVICE_ACCOUNT_EMAIL = os.environ["SERVICE_ACCOUNT_EMAIL"]
TEMP_LOCATION = os.environ["TEMP_LOCATION"]
TOPIC_NAME = os.environ["TOPIC_NAME"]

_credentials, _ = google.auth.default()
_dataflow_client = build("dataflow", "v1b3", credentials=_credentials)


def trigger_stage_chunk(event: dict[str, Any], context: Context) -> None:
    """Cloud Function to launch a Dataflow job to stage PPDB data.

    Parameters
    ----------
    event : `dict`
        The dictionary with data specific to this type of event. The `data`
        field contains a base64-encoded string representing a JSON message
        with `bucket`, `name` and `dataset` fields.
    context : `google.cloud.functions.Context`
        Metadata of triggering event including `event_id`.
    """
    # Fields attached to every structured log entry for this invocation.
    log_fields: dict[str, Any] = {"event_id": getattr(context, "event_id", None)}

    def log_event(
        level: int,
        message: str,
        event_name: str,
        *,
        exc_info: bool = False,
        **fields: Any,
    ) -> None:
        """Emit a structured log entry under Cloud Logging ``json_fields``."""
        logging.log(
            level,
            message,
            exc_info=exc_info,
            extra={"json_fields": {"event": event_name, **log_fields, **fields}},
        )

    try:
        message = base64.b64decode(event["data"]).decode("utf-8")
    except Exception:
        log_event(
            logging.WARNING,
            "Malformed or missing Pub/Sub data payload",
            "malformed_pubsub_payload",
            exc_info=True,
            pubsub_event=event,
        )
        return

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        log_event(
            logging.WARNING,
            "Failed to decode JSON from Pub/Sub message",
            "json_decode_error",
            exc_info=True,
            pubsub_message=message,
        )
        return

    if not isinstance(data, dict):
        log_event(
            logging.WARNING,
            "Pub/Sub message is not a JSON object",
            "invalid_payload_type",
            pubsub_message=data,
        )
        return

    try:
        dataset_id = data["dataset"]
        chunk_id = data["chunk_id"]
        folder = data["folder"]
    except KeyError:
        log_event(
            logging.WARNING,
            "Missing required key in Pub/Sub message",
            "missing_key_in_pubsub_message",
            exc_info=True,
            missing_keys=[
                key for key in ["dataset", "chunk_id", "folder"] if key not in data
            ],
            pubsub_message=data,
        )
        return

    # Attach the correlation identifiers to all subsequent logs.
    log_fields.update(chunk_id=chunk_id, dataset=dataset_id)

    log_event(
        logging.INFO,
        "Received stage chunk request",
        "stage_chunk_request_received",
        gcs_folder=folder,
    )

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
            },
        }
    }

    log_event(
        logging.INFO,
        "Launching Dataflow job",
        "dataflow_job_launching",
        dataflow_job_name=job_name,
        launch_parameters=launch_body["launchParameter"]["parameters"],
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
            log_event(
                logging.ERROR,
                "Dataflow API response missing 'job' field",
                "dataflow_response_missing_job",
                dataflow_response=response,
            )
            return

        job_id = response.get("job", {}).get("id", "unknown")

        log_event(
            logging.INFO,
            "Dataflow job launched successfully",
            "dataflow_job_launched",
            dataflow_job_id=job_id,
            dataflow_job_name=job_name,
        )
    except HttpError as e:
        retryable = e.resp.status in (429, 500, 503)
        log_event(
            logging.WARNING if retryable else logging.ERROR,
            "Retryable HTTP error" if retryable else "Non-retryable HTTP error",
            "retryable_http_error" if retryable else "non_retryable_http_error",
            exc_info=True,
            http_status=e.resp.status,
            error=str(e),
            error_type=type(e).__name__,
        )
        if retryable:
            raise  # Will trigger retry
        return  # Acknowledge message

    except GoogleAPICallError as e:
        log_event(
            logging.WARNING,
            "Retryable GCP API error",
            "gcp_api_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise  # Will trigger retry

    except Exception as e:
        log_event(
            logging.ERROR,
            "Unexpected error during job submission",
            "unexpected_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        return  # Acknowledge message

    return
