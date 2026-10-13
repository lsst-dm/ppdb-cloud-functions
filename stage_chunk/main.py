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
from pathlib import Path
from typing import Any

import functions_framework
import google.auth
import sqlalchemy.exc
from cloudevents.http import CloudEvent
from google.api_core.exceptions import NotFound, ServerError, TooManyRequests
from google.cloud import bigquery
from googleapiclient.discovery import build
from lsst.dax.ppdb.bigquery import (
    ChunkStatus,
    ChunkStatusTransitionError,
    Manifest,
    PpdbBigQuery,
)
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    DecodeMessageDataError,
    decode_message_data,
    handle_database_error,
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


def _find_staged_tables(
    bq_client: bigquery.Client, dataset_fqn: str, chunk_id: int, manifest: Manifest
) -> list[str]:
    """Return the names of the staging tables that already contain rows for
    the chunk.
    """
    if manifest.is_empty_chunk:
        return []

    location = bq_client.get_dataset(dataset_fqn).location
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("chunk_id", "INT64", chunk_id)]
    )
    staged_table_names = []
    for file_name in manifest.files:
        table_name = Path(file_name).stem
        sql = (
            f"SELECT COUNT(*) FROM `{dataset_fqn}.{table_name}` "
            "WHERE apdb_replica_chunk = @chunk_id"
        )
        job = bq_client.query(sql, job_config=job_config, location=location)
        if next(iter(job.result()))[0] > 0:
            staged_table_names.append(table_name)
    return staged_table_names


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
        chunk_id = int(data["chunk_id"])
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
    except (ValueError, TypeError) as e:
        # Non-retryable error, just log and return.
        logger.log_event(
            logging.WARNING,
            "Invalid chunk_id in Pub/Sub message",
            "invalid_chunk_id",
            error=e,
            chunk_id=data["chunk_id"],
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
    except sqlalchemy.exc.SQLAlchemyError as e:
        # Raising a retryable error triggers a Pub/Sub retry.
        if handle_database_error(logger, e):
            raise
        return

    # Check if the chunk was found in the database.
    if chunk is None:
        logger.log_event(
            logging.WARNING,
            "Chunk not found in PPDB",
            "ppdb_chunk_not_found",
        )
        return

    # Read the chunk's manifest from GCS.
    try:
        manifest = Manifest.from_uri(folder)
    except FileNotFoundError as e:
        logger.log_event(
            logging.ERROR,
            "Chunk manifest not found in GCS",
            "manifest_not_found",
            error=e,
            gcs_folder=folder,
        )
        return
    except (ServerError, TooManyRequests) as e:
        # Transient GCS errors that persisted through the retries done when
        # reading. Raising triggers a Pub/Sub retry.
        logger.log_event(
            logging.WARNING,
            "Retryable error reading chunk manifest from GCS",
            "retryable_manifest_read_error",
            error=e,
            gcs_folder=folder,
        )
        raise
    except Exception as e:
        logger.log_event(
            logging.ERROR,
            "Unexpected error while reading chunk manifest",
            "unexpected_manifest_read_error",
            error=e,
            gcs_folder=folder,
        )
        return

    # Check that none of the chunk's data is already in the BigQuery staging
    # tables, which would cause duplicate records.
    try:
        bq_client = bigquery.Client(project=PROJECT_ID, credentials=_credentials)
        staged_table_names = _find_staged_tables(
            bq_client, f"{PROJECT_ID}.{dataset_id}", chunk_id, manifest
        )
    except NotFound as e:
        # Missing staging dataset or table is a configuration error.
        logger.log_event(
            logging.ERROR,
            "BigQuery staging dataset or table not found",
            "staging_resource_not_found",
            error=e,
        )
        return
    except (ServerError, TooManyRequests) as e:
        # Transient BigQuery errors. Raising triggers a Pub/Sub retry.
        logger.log_event(
            logging.WARNING,
            "Retryable BigQuery error while validating chunk for staging",
            "retryable_staging_check_error",
            error=e,
        )
        raise
    except Exception as e:
        logger.log_event(
            logging.ERROR,
            "Unexpected error while validating chunk for staging",
            "unexpected_chunk_validation_error",
            error=e,
        )
        return

    if staged_table_names:
        logger.log_event(
            logging.ERROR,
            "Chunk already has staged data in BigQuery",
            "chunk_already_staged",
            table_names=staged_table_names,
        )
        return

    # Claim the chunk by setting its status to staging. Only one caller can
    # succeed, so duplicate deliveries of the same message cannot launch more
    # than one Dataflow job for the chunk.
    try:
        PPDB.update_chunk_status(chunk_id, ChunkStatus.STAGING)
    except ChunkStatusTransitionError as e:
        logger.log_event(
            logging.WARNING,
            "Chunk could not be claimed for staging",
            "chunk_claim_failed",
            error=e,
            current_status=e.current_status,
            new_status=e.new_status,
        )
        return
    except LookupError as e:
        logger.log_event(
            logging.WARNING,
            "Chunk not found in PPDB",
            "ppdb_chunk_not_found",
            error=e,
        )
        return
    except sqlalchemy.exc.SQLAlchemyError as e:
        # Raising a retryable error triggers a Pub/Sub retry.
        if handle_database_error(logger, e):
            raise
        return

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

        # The job is always present if the launch succeeded.
        if "job" not in response:
            logger.log_event(
                logging.ERROR,
                "Dataflow API response missing 'job' field",
                "dataflow_response_missing_job",
                dataflow_response=response,
            )
            try:
                PPDB.update_chunk_status(chunk_id, ChunkStatus.FAILED)
            except (ChunkStatusTransitionError, LookupError) as status_error:
                logger.log_event(
                    logging.ERROR,
                    "Failed to mark chunk as failed after missing job",
                    "chunk_status_update_failed",
                    error=status_error,
                )
            except sqlalchemy.exc.SQLAlchemyError as status_error:
                handle_database_error(logger, status_error)
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

        # Release the claim so the chunk is not left in staging. A failed
        # chunk can be claimed again, so a Pub/Sub retry can still proceed.
        try:
            PPDB.update_chunk_status(chunk_id, ChunkStatus.FAILED)
        except (ChunkStatusTransitionError, LookupError) as status_error:
            logger.log_event(
                logging.ERROR,
                "Failed to mark chunk as failed after launch error",
                "chunk_status_update_failed",
                error=status_error,
            )
        except sqlalchemy.exc.SQLAlchemyError as status_error:
            handle_database_error(logger, status_error)

        if retryable:
            # Raising the exception should trigger a retry.
            raise
        else:
            # Non-retryable error, just return.
            return

    return
