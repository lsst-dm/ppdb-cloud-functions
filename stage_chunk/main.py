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
from datetime import timedelta
from typing import Any

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from lsst.dax.ppdb.bigquery import (
    ChunkStager,
    ChunkStagingError,
    ChunkStagingRetryableError,
    ChunkStatusError,
    PpdbBigQuery,
)
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    DecodeMessageDataError,
    cloud_run_initialize_sentry,
    decode_message_data,
    flush_sentry,
    setup_cloud_logging,
)

cloud_run_initialize_sentry()

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("stage_chunk")

# Created once per instance and shared by all requests, so that database
# connections and the BigQuery client are reused.
_ppdb = PpdbBigQuery.from_env()
_bigquery_client = bigquery.Client(project=_ppdb.config.project_id)

# BigQuery limits for the stager, set by Terraform.
BIGQUERY_CHECK_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_CHECK_TIMEOUT_SECONDS"])
BIGQUERY_JOB_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_JOB_TIMEOUT_SECONDS"])
BIGQUERY_RETRY_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_RETRY_TIMEOUT_SECONDS"])

# The stager is shared by all requests too.
_stager = ChunkStager(
    _ppdb,
    _bigquery_client,
    check_timeout=timedelta(seconds=BIGQUERY_CHECK_TIMEOUT_SECONDS),
    job_timeout=timedelta(seconds=BIGQUERY_JOB_TIMEOUT_SECONDS),
    retry_timeout=timedelta(seconds=BIGQUERY_RETRY_TIMEOUT_SECONDS),
)


@functions_framework.cloud_event
@flush_sentry
def stage_chunk(event: CloudEvent) -> None:
    """Cloud Function that stages a replica chunk into the BigQuery staging
    tables.

    Parameters
    ----------
    event
        The CloudEvent delivered by the Pub/Sub trigger. The message data
        should include ``chunk_id`` and ``folder``, the chunk's folder in
        Cloud Storage.

    Notes
    -----
    Returning acknowledges the message and raising makes Pub/Sub retry it.
    Bad messages, rejected chunks and permanent staging failures are
    acknowledged. Any other error is raised and retried, including
    temporary staging errors. `ChunkStager` records the chunk's status and
    logs the outcome.
    """
    # Fields added to every log entry, including those of ChunkStager.
    log_fields: dict[str, Any] = {}
    logger = CloudEventLogger(_LOG, event["id"], log_fields)

    try:
        data = decode_message_data(logger, event)
    except DecodeMessageDataError:
        # Already logged by decode_message_data.
        return

    try:
        chunk_id = int(data["chunk_id"])
    except (KeyError, ValueError, TypeError) as e:
        # There is no valid chunk ID, so there is no chunk to mark as failed.
        logger.log_event(
            logging.ERROR,
            "Missing or invalid chunk_id in stage chunk message",
            "invalid_chunk_id",
            error=e,
            pubsub_message=data,
        )
        return
    log_fields["chunk_id"] = chunk_id

    try:
        folder = data["folder"]
    except KeyError as e:
        logger.log_event(
            logging.ERROR,
            "Missing folder in stage chunk message",
            "missing_folder",
            error=e,
            pubsub_message=data,
        )
        return

    try:
        _stager.stage_chunk(logger, chunk_id, folder)
    except ChunkStagingRetryableError:
        # Raised so that Pub/Sub retries the message. The stager has logged
        # the error and set the chunk to retrying.
        raise
    except (ChunkStagingError, ChunkStatusError):
        # Not retried: the chunk was rejected because of its status, or
        # staging failed permanently. The stager has logged why. Any other
        # error is unexpected and propagates, so it is reported.
        return
