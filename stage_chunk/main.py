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

import functions_framework
from cloudevents.http import CloudEvent
from lsst.dax.ppdb.bigquery import (
    ChunkStager,
    ChunkStagingError,
    ChunkStagingRetryableError,
    ChunkStatusError,
    InvalidManifestError,
    Manifest,
    PpdbBigQuery,
)
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    DecodeMessageDataError,
    cloud_run_initialize_sentry,
    decode_message_data,
    flush_sentry,
    handle_storage_error,
    setup_cloud_logging,
)

cloud_run_initialize_sentry()

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("stage_chunk")

# Created once per instance and shared by all requests, so that database
# connections are reused.
_ppdb = PpdbBigQuery.from_env()


@functions_framework.cloud_event
@flush_sentry
def trigger_stage_chunk(event: CloudEvent) -> None:
    """Cloud Function that stages a replica chunk into the BigQuery staging
    tables.

    Parameters
    ----------
    event
        The CloudEvent delivered by the Pub/Sub trigger. The message data
        should include ``folder``, the chunk's folder in Cloud Storage. The
        chunk ID is read from the manifest in that folder.

    Notes
    -----
    Returning acknowledges the message and raising makes Pub/Sub retry it.
    Only transient staging errors are retried. `ChunkStager` records the
    chunk's status and logs the outcome.
    """
    logger = CloudEventLogger(_LOG, event["id"])

    try:
        data = decode_message_data(logger, event)
    except DecodeMessageDataError:
        # Already logged by decode_message_data.
        return

    # Read the chunk's manifest. The chunk hasn't been claimed yet, so its
    # status is unchanged if this fails.
    try:
        folder = data["folder"]
        manifest = Manifest.from_uri(folder)
    except (KeyError, InvalidManifestError) as e:
        logger.log_event(
            logging.ERROR,
            "Invalid stage chunk message or manifest",
            "invalid_stage_chunk_request",
            error=e,
            pubsub_message=data,
        )
        return
    except Exception as e:
        # handle_storage_error logs the error. Transient errors are retried,
        # and anything else, e.g. a missing manifest, is dropped.
        if handle_storage_error(logger, e, "reading the chunk manifest"):
            raise
        return

    try:
        ChunkStager(_ppdb, logger, folder, manifest).stage_chunk()
    except ChunkStagingRetryableError:
        # Raised so that Pub/Sub retries the message. The stager has logged
        # the error and set the chunk to retrying.
        raise
    except (ChunkStagingError, ChunkStatusError):
        # Not retried: the chunk was rejected because of its status, or
        # staging failed permanently. The stager has logged why. Any other
        # error is unexpected and propagates, so it is reported.
        return
