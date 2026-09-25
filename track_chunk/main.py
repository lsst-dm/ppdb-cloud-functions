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
from typing import Any

import functions_framework
import sqlalchemy.exc
from cloudevents.http import CloudEvent
from lsst.dax.ppdb.bigquery import ChunkStatus, PpdbBigQuery, UpdatableField
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    DecodeMessageDataError,
    decode_message_data,
    setup_cloud_logging,
)

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("track_chunk")

# Setup PPDB BigQuery interface from environment variable configuration
ppdb = PpdbBigQuery.from_env()


@functions_framework.cloud_event
def track_chunk(event: CloudEvent) -> None:
    """Cloud Function to update the status of an APDB replica chunk.

    Parameters
    ----------
    event : `CloudEvent`
        The CloudEvent delivered by the Pub/Sub trigger. The Pub/Sub message
        is available at ``event.data["message"]`` and its ``data`` field
        contains a base64-encoded string representing a JSON message with
        ``operation``, ``apdb_replica_chunk`` and ``values`` fields.
    """
    log_fields: dict[str, Any] = {}

    logger = CloudEventLogger(_LOG, event["id"], log_fields)

    try:
        data = decode_message_data(logger, event)
    except DecodeMessageDataError:
        return

    logger.log_event(
        logging.INFO,
        "Received event to track replica chunk",
        "track_chunks_event_received",
        data=data,
    )

    operation = data.get("operation")
    if not operation or operation != "update":
        logger.log_event(
            logging.ERROR,
            "Unsupported or missing operation",
            "unsupported_operation",
            operation=operation,
            pubsub_message=data,
        )
        return

    values = data.get("values")
    if not values:
        logger.log_event(
            logging.ERROR,
            "No 'values' key found in Pub/Sub message",
            "missing_values",
            pubsub_message=data,
        )
        return

    if not isinstance(values, dict):
        logger.log_event(
            logging.ERROR,
            "'values' is not a JSON object",
            "invalid_values_type",
            values=values,
        )
        return

    apdb_replica_chunk = data.get("apdb_replica_chunk")
    if not apdb_replica_chunk:
        logger.log_event(
            logging.ERROR,
            "No 'apdb_replica_chunk' value in Pub/Sub message",
            "missing_chunk_id",
            pubsub_message=data,
        )
        return

    new_status = values.get("status")
    if not new_status:
        logger.log_event(
            logging.ERROR,
            "Empty 'status' value in values for update operation",
            "missing_status",
            apdb_replica_chunk=apdb_replica_chunk,
            values=values,
        )
        return

    # Attach chunk ID to subsequent log entries.
    log_fields.update(chunk_id=apdb_replica_chunk)

    try:
        chunk = ppdb.find_chunk_by_id(int(apdb_replica_chunk))
        if not chunk:
            logger.log_event(
                logging.WARNING,
                "Replica chunk not found",
                "chunk_not_found",
            )
            return

        update_count = ppdb.update_chunks(
            [chunk.with_new_status(ChunkStatus(new_status))], {UpdatableField.STATUS}
        )
    except LookupError as e:
        # Raised by update_chunks if the chunk was removed between the
        # existence check above and the update. This is highly unlikely but
        # trap for it just in case. This error is not retryable.
        logger.log_event(
            logging.WARNING,
            "Replica chunk no longer exists",
            "chunk_not_found_during_update",
            error=e,
        )
        return
    except sqlalchemy.exc.SQLAlchemyError as e:
        # These are database-related errors; not considered retryable.
        logger.log_event(
            logging.ERROR,
            "Database error while updating replica chunk",
            "database_error",
            error=e,
        )
        return
    except Exception as e:
        # Catch-all for unexpected errors; not considered retryable.
        logger.log_event(
            logging.ERROR,
            "Unexpected error while updating replica chunk",
            "unexpected_error",
            error=e,
        )
        return

    if update_count < 1:
        # This may not even be possible without another error occurring first,
        # but log an error anyway. This is not retryable.
        logger.log_event(
            logging.ERROR,
            "Failed to update replica chunk",
            "chunk_update_failed",
            values=values,
        )
        return

    logger.log_event(
        logging.INFO,
        "Updated replica chunk status",
        "replica_chunk_status_updated",
        values=values,
        affected_rows=update_count,
    )
