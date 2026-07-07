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
import binascii
import json
import logging
import os
from typing import Any

from google.cloud import logging as cloud_logging
from lsst.dax.ppdb.bigquery import (
    ChunkStatus,
    PpdbBigQuery,
)

# Configure cloud logging.
client = cloud_logging.Client()
client.setup_logging()  # Redirects standard logging to Cloud Logging
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.getLogger().setLevel(log_level)


# Setup PPDB BigQuery interface from environment variable configuration
ppdb = PpdbBigQuery.from_env()


def track_chunk(event: dict[str, Any], context: Any) -> None:
    try:
        try:
            message = base64.b64decode(event["data"]).decode("utf-8")
        except (KeyError, binascii.Error, UnicodeDecodeError) as e:
            raise Exception("Malformed or missing Pub/Sub data payload") from e

        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            raise Exception("Failed to decode JSON from Pub/Sub message") from e

        logging.info(
            "Received event to track replica chunk",
            extra={
                "json_fields": {"event": "track_chunks_event_received", "data": data}
            },
        )

        operation = data.get("operation", None)
        if not operation:
            raise ValueError("Empty 'operation' value in Pub/Sub message")
        if operation != "update":
            raise ValueError(f"Unsupported operation: {operation}")

        values = data.get("values", None)
        if not values:
            raise ValueError("No 'values' key found in Pub/Sub message")

        chunk_id = data.get("apdb_replica_chunk", None)
        if not chunk_id:
            raise ValueError("No 'apdb_replica_chunk' value in Pub/Sub message")

        if operation != "update":
            raise ValueError(f"Unsupported operation: {operation}")

        chunk = ppdb.find_chunk_by_id(int(chunk_id))
        if not chunk:
            raise LookupError(f"Replica chunk {chunk_id} not found")

        new_status = values.get("status", None)
        if not new_status:
            raise ValueError("Empty 'status' value in values for update operation")

        update_count = ppdb.update_chunks(
            [chunk.with_new_status(ChunkStatus(new_status))], {"status"}
        )
        if update_count < 1:
            raise LookupError(
                f"Failed to update replica chunk {chunk_id} with values: {values}"
            )

        logging.info(
            "Updated replica chunk status",
            extra={
                "json_fields": {
                    "event": "replica_chunk_status_updated",
                    "chunk_id": chunk_id,
                    "values": values,
                    "affected_rows": update_count,
                }
            },
        )

    except Exception:
        logging.exception("Error processing Pub/Sub event")
