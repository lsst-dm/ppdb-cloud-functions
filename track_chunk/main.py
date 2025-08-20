# This file is part of ppdb-cloud-functions
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
from typing import Any

from lsst.dax.ppdbx.gcp.db import ReplicaChunkDatabase
from lsst.dax.ppdbx.gcp.log_config import setup_logging

setup_logging()

db = ReplicaChunkDatabase.from_env()


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

        logging.info("Received Pub/Sub message: %s", data)

        operation = data.get("operation")
        if not operation:
            raise KeyError("Missing 'operation' key in Pub/Sub message")
        if operation not in ["insert", "update"]:
            raise ValueError(f"Unsupported operation: {operation}")

        values = data.get("values")
        if not values:
            raise ValueError("No 'values' key found in Pub/Sub message")

        if "apdb_replica_chunk" not in data:
            raise KeyError("Missing 'apdb_replica_chunk' in message")

        chunk_id = data["apdb_replica_chunk"]

        if operation == "update":
            db.update(chunk_id, values)
        elif operation == "insert":
            db.insert(chunk_id, values)

    except Exception:
        logging.exception("Error processing Pub/Sub message")
