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
import os
from typing import Any

import google.cloud.logging
from google.cloud import secretmanager
from sqlalchemy import create_engine, insert, update, MetaData, Table
from sqlalchemy.engine import Engine

# Set up Google Cloud Logging
client = google.cloud.logging.Client()
client.setup_logging()
logging.getLogger().setLevel(logging.DEBUG)

# Required environment variables
PROJECT_ID = os.environ["PROJECT_ID"]
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_SCHEMA = os.environ["DB_SCHEMA"]

# Module-level cache for engine and table
_engine: Engine | None = None
_table: Table | None = None


def get_db_password() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/ppdb-db-password/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        db_password = get_db_password()
        db_url = (
            f"postgresql+psycopg2://{DB_USER}:{db_password}@{DB_HOST}:5432/{DB_NAME}"
        )
        logging.info(
            "Connecting to database at: %s",
            f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:5432/{DB_NAME}",
        )
        _engine = create_engine(
            db_url,
            pool_pre_ping=True,
            connect_args={"options": f"-c search_path={DB_SCHEMA}"},
        )
    return _engine


def get_table() -> Table:
    global _table
    if _table is None:
        metadata = MetaData()
        _table = Table("PpdbReplicaChunk", metadata, autoload_with=get_engine())
    return _table


def _update(
    engine: Engine, table: Table, chunk_id: int, values: dict[str, Any]
) -> None:
    logging.info(
        "Preparing to update replica chunk %d with values: %s", chunk_id, values
    )
    stmt = update(table).where(table.c.apdb_replica_chunk == chunk_id).values(values)
    with engine.begin() as conn:
        result = conn.execute(stmt)
        affected_rows = result.rowcount

    new_status = values.get("status")
    if affected_rows == 0:
        logging.warning(
            "No rows updated for replica chunk %s with status '%s'",
            chunk_id,
            new_status,
        )
    else:
        logging.info(
            "Successfully updated %d row(s) for replica chunk %s to status '%s'",
            affected_rows,
            chunk_id,
            new_status,
        )


def _insert(
    engine: Engine, table: Table, chunk_id: int, values: dict[str, Any]
) -> None:
    insert_values = {"apdb_replica_chunk": chunk_id, **values}
    logging.info(
        "Preparing to insert replica chunk %d with values: %s", chunk_id, insert_values
    )
    stmt = insert(table).values(insert_values)
    with engine.begin() as conn:
        result = conn.execute(stmt)
        affected_rows = result.rowcount

    new_status = insert_values.get("status")
    if affected_rows == 0:
        logging.error(
            "Expected to insert replica chunk %s with status '%s', but no rows were inserted.",
            chunk_id,
            new_status,
        )
        raise RuntimeError(
            f"No rows inserted for apdb_replica_chunk={chunk_id} - insert silently failed."
        )
    else:
        logging.info(
            "Successfully inserted %d row(s) for replica chunk %s with status '%s'",
            affected_rows,
            chunk_id,
            new_status,
        )


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

        engine = get_engine()
        table = get_table()

        if operation == "update":
            _update(engine, table, chunk_id, values)
        elif operation == "insert":
            _insert(engine, table, chunk_id, values)

    except Exception:
        logging.exception("Error processing Pub/Sub message")
