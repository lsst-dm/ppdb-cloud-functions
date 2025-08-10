"""Promote APDB replica chunks from staging into production."""

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

from flask import Request, jsonify
from lsst.ppdb.gcp.bq import NoPromotableChunksError, ReplicaChunkPromoter
from lsst.ppdb.gcp.db import ReplicaChunkDatabase
from lsst.ppdb.gcp.log_config import setup_logging

# Setup logging
setup_logging()

# Initialize the chunk tracking database
_replica_chunk_db = ReplicaChunkDatabase.from_env()


def promote_chunks(request: Request):
    """Promotes APDB replica chunks into production that have been copied into
    staging tables by the ``ChunkUploader`. Only chunks that are staged and
    have all prior chunks promoted will be considered for promotion.

    Parameters
    ----------
    request : Request
        The Flask request object containing the payload for promotion. This
        will typically be empty as the promotion is based on the current state
        of the database.

    Returns
    -------
    Response
        A JSON response indicating the success or failure of the promotion.
        This will include the number of chunks promoted and any error messages,
        if applicable.
    """
    promoted_count = 0
    try:
        # Fetch a list of promotable chunk IDs from the database
        promotable_chunks = _replica_chunk_db.get_promotable_chunks()

        # Promote the chunks using the ReplicaChunkPromoter
        promoter = ReplicaChunkPromoter()
        promoter.promote_chunks(promotable_chunks)

        # Mark the chunks as promoted in the chunk tracking database
        promoted_count = _replica_chunk_db.mark_chunks_promoted(promotable_chunks)
    except NoPromotableChunksError as e:
        logging.info("No promotable chunks found: %s", str(e))
        return jsonify(
            {"ok": True, "message": "No promotable chunks found", "chunks_promoted": 0}
        ), 200
    except Exception as e:
        logging.exception("Error during chunk promotion")
        return jsonify({"ok": False, "error": str(e), "chunks_promoted": 0}), 500
    return jsonify(
        {"ok": True, "mode": "execute", "chunks_promoted": promoted_count}
    ), 200
