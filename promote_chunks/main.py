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

from lsst.dax.ppdbx.gcp.log_config import setup_logging
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.replica_chunk_promoter import (
    ChunkPromoter,
    NoPromotableChunksError,
)

# Configure cloud logging
setup_logging()

# Setup PPDB BigQuery interface from environment variable configuration
ppdb = PpdbBigQuery.from_env()


def promote_chunks(request: Request):
    """Promotes APDB replica chunks into production that have been copied into
    staging tables by the ``ChunkUploader`. Only chunks that are staged and
    have all prior chunks promoted will be considered for promotion.

    Parameters
    ----------
    request : `Request`
        The Flask request object containing the payload for promotion. This
        will typically be empty as the promotion is based on the current state
        of the database.

    Returns
    -------
    response: `Response`
        A JSON response indicating the success or failure of the promotion.
        This will include the number of chunks promoted and any error messages,
        if applicable.
    """
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    # Execute dry run if requested and just print the IDs of the promotable
    # chunks without making any changes
    if dry_run:
        logging.info("Dry run mode enabled - promotion will not be executed")
        promotable_chunks = ppdb.get_promotable_chunks()
        return jsonify(
            {
                "ok": True,
                "mode": "dry_run",
                "chunks_promoted": 0,
                "promotable_chunk_count": len(promotable_chunks),
            }
        ), 200

    # Promote the chunks and return the number promoted
    try:
        promoter = ChunkPromoter(ppdb)
        promoter.promote_chunks()
    except NoPromotableChunksError as e:
        # This is not a real error condition. It just means there are no chunks
        # ready for promotion. It is easiest to catch this as an exception.
        logging.info("No promotable chunks found: %s", str(e))
        return jsonify(
            {"ok": True, "message": "No promotable chunks found", "chunks_promoted": 0}
        ), 200
    except Exception as e:
        # Some error occurred during the promotion process.
        logging.exception("Error during chunk promotion")
        return jsonify({"ok": False, "error": str(e), "chunks_promoted": 0}), 500
    # Promotion succeeded! Return the number of chunks promoted
    return jsonify(
        {
            "ok": True,
            "mode": "execute",
            "chunks_promoted": len(promoter.promotable_chunks),
        }
    ), 200
