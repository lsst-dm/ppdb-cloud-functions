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

from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.chunk_promoter import (
    ChunkPromoter,
    ChunkPromotionError,
    NoPromotableChunksError,
)
from lsst.dax.ppdb.gcp import CloudEventLogger, setup_cloud_logging

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("promote_chunks")


def promote_chunks():
    logger = CloudEventLogger(_LOG)

    promotable_chunks = ppdb.get_promotable_chunks()
    chunk_count = len(promotable_chunks)
    logger.log_event(
        logging.INFO,
        "Found promotable chunks",
        "promotable_chunks_found",
        chunk_count=chunk_count,
    )

    try:
        # Execute the promotion process.
        promoter = ChunkPromoter(ppdb)
        promoter.promote_chunks(promotable_chunks)
    except NoPromotableChunksError as e:
        # No promotable chunks were found. This is handled as an error
        # condition for control flow but may occur normally if no new chunks
        # were staged for promotion. The message is emitted at `WARNING` level
        # to improve visibility. No error is raised in this circumstance.
        logger.log_event(
            logging.WARNING,
            "No promotable chunks found",
            "no_promotable_chunks",
            error=e,
        )
    except ChunkPromotionError as e:
        # An error occurred during the promotion process which was trapped by
        # the `ChunkPromoter` instance. Re-raise the exception to propagate
        # the error.
        logger.log_event(
            logging.ERROR,
            "Error during chunk promotion",
            "chunk_promotion_error",
            error=e,
        )
        raise
    except Exception as e:
        # Catch any other unexpected exceptions so they can be logged. This
        # should not occur under normal circumstances as `ChunkPromoter` is
        # designed to handle all expected errors. Re-raise the unexpected
        # exception to propagate the error.
        logger.log_event(
            logging.ERROR,
            "Unexpected error while promoting chunks",
            "unexpected_promotion_error",
            error=e,
        )
        raise

    # Promotion succeeded! Log the number of chunks promoted.
    logger.log_event(
        logging.INFO,
        "Chunks promoted",
        "chunks_promoted",
        chunk_count=chunk_count,
    )


if __name__ == "__main__":
    _LOG.info("Promote Chunks Job starting")
    ppdb = PpdbBigQuery.from_env()
    promote_chunks()
    _LOG.info("Promote Chunks Job finished")
