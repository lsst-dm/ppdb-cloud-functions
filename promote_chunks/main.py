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
import os

from google.cloud import logging as cloud_logging
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.chunk_promoter import (
    ChunkPromoter,
    NoPromotableChunksError,
)

# Configure cloud logging.
client = cloud_logging.Client()
client.setup_logging()  # Redirects standard logging to Cloud Logging
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
logging.getLogger().setLevel(log_level)


# Setup PPDB BigQuery interface from environment variable configuration
ppdb = PpdbBigQuery.from_env()


def promote_chunks():

    promotable_chunks = ppdb.get_promotable_chunks()
    chunk_count = len(promotable_chunks)
    logging.info("Found %d promotable chunks", chunk_count)

    # Promote the chunks and log the number promoted.
    try:
        promoter = ChunkPromoter(ppdb)
        promoter.promote_chunks(promotable_chunks)
    except NoPromotableChunksError as e:
        # This is not a real error condition. It just means there are no chunks
        # ready for promotion. It is easiest to catch this as an exception.
        logging.info("No promotable chunks found: %s", str(e))

    except Exception as e:
        # Some error occurred during the promotion process.
        logging.exception("Error during chunk promotion")
        raise

    # Promotion succeeded! Return the number of chunks promoted.
    logging.info("%s, Chunks Promoted", str(chunk_count))

if __name__ == "__main__":
    promote_chunks()