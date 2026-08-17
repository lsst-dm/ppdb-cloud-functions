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
from google.cloud.logging.handlers import StructuredLogHandler
from google.cloud.logging_v2.handlers.container_engine import ContainerEngineHandler
from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.chunk_promoter import (
    ChunkPromoter,
    ChunkPromotionError,
    NoPromotableChunksError,
)

def setup_logging():
    # Set up stdout structured logging
    handler = StructuredLogHandler()
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

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
    except ChunkPromotionError as e:
        # Some error occurred during the promotion process.
        logging.exception("Error during chunk promotion: %s", str(e))
        raise
    except Exception as e:
        logging.exception("Unexpected error while promoting chunks: %s", str(e))
        raise

    # Promotion succeeded! Return the number of chunks promoted.
    logging.info("Chunks promoted: %s", str(chunk_count))

if __name__ == "__main__":
    setup_logging()
    logging.info("Promote Chunks Job starting")
    # Setup PPDB BigQuery interface from environment variable configuration
    ppdb = PpdbBigQuery.from_env()
    promote_chunks()