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

from lsst.dax.ppdb.bigquery import PpdbBigQuery
from lsst.dax.ppdb.bigquery.chunk_promoter import (
    ChunkPromoter,
    ChunkPromotionError,
    NoPromotableChunksError,
)
from lsst.dax.ppdb.gcp import (
    CloudEventLogger,
    # setup_cloud_logging,
)

from google.cloud.logging_v2.handlers import StructuredLogHandler, setup_logging


def _debug_logging(phase: str, logger: logging.Logger | None = None) -> None:
    logger = logger or logging.getLogger()
    logging.info(
        f"::::{phase} Active logging handlers: %s",
        [type(h).__name__ for h in logger.handlers],
    )
    # for var in [
    #     "GAE_SERVICE",
    #     "GAE_VERSION",
    #     "GAE_INSTANCE",
    #     "FUNCTION_TARGET",
    #     "FUNCTION_SIGNATURE_TYPE",
    #     "FUNCTION_NAME",
    #     "FUNCTION_REGION",
    #     "ENTRY_POINT",
    #     "K_SERVICE",
    #     "K_REVISION",
    #     "K_CONFIGURATION",
    #     "CLOUD_RUN_JOB",
    #     "CLOUD_RUN_EXECUTION",
    #     "CLOUD_RUN_TASK_INDEX",
    #     "CLOUD_RUN_TASK_ATTEMPT",
    # ]:
    #     logging.info(
    #         f"::::{phase} {var}: In os.environ: {var in os.environ}, {os.environ.get(var)}"
    #     )


def setup_cloud_logging() -> None:
    """Set up Cloud Logging and configure the root logger."""
    # _debug_logging(phase="start-setup-cloud-logging")
    # cloud_logging.Client().setup_logging()  # Redirects standard logging to Cloud Logging

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    setup_logging(StructuredLogHandler(), log_level=log_level)
    # _debug_logging(phase="mid-setup-cloud-logging")

    # Silence noisy warnings from google-auth-httplib2.
    logging.getLogger("google_auth_httplib2").setLevel(logging.ERROR)
    # _debug_logging(phase="finish-setup-cloud-logging")


# _debug_logging(phase="outside-before-sentry")
# cloud_run_initialize_sentry()

# Configure cloud logging.
# _debug_logging(phase="outside-before-setup")
setup_cloud_logging()
# _debug_logging(phase="outside-after-setup")
_LOG = logging.getLogger("promote_chunks")


def promote_chunks():
    logger = CloudEventLogger(_LOG)
    ppdb = PpdbBigQuery.from_env()

    # _debug_logging(phase="inside", logger=_LOG)

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
        promoter = ChunkPromoter(ppdb, logger)
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


# @flush_sentry
def main() -> None:
    _LOG.info("Promote Chunks Job starting")
    promote_chunks()
    _LOG.info("Promote Chunks Job finished")


if __name__ == "__main__":
    main()
