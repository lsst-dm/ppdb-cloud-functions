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
from datetime import timedelta

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from lsst.dax.ppdb import PpdbConfig
from lsst.dax.ppdb.bigquery import PpdbBigQueryConfig
from lsst.dax.ppdb.bigquery.export import DiaSourceMinSspExporter, string_to_bigquery_compression
from lsst.dax.ppdb.gcp import (
    cloud_run_initialize_sentry,
    flush_sentry,
    setup_cloud_logging,
)

cloud_run_initialize_sentry()

setup_cloud_logging()
_LOG = logging.getLogger("export_diasource_min_ssp")

_config = PpdbConfig.from_uri(os.environ["PPDB_CONFIG_URI"])
if not isinstance(_config, PpdbBigQueryConfig):
    raise TypeError(f"PPDB config has wrong type: {type(_config)}")

EXPORT_BUCKET = os.environ["EXPORT_BUCKET"]
EXPORT_COMPRESSION = string_to_bigquery_compression(os.environ["EXPORT_COMPRESSION"])
BIGQUERY_JOB_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_JOB_TIMEOUT_SECONDS"])
BIGQUERY_RETRY_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_RETRY_TIMEOUT_SECONDS"])

_exporter = DiaSourceMinSspExporter(
    _config,
    bigquery.Client(project=_config.project_id),
    bucket=EXPORT_BUCKET,
    compression=EXPORT_COMPRESSION,
    job_timeout=timedelta(seconds=BIGQUERY_JOB_TIMEOUT_SECONDS),
    retry_timeout=timedelta(seconds=BIGQUERY_RETRY_TIMEOUT_SECONDS),
)


@functions_framework.cloud_event
@flush_sentry
def export_diasource_min_ssp(event: CloudEvent) -> None:
    """Cloud Function that exports the DiaSource columns SSP needs to
    Parquet files in Cloud Storage after each successful promotion.

    Parameters
    ----------
    event
        The CloudEvent delivered by the Pub/Sub trigger on
        ``promotion-succeeded``. The message data should include
        ``promotion_time``, which names the export's folder.

    Notes
    -----
    Returning acknowledges the message and raising makes Pub/Sub retry it.
    `DiaSourceMinSspExporter.export_from_event` runs the export and logs the
    outcome. Every export failure is acknowledged, so a failed export isn't
    retried.
    """
    # Error handling is internal to the exporter. It doesn't raise for a
    # failed export, so the message is acknowledged and will not be retried.
    _exporter.export_from_event(event, _LOG)
