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
from lsst.dax.ppdb.bigquery.sso import Loader
from lsst.dax.ppdb.gcp import (
    cloud_run_initialize_sentry,
    flush_sentry,
    setup_cloud_logging,
)

cloud_run_initialize_sentry()

# Configure cloud logging.
setup_cloud_logging()
_LOG = logging.getLogger("load_sso")

# The PPDB config gives the project and the landing and internal datasets.
_config = PpdbConfig.from_uri(os.environ["PPDB_CONFIG_URI"])
if not isinstance(_config, PpdbBigQueryConfig):
    raise TypeError(f"PPDB config has wrong type: {type(_config)}")

# BigQuery limits for the loader, set by Terraform.
BIGQUERY_JOB_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_JOB_TIMEOUT_SECONDS"])
BIGQUERY_RETRY_TIMEOUT_SECONDS = int(os.environ["BIGQUERY_RETRY_TIMEOUT_SECONDS"])

# Created once per instance and shared by all requests, so that the clients
# are reused.
_loader = Loader(
    _config,
    bigquery.Client(project=_config.project_id),
    job_timeout=timedelta(seconds=BIGQUERY_JOB_TIMEOUT_SECONDS),
    retry_timeout=timedelta(seconds=BIGQUERY_RETRY_TIMEOUT_SECONDS),
)


@functions_framework.cloud_event
@flush_sentry
def load_sso(event: CloudEvent) -> None:
    """Cloud Function that loads an upload of SSO parquet files into the
    internal SSO tables.

    Parameters
    ----------
    event
        The CloudEvent delivered by the Pub/Sub trigger. The message data
        should include ``bucket`` and ``object_prefix``, the location of the
        upload in Cloud Storage.

    Notes
    -----
    Returning acknowledges the message and raising makes Pub/Sub retry it.
    `Loader.load_from_event` decodes the message, loads the upload, logs the
    outcome and publishes the result. Bad messages and every load failure
    are acknowledged, so a failed load isn't retried.

    Only one load runs at a time, because the function runs on at most one
    instance, handling one message at a time.

    The result of each load, whether it succeeded or failed, is published to
    the ``load-sso-result`` topic, where ``ppdb-cli upload-sso-data``
    waits for it.
    """
    # Error handling is internal to the loader. It doesn't raise for a bad
    # message or a failed load, so the message is acknowledged and will not
    # be retried.
    _loader.load_from_event(event, _LOG)
