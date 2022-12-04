# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import List
from typing import Optional
from typing import Tuple
import pandas as pd

from vdk.api.plugin.plugin_input import IIngesterPlugin
from vdk.internal.builtin_plugins.run.job_context import JobContext
from vdk.internal.core import errors

_log = logging.getLogger(__name__)


class IngestWithoutMissingValues(IIngesterPlugin):
    """
    Create a new ingestion mechanism for ingesting without missing values
    """

    def __init__(self, context: JobContext):
        self._context = context

    def pre_ingest_process(self, payload):

        # Ensure all values in the payload are strings
        metadata = IIngesterPlugin.IngestionMetadata({})
        df = pd.DataFrame.from_dict(payload)
        df.dropna(inplace=True)
        
        return df.to_dict(orient='records'), metadata

    