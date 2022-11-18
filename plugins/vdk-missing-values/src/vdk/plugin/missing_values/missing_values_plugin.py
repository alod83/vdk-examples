# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
from tabulate import tabulate
from vdk.api.plugin.hook_markers import hookimpl
from vdk.internal.builtin_plugins.run.job_context import JobContext
from vdk.plugin.missing_values.ingest_without_missing_values import IngestWithoutMissingValues


@hookimpl
def initialize_job(context: JobContext) -> None:
    context.ingester.add_ingester_factory_method(
        "MISSINGVALUES", lambda: IngestWithoutMissingValues(context)
    )

