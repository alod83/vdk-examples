# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from sqlite3 import OperationalError
from unittest import mock

from click.testing import Result
from vdk.internal.core.errors import UserCodeError
from vdk.plugin.missing_values.ingest_without_missing_values import IngestWithoutMissingValues
from vdk.plugin.sqlite import sqlite_plugin
from vdk.plugin.sqlite.ingest_to_sqlite import IngestToSQLite
from vdk.plugin.sqlite.sqlite_configuration import SQLiteConfiguration
from vdk.plugin.test_utils.util_funcs import cli_assert_equal
from vdk.plugin.test_utils.util_funcs import CliEntryBasedTestRunner
#from vdk.plugin.test_utils.util_plugins import IngestIntoMemoryPlugin



def test_format_column(tmpdir):
    
    db_dir = str(tmpdir) + "vdk-sqlite.db"
    with mock.patch.dict(
        os.environ,
        {
            "VDK_DB_DEFAULT_TYPE": "SQLITE",
            "VDK_SQLITE_FILE": db_dir,
        },
    ):
        runner = CliEntryBasedTestRunner(sqlite_plugin)
        runner.invoke(
            [
                "sqlite-query",
                "--query",
                "CREATE TABLE test_table (city TEXT, country TEXT)",
            ]
        )

        context = None # where should I take context?
        
        ingest = IngestWithoutMissingValues(context)
        payload = [
            {"city": "Pisa", "country": ""},
            {"city": "Milano", "country": "Italia"},
            {"city": "Paris", "country": "France"},
        ]

        ingest.ingest_payload(
            payload=payload,
            destination_table="test_table",
            target=db_dir,
        )

        
        result = runner.invoke(
            [
                "sqlite-query",
                "--query",
                "SELECT country FROM test_table",
            ]
        )

        output = result.stdout
        
        assert output == (  
        "country\n"
        "---------\n"
        "Italia\n"
        "France\n")
        
