# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import os
from sqlite3 import OperationalError
from unittest import mock

from click.testing import Result
from vdk.internal.core.errors import UserCodeError
from vdk.plugin.preprocessing import preprocessing_plugin
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
        runner = CliEntryBasedTestRunner(sqlite_plugin, preprocessing_plugin)
        runner.invoke(
            [
                "sqlite-query",
                "--query",
                "CREATE TABLE test_table (city TEXT, country TEXT)",
            ]
        )

        mock_sqlite_conf = mock.MagicMock(SQLiteConfiguration)
        sqlite_ingest = IngestToSQLite(mock_sqlite_conf)
        payload = [
            {"city": "Pisa", "country": "Italie"},
            {"city": "Milano", "country": "Italia"},
            {"city": "Paris", "country": "France"},
        ]

        sqlite_ingest.ingest_payload(
            payload=payload,
            destination_table="test_table",
            target=db_dir,
        )
        result = runner.invoke(["format-column", "--table", "test_table", "--column", "country", "--occurrences", "Italie,Italia", "--destination", "Italy"])
        
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
        "Italy\n"
        "Italy\n"
        "France\n")
        
def test_drop_duplicates(tmpdir):
    
    db_dir = str(tmpdir) + "vdk-sqlite.db"
    with mock.patch.dict(
        os.environ,
        {
            "VDK_DB_DEFAULT_TYPE": "SQLITE",
            "VDK_SQLITE_FILE": db_dir,
        },
    ):
        runner = CliEntryBasedTestRunner(sqlite_plugin, preprocessing_plugin)
        runner.invoke(
            [
                "sqlite-query",
                "--query",
                "CREATE TABLE test_table (city TEXT, country TEXT)",
            ]
        )

        mock_sqlite_conf = mock.MagicMock(SQLiteConfiguration)
        sqlite_ingest = IngestToSQLite(mock_sqlite_conf)
        payload = [
            {"city": "Pisa", "country": "Italy"},
            {"city": "Pisa", "country": "Italy"},
            {"city": "Paris", "country": "France"},
        ]

        sqlite_ingest.ingest_payload(
            payload=payload,
            destination_table="test_table",
            target=db_dir,
        )
        result = runner.invoke(["drop-duplicates", "--table", "test_table", "--destination", "cleaned_table"])
        
        result = runner.invoke(
            [
                "sqlite-query",
                "--query",
                "SELECT country FROM cleaned_table",
            ]
        )

        output = result.stdout

        
        assert output == (  
        "country\n"
        "---------\n"
        "Italy\n"
        "France\n")
        
def test_drop_missing_values(tmpdir):
    
    db_dir = str(tmpdir) + "vdk-sqlite.db"
    with mock.patch.dict(
        os.environ,
        {
            "VDK_DB_DEFAULT_TYPE": "SQLITE",
            "VDK_SQLITE_FILE": db_dir,
        },
    ):
        runner = CliEntryBasedTestRunner(sqlite_plugin, preprocessing_plugin)
        runner.invoke(
            [
                "sqlite-query",
                "--query",
                "CREATE TABLE test_table (city TEXT, country TEXT)",
            ]
        )

        mock_sqlite_conf = mock.MagicMock(SQLiteConfiguration)
        sqlite_ingest = IngestToSQLite(mock_sqlite_conf)
        payload = [
            {"city": "Pisa", "country": "Italy"},
            {"city": "Milan", "country": ""},
            {"city": "Paris", "country": "France"},
        ]

        sqlite_ingest.ingest_payload(
            payload=payload,
            destination_table="test_table",
            target=db_dir,
        )
        result = runner.invoke(["drop-missing-values", "--table", "test_table", "--column", "country"])
        
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
        "Italy\n"
        "France\n")
        
