# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
"""
VDK Preprocessing plugin script.
"""
import json
import logging
import os

import click
from click import Context
from vdk.api.plugin.hook_markers import hookimpl
from vdk.internal.builtin_plugins.run.cli_run import run
from vdk.internal.core import errors
from vdk.plugin.preprocessing.preprocessing_formatting_job import format_column_step
from vdk.plugin.preprocessing.preprocessing_formatting_job import drop_duplicates_step
from vdk.plugin.preprocessing.preprocessing_formatting_job import drop_missing_values_step


log = logging.getLogger(__name__)

# Format Column
@click.command(
    name="format-column",
    help="Execute a SQL query against a configured database and format the table.",
    no_args_is_help=True,
)
@click.option(
    "-t",
    "--table",
    help="The name of the table.",
    default="my_table",
    type=click.STRING,
)

@click.option(
    "-c",
    "--column",
    help="The column to format.",
    default="my_column",
    type=click.STRING,
)

@click.option(
    "-o",
    "--occurrences",
    help="The list of occurrences to modify separated by comma.",
    default="",
    type=click.STRING,
)

@click.option(
    "-d",
    "--destination",
    help="The target value.",
    default="",
    type=click.STRING,
)

@click.pass_context
def format_column(ctx: click.Context, table: str, column: str, occurrences : str, destination : str):
    
    args = dict(table=table, column=column, occurrences=occurrences, destination=destination)
    ctx.invoke(
        run,
        data_job_directory=os.path.dirname(format_column_step.__file__),
        arguments=json.dumps(args),
    )

# Drop duplicates
@click.command(
    name="drop-duplicates",
    help="Execute a SQL query against a configured database and drop duplicates.",
    no_args_is_help=True,
)
@click.option(
    "-t",
    "--table",
    help="The name of the table.",
    default="my_table",
    type=click.STRING,
)
@click.option(
    "-d",
    "--destination",
    help="The name of the destination table.",
    default="",
    type=click.STRING,
)

@click.pass_context
def drop_duplicates(ctx: click.Context, table: str, destination : str):
    
    args = dict(table=table, destination=destination)
    ctx.invoke(
        run,
        data_job_directory=os.path.dirname(drop_duplicates_step.__file__),
        arguments=json.dumps(args),
    )


# Drop missing values
@click.command(
    name="drop-missing-values",
    help="Execute a SQL query against a configured database and drop missing values related to a column.",
    no_args_is_help=True,
)
@click.option(
    "-t",
    "--table",
    help="The name of the table.",
    default="my_table",
    type=click.STRING,
)
@click.option(
    "-d",
    "--column",
    help="The name of the column to clean.",
    default="",
    type=click.STRING,
)

@click.pass_context
def drop_missing_values(ctx: click.Context, table: str, column : str):
    
    args = dict(table=table, column=column)
    ctx.invoke(
        run,
        data_job_directory=os.path.dirname(drop_missing_values_step.__file__),
        arguments=json.dumps(args),
    )


@hookimpl
def vdk_command_line(root_command: click.Group) -> None:
    root_command.add_command(format_column)
    root_command.add_command(drop_duplicates)
    root_command.add_command(drop_missing_values)
    
