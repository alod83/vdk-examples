# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging

from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)


class ColumnFormatter:
    def __init__(self, job_input: IJobInput):
        self.__job_input = job_input

    def format_column(self, table: str, column: str, occurrences: list, destination : str):

        for value in occurrences:
            query = f"UPDATE {table} SET {column} = '{destination}' WHERE {column} = '{value}'; "
            self.__job_input.execute_query(query)
            log.info(value)
        
        #log.info(f"Formatted table successfully.You can find the result here: {table}")


def run(job_input: IJobInput) -> None:
    table = job_input.get_arguments().get("table")
    column = job_input.get_arguments().get("column")
    occurrences = job_input.get_arguments().get("occurrences").split(",")
    destination = job_input.get_arguments().get("destination")
    
    formatter = ColumnFormatter(job_input)
    formatter.format_column(table, column, occurrences,destination)
