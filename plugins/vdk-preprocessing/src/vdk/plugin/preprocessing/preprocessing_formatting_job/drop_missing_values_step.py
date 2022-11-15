# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging

from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)


class MissingValuesDropper:
    def __init__(self, job_input: IJobInput):
        self.__job_input = job_input

    def drop_missing_values(self, table: str, column : str):

        query = f"DELETE FROM {table} WHERE {column} IS NULL OR {column}  = '';"
        self.__job_input.execute_query(query)
        

def run(job_input: IJobInput) -> None:
    table = job_input.get_arguments().get("table")
    column = job_input.get_arguments().get("column")
    
    formatter = MissingValuesDropper(job_input)
    formatter.drop_missing_values(table, column)
