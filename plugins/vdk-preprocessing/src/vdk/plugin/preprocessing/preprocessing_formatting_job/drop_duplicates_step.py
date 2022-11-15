# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging

from vdk.api.job_input import IJobInput

log = logging.getLogger(__name__)


class DuplicateDropper:
    def __init__(self, job_input: IJobInput):
        self.__job_input = job_input

    def drop_duplicates(self, table: str, destination : str):

        query = f"CREATE TABLE {destination} as SELECT DISTINCT * FROM {table};"
        self.__job_input.execute_query(query)
        

def run(job_input: IJobInput) -> None:
    table = job_input.get_arguments().get("table")
    destination = job_input.get_arguments().get("destination")
    
    formatter = DuplicateDropper(job_input)
    formatter.drop_duplicates(table, destination)
