""" 
Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

  http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
"""

from contextlib import nullcontext
import pymysql
import traceback
import os
import logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger()

if logger.hasHandlers():
    logger.setLevel(LOG_LEVEL)
else:
    logging.basicConfig(level=LOG_LEVEL)


class Connection:
    def __init__(self, hostname, port, username, password, database):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def get_query_results(self, query):
        db_connection = None
        try:
            db_connection = pymysql.connect(
                host=self.hostname,
                user=self.username,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            cursor = db_connection.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return response
        except Exception as e:
            logger.error(traceback.format_exc())
            raise e
        finally:
            if db_connection:
                db_connection.close()
