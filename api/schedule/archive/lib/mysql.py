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


def convert_schema(type):

    if "char" in type:
        if type == "varchar":
            return "varchar"
        else:
            return "string"
    elif "boolean" == type or "tinyint(1)" == type:
        return "boolean"
    elif "bigint" == type:
        return "bigint"
    elif "smallint" == type:
        return "smallint"
    elif "int" in type:
        return "int"
    elif "date" == type:
        return "date"
    elif "datetime" in type:
        return "timestamp"
    elif "enum" in type:
        return "string"
    elif type in ["text", "longtext", "mediumtext", "tinytext"]:
        return "string"
    elif "decimal" in type:
        return "decimal"
    elif "json" in type:
        return "string"
    elif "uuid" == type:
        return "string"
    else:
        return "string"


class Connection:
    def __init__(self, hostname, port, username, password, database):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def _fetch_table_schema(self, table_name):
        try:
            table_connection = pymysql.connect(
                host=self.hostname,
                user=self.username,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            table_cursor = table_connection.cursor()
            table_cursor.execute(f"DESCRIBE {self.database}.{table_name}")
            row_list = []
            primary_key = ""
            for row in table_cursor.fetchall():
                if row["Key"] == "PRI" and "int" in row["Type"]:
                    primary_key = row["Field"]
                row_type = convert_schema(row["Type"])
                row_list.append(
                    {
                        "key": row["Field"],
                        "value": row_type,
                        "origin_type": row["Type"],
                        "existing": True,
                    }
                )
            return {
                "table": table_name,
                "schema": row_list,
                "primary_key": primary_key,
            }
        except Exception as e:
            logger.error(traceback.format_exc())
            raise
        finally:
            table_connection.close()

    def get_schema(self, tables=None):

        table_list = []

        try:
            connection = pymysql.connect(
                host=self.hostname,
                user=self.username,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )

            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            for table in tables:
                table_name = list(table.values())[0]
                table_schema = self._fetch_table_schema(table_name=table_name)
                if table_schema:
                    table_list.append(table_schema)

            return table_list

        except Exception as e:
            logger.error(traceback.format_exc())
            raise
        finally:
            cursor.close()
            connection.close()

    def get_schema_by_tables(self, table_names):
        table_list = []
        try:
            for table_name in table_names:
                table_schema = self._fetch_table_schema(table_name=table_name)
                if table_schema:
                    table_list.append(table_schema)
            return table_list
        except Exception as e:
            logger.error(traceback.format_exc())
            raise
