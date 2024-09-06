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
import psycopg2
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

    if "bigint" == type:
        return "bigint"
    elif "bigserial" == type:
        return "int"
    elif "bit" in type:
        return "string"
    elif "boolean" == type:
        return "boolean"
    elif "box" in type:
        return "string"
    elif "bytea" in type:
        return "binary"
    elif "character" in type:
        return "string"
    elif "character" in type:
        return "string"
    elif "cidr" in type:
        return "string"
    elif "circle" in type:
        return "string"
    elif "date" == type:
        return "date"
    elif "datetime" == type:
        return "timestamp"
    elif "double precision" in type:
        return "decimal(38,6)"
    elif "inet" in type:
        return "string"
    elif "integer" == type:
        return "int"
    elif "interval" == type:
        return "string"
    elif "json" in type:
        return "string"
    elif "jsonb" in type:
        return "string"
    elif "lseg" in type:
        return "string"
    elif "macaddr" in type:
        return "string"
    elif "macaddr8" in type:
        return "string"
    elif "money" in type:
        return "decimal(19,4)"
    elif "numeric" in type:
        return "decimal(38,18)"
    elif "path" in type:
        return "string"
    elif "pg_lsn" in type:
        return "string"
    elif "pg_snapshot" in type:
        return "string"
    elif "point" in type:
        return "string"
    elif "polygon" in type:
        return "string"
    elif "real" in type:
        return "decimal(19,4)"
    elif "smallint" in type:
        return "smallint"
    elif "smallserial" in type:
        return "int"
    elif "serial" in type:
        return "int"
    elif "text" in type:
        return "string"
    elif "timestamp" in type:
        return "timestamp"
    elif "time" in type:
        return "string"
    elif "tsquery" in type:
        return "string"
    elif "tsvector" in type:
        return "string"
    elif "txid_snapshot" in type:
        return "string"
    elif "uuid" == type:
        return "string"
    elif "xml" in type:
        return "string"
    elif "ARRAY" in type:
        return "array"
    elif "USER-DEFINED" in type:
        return "string"
    else:
        return "string"


class Connection:

    def __init__(self, hostname, port, username, password, database, schema):
        self.host = hostname
        self.port = port
        self.user = username
        self.password = password
        self.dbname = database
        self.schema = schema

    def get_schema(self):

        table_list = []

        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname,
            )

            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT
                    table_schema || '.' || table_name
                FROM
                    information_schema.tables
                WHERE
                    table_type = 'BASE TABLE'
                AND
                    table_schema = '{schema}';
                """.format(
                    schema=self.schema
                )
            )
            tables = cursor.fetchall()
            for table in tables:
                table_connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.dbname,
                )
                try:
                    table_name = table[0].split(".", 1)[1]
                    primary_key_query = """
                    SELECT
                        pg_attribute.attname
                    FROM 
                        pg_index, pg_class, pg_attribute, pg_namespace
                    WHERE
                        pg_class.oid = '{tableName}'::regclass AND indrelid = pg_class.oid AND
                        nspname = '{schema}' AND
                        pg_class.relnamespace = pg_namespace.oid AND
                        pg_attribute.attrelid = pg_class.oid AND
                        pg_attribute.attnum = any(pg_index.indkey)
                        AND indisprimary
                    """.format(
                        tableName=table_name, schema=self.schema
                    )
                    table_cursor = table_connection.cursor()
                    table_cursor.execute(primary_key_query)
                    response = table_cursor.fetchall()
                    # sample response [(primary_key,)]
                    if len(response) != 0:
                        primary_key = response[0][0]

                    sql_string = """
                        SELECT 
                            column_name, data_type, is_nullable
                        FROM 
                            information_schema.columns
                        WHERE 
                            table_name = '{tableName}' AND table_schema='{schema}';
                        """.format(
                        tableName=table_name, schema=self.schema
                    )

                    table_cursor = table_connection.cursor()
                    table_cursor.execute(sql_string)

                    rows = table_cursor.fetchall()
                    if len(rows) != 0:
                        row_list = []
                        for row in rows:
                            row_type = convert_schema(row[1])
                            row_list.append(
                                {
                                    "key": row[0],
                                    "value": row_type,
                                    "origin_type": row[1],
                                    "existing": True,
                                    "is_nullable": row[2],
                                }
                            )
                        if len(rows) != 0:
                            table_list.append(
                                {
                                    "table": table[0],
                                    "schema": row_list,
                                    "primary_key": primary_key,
                                }
                            )
                except Exception as e:
                    logger.error(traceback.format_exc())
                    raise
                finally:
                    table_connection.close()

            return table_list

        except Exception as e:
            logger.error(traceback.format_exc())
            raise
        finally:
            cursor.close()
            connection.close()
