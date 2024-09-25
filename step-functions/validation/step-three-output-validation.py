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

import boto3
import os
from lib import mysql
from lib import mssql
from lib import oracle
from lib import postgresql


REGION = os.getenv("REGION")
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
secret_client = boto3.client("secretsmanager", region_name=REGION)

PARTITION_COLUMN = "created_at"


def get_table_index(TABLE_NAME, archival_record):
    for index, item in enumerate(archival_record["table_details"]):
        if item["table"] == TABLE_NAME:
            return index
    return None


def get_source_query_map(table_name, sub_query, row_key, validation_type):
    if validation_type == "count_validation":
        query = f"SELECT COUNT(*) AS SOURCE_ROWCOUNT FROM {table_name}"
        if sub_query:
            query += sub_query
    elif validation_type == "number_validation":
        query = f"SELECT SUM('{row_key}') AS SOURCE_NUMBERCOUNT FROM {table_name}"
        if sub_query:
            query += sub_query
    elif validation_type == "string_validation":
        query = f"SELECT SUM(LENGTH('{row_key}') - LENGTH(REPLACE('{row_key}', ' ', '')) + 1) AS SOURCE_WORDCOUNT FROM {table_name}"
        if sub_query:
            query += sub_query
    return query


def update_dynamo_db(
    archive_id, table_index, validation_type, query, status_message, results
):
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    table = dynamodb.Table(parameter["Parameter"]["Value"])
    table.update_item(
        Key={"id": archive_id},
        UpdateExpression=f"set table_details[{table_index}].{validation_type}.source_database = :newJob",
        ExpressionAttributeValues={
            ":newJob": {"query": query, "state": status_message, "results": results}
        },
    )


def lambda_handler(event, context):
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    table = dynamodb.Table(parameter["Parameter"]["Value"])

    for event_record in event:
        TABLE_NAME = event_record["table"]
        DATABASE_NAME = event_record["database"]
        ARCHIVE_ID = event_record["archive_id"]
        DATABASE_ENGINE = event_record["database_engine"]
        VALIDATION_TYPE = event_record["validation_type"]
        ROW_KEY = None
        if VALIDATION_TYPE != "count_validation":
            ROW_KEY = event_record["key"]

        dynamodb_response = table.get_item(Key={"id": ARCHIVE_ID})
        secret_value = secret_client.get_secret_value(
            SecretId=dynamodb_response["Item"]["secret_arn"]
        )
        hostname = dynamodb_response["Item"]["hostname"]
        port = dynamodb_response["Item"]["port"]
        username = dynamodb_response["Item"]["username"]
        password = secret_value["SecretString"]
        database = DATABASE_NAME

        ARCHIVAL_START_DATE = dynamodb_response["Item"]["archival_start_date"]
        ARCHIVAL_END_DATE = dynamodb_response["Item"]["archival_end_date"]
        sub_query = None
        if ARCHIVAL_START_DATE and ARCHIVAL_END_DATE:
            sub_query = f" WHERE {PARTITION_COLUMN} BETWEEN '{ARCHIVAL_START_DATE}' AND '{ARCHIVAL_END_DATE}'"

        table_index = get_table_index(
            archival_record=dynamodb_response["Item"], TABLE_NAME=TABLE_NAME
        )

        query = get_source_query_map(
            table_name=TABLE_NAME,
            sub_query=sub_query,
            row_key=ROW_KEY,
            validation_type=VALIDATION_TYPE,
        )

        results = None
        status_message = None
        if DATABASE_ENGINE == "mysql":
            connection = mysql.Connection(hostname, port, username, password, database)
            try:
                response = connection.get_query_results(query=query)
                if response and len(response) > 0:
                    results = response[0]
                    status_message = "SUCCEEDED"
            except:
                status_message = "FAILED"
            finally:
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results=results,
                    status_message=status_message,
                )

        elif DATABASE_ENGINE == "mssql":
            connection = mssql.Connection(hostname, port, username, password, database)
            try:
                response = connection.get_query_results(query=query)
                if response and len(response) > 0:
                    results = response[0]
                    status_message = "SUCCEEDED"
            except:
                status_message = "FAILED"
            finally:
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results=results,
                    status_message=status_message,
                )
        elif DATABASE_ENGINE == "postgresql":
            connection = postgresql.Connection(
                hostname, port, username, password, database, schema="public"
            )
            try:
                response = connection.get_query_results(query=query)
                if response and len(response) > 0:
                    results = response[0]
                    status_message = "SUCCEEDED"
            except:
                status_message = "FAILED"
            finally:
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results=results,
                    status_message=status_message,
                )
        elif DATABASE_ENGINE == "oracle":
            oracle_owner = dynamodb_response["Item"]["oracle_owner"]
            oracle_owner_list = oracle_owner.split(",")
            for owner in oracle_owner_list:
                oracle_connection = oracle.Connection(
                    hostname, port, username, password, database, owner
                )
                try:
                    response = oracle_connection.get_query_results(query=query)
                    if response and len(response) > 0:
                        results = response[0]
                        status_message = "SUCCEEDED"
                except:
                    status_message = "FAILED"
                finally:
                    update_dynamo_db(
                        archive_id=ARCHIVE_ID,
                        table_index=table_index,
                        validation_type=VALIDATION_TYPE,
                        query=query,
                        results=results,
                        status_message=status_message,
                    )
    return event
