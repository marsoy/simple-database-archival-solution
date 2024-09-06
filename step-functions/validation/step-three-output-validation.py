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

import json
import boto3
import os
import time
from lib import mysql
from lib import mssql
from lib import oracle
from lib import postgresql


REGION = os.getenv("REGION")
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
secret_client = boto3.client("secretsmanager", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)

DEFAULT_PARTITION_COLUMN = "created_at"

RESULT_KEY_MAP = {
    "count_validation": "ROWCOUNT",
    "number_validation": "NUMBERCOUNT",
    "string_validation": "WORDCOUNT",
}


def get_table_index(TABLE_NAME, archival_record):
    for index, item in enumerate(archival_record["table_details"]):
        if item["table"] == TABLE_NAME:
            return item, index
    return None, None


def get_source_query_map(
    table_name, range_filter_query, row_key, validation_type, related_table_details
):
    if validation_type == "count_validation":
        select_query = f"SELECT COUNT(*) AS ROWCOUNT"
    elif validation_type == "number_validation":
        select_query = f"SELECT SUM({row_key}) AS NUMBERCOUNT"
    elif validation_type == "string_validation":
        select_query = f"SELECT SUM(LENGTH({row_key}) - LENGTH(REPLACE({row_key}, ' ', '')) + 1) AS WORDCOUNT"

    if related_table_details:
        table_column = related_table_details.get("table_column")
        fk_table_column = related_table_details.get("fk_table_column")
        fk_table = related_table_details.get("fk_table")
        sql_query = f" FROM {table_name} ot INNER JOIN {fk_table} fkr ON ot.{table_column} = fkr.{fk_table_column}"
    else:
        sql_query = f" FROM {table_name}"
    return select_query + sql_query + range_filter_query


def update_dynamo_db(
    archive_id,
    table_index,
    validation_type,
    query,
    results,
    status_message,
    error_message,
):
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    table = dynamodb.Table(parameter["Parameter"]["Value"])
    table.update_item(
        Key={"id": archive_id},
        UpdateExpression=f"set table_details[{table_index}].{validation_type}.source_database = :newJob",
        ExpressionAttributeValues={
            ":newJob": {
                "query": query,
                "state": status_message,
                "results": results,
                "error_message": error_message,
            }
        },
    )


def lambda_handler(event, context):
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    table = dynamodb.Table(parameter["Parameter"]["Value"])

    sqs_parameter = ssm.get_parameter(Name="/sqs/validation", WithDecryption=True)
    sqs_parameter_value = sqs_parameter["Parameter"]["Value"]
    print(sqs_parameter_value)

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

        table_detail, table_index = get_table_index(
            archival_record=dynamodb_response["Item"], TABLE_NAME=TABLE_NAME
        )
        partition_column = table_detail.get("partition_column")
        use_related = table_detail.get("use_related", False)
        related_table_details = None
        if use_related:
            related_table_details = table_detail.get("related_table_details")
            partition_column = related_table_details.get("fk_partition_column")
        elif not partition_column:
            partition_column = DEFAULT_PARTITION_COLUMN

        ARCHIVAL_START_DATE = dynamodb_response["Item"]["archival_start_date"]
        ARCHIVAL_END_DATE = dynamodb_response["Item"]["archival_end_date"]
        range_filter_query = None
        if ARCHIVAL_START_DATE and ARCHIVAL_END_DATE:
            range_filter_query = f" WHERE {partition_column} BETWEEN '{ARCHIVAL_START_DATE}' AND '{ARCHIVAL_END_DATE}'"

        query = get_source_query_map(
            table_name=TABLE_NAME,
            range_filter_query=range_filter_query,
            row_key=ROW_KEY,
            validation_type=VALIDATION_TYPE,
            related_table_details=related_table_details,
        )

        update_dynamo_db(
            archive_id=ARCHIVE_ID,
            table_index=table_index,
            validation_type=VALIDATION_TYPE,
            query=query,
            results={},
            status_message="RUNNING",
            error_message=None,
        )

        error_message = None
        if DATABASE_ENGINE == "mysql":
            connection = mysql.Connection(hostname, port, username, password, database)
            try:
                response = connection.get_query_results(query=query)
                if response and len(response) > 0:
                    for key, value in response[0].items():
                        results = {"key": key, "value": value}
                    status_message = "SUCCEEDED"
                    update_dynamo_db(
                        archive_id=ARCHIVE_ID,
                        table_index=table_index,
                        validation_type=VALIDATION_TYPE,
                        query=query,
                        results=results,
                        status_message=status_message,
                        error_message=error_message,
                    )
            except Exception as e:
                status_message = "FAILED"
                error_message = str(e)
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results={},
                    status_message=status_message,
                    error_message=error_message,
                )

        elif DATABASE_ENGINE == "mssql":
            connection = mssql.Connection(hostname, port, username, password, database)
            try:
                response = connection.get_query_results(query=query)
                if response and len(response) > 0:
                    for key, value in response[0].items():
                        results = {"key": key, "value": value}
                    status_message = "SUCCEEDED"
                    update_dynamo_db(
                        archive_id=ARCHIVE_ID,
                        table_index=table_index,
                        validation_type=VALIDATION_TYPE,
                        query=query,
                        results=results,
                        status_message=status_message,
                        error_message=error_message,
                    )
            except Exception as e:
                status_message = "FAILED"
                error_message = str(e)
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results={},
                    status_message=status_message,
                    error_message=error_message,
                )
        elif DATABASE_ENGINE == "postgresql":
            connection = postgresql.Connection(
                hostname, port, username, password, database, schema="public"
            )
            try:
                response = connection.get_query_results(query=query)
                print(f"Postgresql response: {response}")
                if response and len(response) > 0:
                    (value,) = response[0]
                    results = {"key": RESULT_KEY_MAP[VALIDATION_TYPE], "value": value}
                    status_message = "SUCCEEDED"
                    update_dynamo_db(
                        archive_id=ARCHIVE_ID,
                        table_index=table_index,
                        validation_type=VALIDATION_TYPE,
                        query=query,
                        results=results,
                        status_message=status_message,
                        error_message=error_message,
                    )
            except Exception as e:
                status_message = "FAILED"
                error_message = str(e)
                update_dynamo_db(
                    archive_id=ARCHIVE_ID,
                    table_index=table_index,
                    validation_type=VALIDATION_TYPE,
                    query=query,
                    results={},
                    status_message=status_message,
                    error_message=error_message,
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
                        for key, value in response[0].items():
                            results = {"key": key, "value": value}
                        status_message = "SUCCEEDED"
                        update_dynamo_db(
                            archive_id=ARCHIVE_ID,
                            table_index=table_index,
                            validation_type=VALIDATION_TYPE,
                            query=query,
                            results=results,
                            status_message=status_message,
                            error_message=error_message,
                        )
                except Exception as e:
                    status_message = "FAILED"
                    error_message = str(e)
                    update_dynamo_db(
                        archive_id=ARCHIVE_ID,
                        table_index=table_index,
                        validation_type=VALIDATION_TYPE,
                        query=query,
                        results={},
                        status_message=status_message,
                        error_message=error_message,
                    )

        # Send message to SQS queue
        query_execution_id = dynamodb_response["Item"]["table_details"][table_index][
            VALIDATION_TYPE
        ]["archived"]["query_execution_id"]
        message = {
            "archive_id": ARCHIVE_ID,
            "table_index": table_index,
            "validation_type": VALIDATION_TYPE,
        }
        response = sqs.send_message(
            QueueUrl=str(sqs_parameter_value),
            MessageGroupId=ARCHIVE_ID,
            MessageDeduplicationId=str(query_execution_id),
            MessageBody=json.dumps(message),
        )
        print(response)

        # Adding sleep for 1 sec
        time.sleep(1)
    return event
