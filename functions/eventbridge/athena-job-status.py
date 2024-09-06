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
import json
import os

REGION = os.getenv("REGION")

client = boto3.client("athena", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm")


# Get Athena Response
def get_athena_response(query_execution_id):
    response = client.get_query_results(
        QueryExecutionId=query_execution_id, MaxResults=123
    )
    return response


def get_athena_result_map(response):
    rows = response["ResultSet"]["Rows"]
    if len(rows) != 0:
        key = rows[0]["Data"][0]["VarCharValue"]
        row_data = rows[1]["Data"][0]
        value = None
        if len(row_data) != 0:
            value = row_data["VarCharValue"]
        return {"key": key, "value": value}
    return {}


# Set Job State Function
def update_validation_state(
    archive_id,
    query_execution_id,
    table_name,
    validation_type,
    athena_response,
    query,
    row_key,
    status_message,
):
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    table = dynamodb.Table(parameter["Parameter"]["Value"])
    dynamodb_response = table.get_item(Key={"id": archive_id})

    for index, item in enumerate(dynamodb_response["Item"]["table_details"]):
        if item["table"] == table_name:
            table.update_item(
                Key={"id": archive_id},
                UpdateExpression=f"set table_details[{index}].{validation_type}.row_key = :s",
                ExpressionAttributeValues={":s": row_key},
                ReturnValues="UPDATED_NEW",
            )
            table.update_item(
                Key={"id": archive_id},
                UpdateExpression=f"set table_details[{index}].{validation_type}.archived = :newJob",
                ExpressionAttributeValues={
                    ":newJob": {
                        "query_execution_id": query_execution_id,
                        "query": query,
                        "state": status_message,
                        "results": get_athena_result_map(response=athena_response),
                    }
                },
            )


def get_archive(query_execution_id):
    parameter = ssm.get_parameter(
        Name="/archive/query-lookup-dynamodb-table", WithDecryption=True
    )

    table = dynamodb.Table(parameter["Parameter"]["Value"])
    dynamodb_response = table.get_item(Key={"id": query_execution_id})
    if "Item" not in dynamodb_response:
        return None, None, None, None, None
    archive_id = dynamodb_response["Item"]["archive_id"]
    table_name = dynamodb_response["Item"]["table_name"]
    validation_type = dynamodb_response["Item"]["validation_type"]
    query = dynamodb_response["Item"]["query"]
    row_key = dynamodb_response["Item"]["row_key"]

    return archive_id, table_name, validation_type, query, row_key


def lambda_handler(event, context):
    if event["detail"]["currentState"] in ["QUEUED", "RUNNING"]:
        return
    archive_id, table_name, validation_type, query, row_key = get_archive(
        event["detail"]["queryExecutionId"]
    )
    if event["detail"]["currentState"] == "SUCCEEDED":
        athena_response = get_athena_response(event["detail"]["queryExecutionId"])
        update_validation_state(
            archive_id,
            event["detail"]["queryExecutionId"],
            table_name,
            validation_type,
            athena_response,
            query,
            row_key,
            "SUCCEEDED",
        )
    elif event["detail"]["currentState"] == "FAILED":
        parameter = ssm.get_parameter(
            Name="/archive/dynamodb-table", WithDecryption=True
        )
        table = dynamodb.Table(parameter["Parameter"]["Value"])
        table.update_item(
            Key={"id": archive_id},
            UpdateExpression="SET archive_status= :s",
            ExpressionAttributeValues={":s": "Failed"},
            ReturnValues="UPDATED_NEW",
        )
    return event
