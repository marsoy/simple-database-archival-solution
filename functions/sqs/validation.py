"""
Copyright 2024 Amazon.com, Inc. and its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

  https://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
"""

from decimal import Decimal
import boto3
import json
import os
import time

REGION = os.getenv("REGION")
ssm = boto3.client("ssm", region_name=REGION)
sqs_client = boto3.client("sqs", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def lambda_handler(event, context):
    dynamodb_parameter = ssm.get_parameter(
        Name="/archive/dynamodb-table", WithDecryption=True
    )
    table = dynamodb.Table(dynamodb_parameter["Parameter"]["Value"])

    sqs_parameter = ssm.get_parameter(Name="/sqs/validation", WithDecryption=True)
    sqs_parameter_value = sqs_parameter["Parameter"]["Value"]

    for message in event["Records"]:
        message_body = json.loads(message["body"])
        print("recevied message_body:", message_body)

        # Fetch current state from DynamoDB
        archive_id = message_body["archive_id"]
        dynamodb_response = table.get_item(Key={"id": archive_id})
        if "Item" not in dynamodb_response:
            print(f"dynamodb_response: {dynamodb_response}")
            continue

        table_index = message_body["table_index"]
        validation_type = message_body["validation_type"]

        archived = dynamodb_response["Item"]["table_details"][table_index][
            validation_type
        ]["archived"]
        source_database = dynamodb_response["Item"]["table_details"][table_index][
            validation_type
        ]["source_database"]

        if archived["state"] == "RUNNING" or source_database["state"] == "RUNNING":
            print(
                f"Validation is still in progress for archival: {archive_id} table_index: {table_index}"
            )
            time.sleep(60)
            query_execution_id = archived["query_execution_id"]
            response = sqs_client.send_message(
                QueueUrl=str(sqs_parameter_value),
                MessageGroupId=archive_id,
                MessageDeduplicationId=f"{str(query_execution_id)}_1",
                MessageBody=json.dumps(message_body),
            )
            print(f"SQS message response: {response}")
            return event

        if archived["state"] == "FAILED" or source_database["state"] == "FAILED":
            table.update_item(
                Key={"id": archive_id},
                UpdateExpression="SET archive_status= :s",
                ExpressionAttributeValues={":s": "Failed"},
                ReturnValues="UPDATED_NEW",
            )
            return event

        archived_results = archived["results"]
        source_results = source_database["results"]
        # Archived and Source query results exist and values doesn't match then consider as validation Failed
        if (
            archived_results
            and (
                archived_results["value"] is not None
                and source_results["value"] is not None
            )
            and Decimal(archived_results["value"]) != Decimal(source_results["value"])
        ):
            print(f"Updating archival status to failed for message: {message_body}")
            table.update_item(
                Key={"id": archive_id},
                UpdateExpression="SET archive_status= :s",
                ExpressionAttributeValues={":s": "Failed"},
                ReturnValues="UPDATED_NEW",
            )
            return event

        # Atomically increment the validation_completed counter
        update_response = table.update_item(
            Key={"id": message_body["archive_id"]},
            UpdateExpression="ADD counters.validation.validation_completed :inc",
            ExpressionAttributeValues={":inc": 1},
            ReturnValues="UPDATED_NEW",
        )

        # Get the updated validation_completed count
        validation_completed_increment = update_response["Attributes"]["counters"][
            "validation"
        ]["validation_completed"]

        # Check if validation is complete
        validation_count = dynamodb_response["Item"]["counters"]["validation"][
            "validation_count"
        ]
        if validation_completed_increment == validation_count:
            table.update_item(
                Key={"id": archive_id},
                UpdateExpression="SET archive_status= :s",
                ExpressionAttributeValues={":s": "Archived"},
                ReturnValues="UPDATED_NEW",
            )

        print("completed message_body:", message_body)

        # Delete the SQS message after processing
        sqs_client.delete_message(
            QueueUrl=sqs_parameter_value, ReceiptHandle=message["receiptHandle"]
        )

    return event
