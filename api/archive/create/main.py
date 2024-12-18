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
import logging
import os
import traceback
import datetime
import uuid
import pytz

# region Logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger()

if logger.hasHandlers():
    logger.setLevel(LOG_LEVEL)
else:
    logging.basicConfig(level=LOG_LEVEL)

# endregion

REGION = os.getenv("REGION")

ssm = boto3.client("ssm")
secret_client = boto3.client("secretsmanager", region_name=REGION)
dynamodb_client = boto3.resource("dynamodb")


def mask_sensitive_data(event):
    # remove sensitive data from request object before logging
    keys_to_redact = ["authorization"]
    result = {}
    for k, v in event.items():
        if isinstance(v, dict):
            result[k] = mask_sensitive_data(v)
        elif k in keys_to_redact:
            result[k] = "<redacted>"
        else:
            result[k] = v
    return result


def build_response(http_code, body):
    return {
        "headers": {
            # tell cloudfront and api gateway not to cache the response
            "Cache-Control": "no-cache, no-store",
            "Content-Type": "application/json",
        },
        "statusCode": http_code,
        "body": body,
    }


def lambda_handler(event, context):
    logger.info(mask_sensitive_data(event))

    try:

        parameter = ssm.get_parameter(
            Name="/archive/dynamodb-table", WithDecryption=True
        )
        body = json.loads(event["body"]) if "body" in event else json.loads(event)
        archive_name = body["archive_name"]

        hostname = body["hostname"]
        mode = body["mode"]
        port = body["port"]
        username = body["username"]
        database = body["database"]
        database_engine = body["database_engine"]
        archival_start_date = body["archival_start_date"]
        archival_end_date = body["archival_end_date"]
        table_details = []

        try:
            secret_value = secret_client.get_secret_value(
                SecretId=f"{database}-{username}"
            )
            secret_arn = secret_value["ARN"]
        except Exception as e:
            print(
                f"Fetching secret for database: {database} user: {username} failed with exception: {str(e)}"
            )
            return build_response(500, "Server Error")
        
        for table in body["tables"]:
            table["count_validation"] = {"row_key": None}
            table["string_validation"] = {"row_key": None}
            table["number_validation"] = {"row_key": None}
            table_details.append(table)

        archive_id = str(uuid.uuid4())
        table = dynamodb_client.Table(parameter["Parameter"]["Value"])
        dt = datetime.datetime.now(pytz.UTC)

        table.put_item(
            Item={
                "id": archive_id,
                "database_engine": database_engine,
                "archive_name": archive_name,
                "mode": mode,
                "hostname": hostname,
                "port": port,
                "username": username,
                "secret_arn": secret_arn,
                "database": database,
                "oracle_owner": body["oracle_owner"] if "oracle_owner" in body else "",
                "archival_start_date": archival_start_date,
                "archival_end_date": archival_end_date,
                "table_details": table_details,
                "time_submitted": str(dt),
                "archive_status": "Archive Queue",
                "job_status": "",
                "jobs": {},
                "configuration": {
                    "glue": {"glue_worker": "Standard", "glue_capacity": 2}
                },
                "counters": {
                    "validation": {
                        "validation_count": 0,
                        "validation_completed": 0,
                    }
                },
                "legal_hold": False,
                "expiration_status": False,
                "expiration_date": "",
                "delete_data": False,
            }
        )

        response = {"text": "Example response from authenticated api"}
        return build_response(200, json.dumps(response))
    except Exception as ex:
        logger.error(traceback.format_exc())
        return build_response(500, "Server Error")


if __name__ == "__main__":

    example_event = {}
    response = lambda_handler(example_event, {})
    print(json.dumps(response))
