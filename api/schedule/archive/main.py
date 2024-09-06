import json
import boto3
import logging
import os
import traceback
import datetime
import uuid
import pytz

from lib import mysql
from lib import postgresql

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logger = logging.getLogger()

if logger.hasHandlers():
    logger.setLevel(LOG_LEVEL)
else:
    logging.basicConfig(level=LOG_LEVEL)

# endregion

REGION = os.getenv("REGION")
ARCHIVE_JOB_FUNCTION = os.getenv("ARCHIVE_JOB_FUNCTION")

ssm = boto3.client("ssm")
secret_client = boto3.client("secretsmanager", region_name=REGION)
dynamodb_client = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")


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
        username = body["username"]
        database = body["database"]
        hostname = body["hostname"]
        port = body["port"]
        database_engine = body["database_engine"]
        archival_start_date = body["archival_start_date"]
        archival_end_date = body["archival_end_date"]
        req_table_details = body["table_details"]
        compression = body["compression"]
        partition_keys = body["partition_keys"]
        glue_worker_details = body["glue_worker_details"]

        table_details = []
        try:
            secret_value = secret_client.get_secret_value(
                SecretId=f"{database}-{username}"
            )
            secret_arn = secret_value["ARN"]
            password = secret_value["SecretString"]
        except Exception as e:
            print(
                f"Fetching secret for database: {database} user: {username} failed with exception: {str(e)}"
            )
            return build_response(500, "Server Error")

        if database_engine == "mysql":
            connection = mysql.Connection(hostname, port, username, password, database)
            tables = connection.get_schema_by_tables(
                table_names=req_table_details.keys()
            )

        elif database_engine == "postgresql":
            schema = "public"
            connection = postgresql.Connection(
                hostname, port, username, password, database, schema
            )
            tables = connection.get_schema_by_tables(
                table_names=req_table_details.keys()
            )

        for tab in tables:
            table_detail = req_table_details[tab["table"]]
            partition_column = table_detail.get("partition_column")
            number_validation_row_key = table_detail.get(
                "number_validation_row_key", None
            )
            string_validation_row_key = table_detail.get(
                "string_validation_row_key", None
            )
            tab["count_validation"] = {"row_key": None}
            tab["string_validation"] = {"row_key": string_validation_row_key}
            tab["number_validation"] = {"row_key": number_validation_row_key}
            tab["partition_column"] = partition_column
            if table_detail.get("use_related"):
                tab["use_related"] = table_detail.get("use_related")
                tab["related_table_details"] = table_detail.get("related_table_details")
            table_details.append(tab)

        archive_id = str(uuid.uuid4())
        table = dynamodb_client.Table(parameter["Parameter"]["Value"])
        dt = datetime.datetime.now(pytz.UTC)

        table.put_item(
            Item={
                "id": archive_id,
                "database_engine": database_engine,
                "archive_name": archive_name,
                "mode": "Read",
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
                    "glue": {
                        "glue_worker": glue_worker_details["glue_worker"],
                        "glue_capacity": glue_worker_details["glue_capacity"],
                    },
                    "compression": compression,
                    "partition_keys": partition_keys,
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

        payload = {
            "archive_id": archive_id,
            "worker_capacity": glue_worker_details["glue_capacity"],
            "worker_type": glue_worker_details["glue_worker"],
            "archive_schedule": {
                "run_now": True,
                "date": "",
                "time": "",
            },
        }

        print(
            f"Invoking lambda funciton: {ARCHIVE_JOB_FUNCTION} with payload: {payload}"
        )
        lambda_client.invoke(
            FunctionName=ARCHIVE_JOB_FUNCTION,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )

    except Exception as e:
        logger.error(traceback.format_exc())
        return build_response(500, "Server Error")


if __name__ == "__main__":

    example_event = {}
    response = lambda_handler(example_event, {})
    print(json.dumps(response))
