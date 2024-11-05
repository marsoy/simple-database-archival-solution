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
from botocore.config import Config
import json
import os

REGION = os.environ["REGION"]

client = boto3.client(
    "glue",
    region_name=REGION,
    config=Config(connect_timeout=5, read_timeout=60, retries={"max_attempts": 20}),
)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def lambda_handler(event, context):

    bucketParameter = ssm.get_parameter(
        Name="/job/s3-bucket-table-data", WithDecryption=True
    )
    parameter = ssm.get_parameter(Name="/archive/dynamodb-table", WithDecryption=True)
    temp_dir_parameter = ssm.get_parameter(Name="/glue/temp-dir", WithDecryption=True)

    table = dynamodb.Table(parameter["Parameter"]["Value"])
    temp_dir_parameter_value = temp_dir_parameter["Parameter"]["Value"]
    dynamodb_response = table.get_item(Key={"id": event["archive_id"]})

    try:

        mappings = []

        for schema in event["table_details"]:
            mappings.append(
                [schema["key"], schema["value"], schema["key"], schema["value"]]
            )

        primary_key = None
        partition_column = None
        for table_detail in dynamodb_response["Item"]["table_details"]:
            if table_detail["table"] == event["table"]:
                primary_key = table_detail.get("primary_key", None)
                partition_column = table_detail.get("partition_column", None)
                related_table_details = table_detail.get("related_table_details", None)
                break

        if event["database_engine"] == "mysql":
            configuration = dynamodb_response["Item"]["configuration"]
            response = client.start_job_run(
                JobName=f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                Arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": f"s3://{temp_dir_parameter_value}/temporary/",
                    "--enable-job-insights": "false",
                    "--TABLE": event["table"],
                    "--BUCKET": bucketParameter["Parameter"]["Value"],
                    "--DATABASE": event["database"],
                    "--ARCHIVE_ID": event["archive_id"],
                    "--CONNECTION": f'{event["archive_id"]}-{event["database"]}-connection',
                    "--MAPPINGS": json.dumps(mappings),
                    "--ARCHIVE_OPTIONS": json.dumps(
                        {
                            "archival_start_date": dynamodb_response["Item"][
                                "archival_start_date"
                            ],
                            "archival_end_date": dynamodb_response["Item"][
                                "archival_end_date"
                            ],
                        }
                    ),
                    "--CONFIGURATION_OPTIONS": json.dumps(
                        {
                            "glue_capacity": configuration["glue"]["glue_capacity"],
                            "glue_worker": configuration["glue"]["glue_worker"],
                            "compression": configuration.get("compression", None),
                            "partition_keys": configuration.get("partition_keys", None),
                            "primary_key": primary_key,
                            "partition_column": partition_column,
                            "related_table_details": related_table_details,
                        }
                    ),
                },
                Timeout=2880,
                WorkerType=configuration["glue"]["glue_worker"],
                NumberOfWorkers=int(configuration["glue"]["glue_capacity"]),
            )

            table.update_item(
                Key={"id": event["archive_id"]},
                UpdateExpression=f'set jobs.{response["JobRunId"]} = :newJob',
                ExpressionAttributeValues={
                    ":newJob": {
                        "job_run_id": response["JobRunId"],
                        "state": "RUNNING",
                        "timestamp": response["ResponseMetadata"]["HTTPHeaders"][
                            "date"
                        ],
                        "message": "",
                    }
                },
            )
        elif event["database_engine"] == "postgresql":
            configuration = dynamodb_response["Item"]["configuration"]
            response = client.start_job_run(
                JobName=f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                Arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": f"s3://{temp_dir_parameter_value}/temporary/",
                    "--enable-job-insights": "false",
                    "--TABLE": event["table"],
                    "--BUCKET": bucketParameter["Parameter"]["Value"],
                    "--DATABASE": event["database"],
                    "--ARCHIVE_ID": event["archive_id"],
                    "--CONNECTION": f'{event["archive_id"]}-{event["database"]}-connection',
                    "--MAPPINGS": json.dumps(mappings),
                    "--ARCHIVE_OPTIONS": json.dumps(
                        {
                            "archival_start_date": dynamodb_response["Item"][
                                "archival_start_date"
                            ],
                            "archival_end_date": dynamodb_response["Item"][
                                "archival_end_date"
                            ],
                        }
                    ),
                    "--CONFIGURATION_OPTIONS": json.dumps(
                        {
                            "glue_capacity": configuration["glue"]["glue_capacity"],
                            "glue_worker": configuration["glue"]["glue_worker"],
                            "compression": configuration.get("compression", None),
                            "partition_keys": configuration.get("partition_keys", None),
                            "primary_key": primary_key,
                            "partition_column": partition_column,
                            "related_table_details": related_table_details,
                        }
                    ),
                },
                Timeout=2880,
                WorkerType=configuration["glue"]["glue_worker"],
                NumberOfWorkers=int(configuration["glue"]["glue_capacity"]),
            )

        elif event["database_engine"] == "mssql":
            response = client.start_job_run(
                JobName=f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                Arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": f"s3://{temp_dir_parameter_value}/temporary/",
                    "--enable-job-insights": "false",
                    "--TABLE": event["table"],
                    "--MSSQL_SCHEMA": event["mssql_schema"],
                    "--BUCKET": bucketParameter["Parameter"]["Value"],
                    "--DATABASE": event["database"],
                    "--ARCHIVE_ID": event["archive_id"],
                    "--CONNECTION": f'{event["archive_id"]}-{event["database"]}-connection',
                    "--MAPPINGS": json.dumps(mappings),
                },
                Timeout=2880,
                WorkerType=dynamodb_response["Item"]["configuration"]["glue"][
                    "glue_worker"
                ],
                NumberOfWorkers=int(
                    dynamodb_response["Item"]["configuration"]["glue"]["glue_capacity"]
                ),
            )

            table.update_item(
                Key={"id": event["archive_id"]},
                UpdateExpression=f'set jobs.{response["JobRunId"]} = :newJob',
                ExpressionAttributeValues={
                    ":newJob": {
                        "job_name": f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                        "job_run_id": response["JobRunId"],
                        "state": "RUNNING",
                        "timestamp": response["ResponseMetadata"]["HTTPHeaders"][
                            "date"
                        ],
                    }
                },
            )

        elif event["database_engine"] == "oracle":
            response = client.start_job_run(
                JobName=f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                Arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": f"s3://{temp_dir_parameter_value}/temporary/",
                    "--enable-job-insights": "false",
                    "--OWNER": event["oracle_owner"],
                    "--TABLE": event["table"],
                    "--BUCKET": bucketParameter["Parameter"]["Value"],
                    "--DATABASE": event["database"],
                    "--CONNECTION": f'{event["archive_id"]}-{event["database"]}-connection',
                    "--ARCHIVE_ID": event["archive_id"],
                    "--MAPPINGS": json.dumps(mappings),
                },
                Timeout=2880,
                WorkerType=dynamodb_response["Item"]["configuration"]["glue"][
                    "glue_worker"
                ],
                NumberOfWorkers=int(
                    dynamodb_response["Item"]["configuration"]["glue"]["glue_capacity"]
                ),
            )

            table.update_item(
                Key={"id": event["archive_id"]},
                UpdateExpression=f'set jobs.{response["JobRunId"]} = :newJob',
                ExpressionAttributeValues={
                    ":newJob": {
                        "job_run_id": response["JobRunId"],
                        "state": "RUNNING",
                        "timestamp": response["ResponseMetadata"]["HTTPHeaders"][
                            "date"
                        ],
                        "message": "",
                    }
                },
            )

            table.update_item(
                Key={"id": event["archive_id"]},
                UpdateExpression=f'set jobs.{response["JobRunId"]} = :newJob',
                ExpressionAttributeValues={
                    ":newJob": {
                        "job_name": f'{event["archive_id"]}-{event["database"]}-{event["table"]}',
                        "job_run_id": response["JobRunId"],
                        "state": "RUNNING",
                        "timestamp": response["ResponseMetadata"]["HTTPHeaders"][
                            "date"
                        ],
                    }
                },
            )
    except Exception as ex:
        print(ex)
        print("error")
        raise

    return {"Payload": event}
