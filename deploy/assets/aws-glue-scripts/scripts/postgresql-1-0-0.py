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

import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import year, month, dayofmonth, to_utc_timestamp, to_date


def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    partition_column,
    hash_partitions,
    archive_options,
    table_primary_key_mappings,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:

    archival_start_date = archive_options["archival_start_date"]
    archival_end_date = archive_options["archival_end_date"]
    primary_key = table_primary_key_mappings[table]

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if archival_start_date and archival_end_date:
        sql_query = f"SELECT * FROM {table} WHERE {partition_column} BETWEEN '{archival_start_date}' AND '{archival_end_date}' AND"
        print(f"SQL sampleQuery: {sql_query}")
        connection_options["sampleQuery"] = sql_query
        connection_options["enablePartitioningForSampleQuery"] = True
        connection_options["hashpartitions"] = hash_partitions
        if primary_key:
            connection_options["hashfield"] = primary_key
        else:
            connection_options["hashexpression"] = f"MOD(UNIX_TIMESTAMP({partition_column}), {hash_partitions})"

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


def get_hash_partition(glue_options):
    glue_capacity = int(glue_options["glue_capacity"])
    glue_worker = glue_options["glue_worker"]
    # G.1X 4 vCPU, G.2X 8 vCPU. In glue workers, one will be driver and others will be executors.
    vcpu_count = 8 if glue_worker == "G.2X" else 4
    return vcpu_count if glue_capacity == 1 else vcpu_count * (glue_capacity - 1)


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "TABLE",
        "BUCKET",
        "DATABASE",
        "ARCHIVE_ID",
        "MAPPINGS",
        "TABLE_PRIMARY_KEY_MAPPINGS",
        "CONNECTION",
        "ARCHIVE_OPTIONS",
        "GLUE_OPTIONS",
    ],
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

partition_column = "created_at"
archive_options = json.loads(args["ARCHIVE_OPTIONS"])

table_primary_key_mappings = json.loads(args["TABLE_PRIMARY_KEY_MAPPINGS"])

glue_options = json.loads(args["GLUE_OPTIONS"])
hash_partitions = get_hash_partition(glue_options=glue_options)

# Script generated for node SQL Server table
SQLServertable_node1 = directJDBCSource(
    glueContext,
    connectionName=args["CONNECTION"],
    connectionType="postgresql",
    database=args["DATABASE"],
    table=str(str(args["TABLE"])),
    partition_column=partition_column,
    hash_partitions=hash_partitions,
    archive_options=archive_options,
    table_primary_key_mappings=table_primary_key_mappings,
    redshiftTmpDir="",
    transformation_ctx="SQLServertable_node1",
)

# Script generated for node ApplyMapping
tuples = list(map(tuple, json.loads(args["MAPPINGS"])))

ApplyMapping_node2 = ApplyMapping.apply(
    frame=SQLServertable_node1,
    mappings=tuples,
    transformation_ctx="ApplyMapping_node2",
)

# Script to partition the data
df_with_partition = ApplyMapping_node2.toDF()
partition_column_exists = partition_column.upper() in (
    name.upper() for name in df_with_partition.columns
)
if partition_column_exists:
    df_with_partition = df_with_partition.withColumn(
        partition_column, to_utc_timestamp(df_with_partition[partition_column], "UTC")
    )
    df_with_partition = df_with_partition.withColumn(
        "year", year(df_with_partition[partition_column])
    ).withColumn("month", month(df_with_partition[partition_column]))
    df_with_partition = df_with_partition.repartition(2, "year", "month")
    print(df_with_partition.printSchema())

AddPartition_node3 = DynamicFrame.fromDF(
    df_with_partition, glueContext, "AddPartition_node3"
)
partitionKeys = ["year", "month"] if partition_column_exists else []
path = "s3://" + args["BUCKET"] + "/" + args["DATABASE"] + "/" + args["TABLE"] + "/"

# Script generated for node S3 bucket
S3bucket_node4 = glueContext.write_dynamic_frame.from_options(
    frame=AddPartition_node3,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": path,
        "partitionKeys": partitionKeys,
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node4",
)

if partition_column_exists:
    try:
        WriteSink_node5 = glueContext.getSink(
            path=path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=partitionKeys,
            enableUpdateCatalog=True,
            transformation_ctx="WriteSink_node5",
        )
        WriteSink_node5.setCatalogInfo(
            catalogDatabase=f"{args['DATABASE']}-database",
            catalogTableName=f"{args['DATABASE']}-{args['TABLE']}-table",
        )
        WriteSink_node5.setFormat("glueparquet", compression="snappy")
        WriteSink_node5.writeFrame(AddPartition_node3)
    except Exception as e:
        print(f"Exception occurred while updating datalog, error: {str(e)}")

job.commit()
