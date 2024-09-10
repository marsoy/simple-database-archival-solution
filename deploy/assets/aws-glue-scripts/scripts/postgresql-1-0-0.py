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
from pyspark.sql.functions import year, month, dayofmonth, to_utc_timestamp


def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME", "TABLE", "BUCKET", "DATABASE", "ARCHIVE_ID", "MAPPINGS", "CONNECTION"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node SQL Server table
SQLServertable_node1 = directJDBCSource(
    glueContext,
    connectionName=args["CONNECTION"],
    connectionType="postgresql",
    database=args["DATABASE"],
    table=str(str(args["TABLE"])),
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
partition_column = "created_at"
df_with_partition = ApplyMapping_node2.toDF()
partition_column_exists = partition_column.upper() in (name.upper() for name in df_with_partition.columns)
if partition_column_exists:
    df_with_partition = df_with_partition.withColumn(partition_column, to_utc_timestamp(df_with_partition[partition_column], "UTC"))
    df_with_partition = df_with_partition.withColumn(
        "year", year(df_with_partition[partition_column])
    ).withColumn(
        "month", month(df_with_partition[partition_column])
    ).withColumn(
        "day", dayofmonth(df_with_partition[partition_column])
    )
    print(df_with_partition.printSchema())

AddPartition_node3 = DynamicFrame.fromDF(df_with_partition, glueContext, "AddPartition_node3")
partitionKeys = ["year", "month", "day"] if partition_column_exists else []

# Script generated for node S3 bucket
S3bucket_node4 = glueContext.write_dynamic_frame.from_options(
    frame=AddPartition_node3,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://" + args["BUCKET"] + "/" + args["ARCHIVE_ID"] + "/" + args["DATABASE"] + "/" + args["TABLE"] + "/",
        "partitionKeys": partitionKeys,
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node4",
)

job.commit()
