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
from pyspark.sql.functions import year, month


def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    partition_column,
    related_table_details,
    hash_partitions,
    archive_options,
    primary_key,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:

    archival_start_date = archive_options["archival_start_date"]
    archival_end_date = archive_options["archival_end_date"]

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if archival_start_date and archival_end_date:
        sub_query = f" BETWEEN '{archival_start_date}' AND '{archival_end_date}' AND"
        if related_table_details:
            table_column = related_table_details.get("table_column")
            fk_table_column = related_table_details.get("fk_table_column")
            fk_table = related_table_details.get("fk_table")
            primary_key = None
            partition_column = related_table_details.get("fk_partition_column")
            sql_query = f"SELECT ot.*, fkr.{partition_column} FROM {table} ot INNER JOIN {fk_table} fkr ON ot.{table_column} = fkr.{fk_table_column} WHERE {partition_column}"
        else:
            sql_query = f"SELECT * FROM {table} WHERE {partition_column}"
        sql_query += sub_query
        print(f"SQL sampleQuery: {sql_query}")
        connection_options["sampleQuery"] = sql_query
        connection_options["enablePartitioningForSampleQuery"] = True
        connection_options["hashpartitions"] = hash_partitions
        if primary_key:
            connection_options["hashfield"] = primary_key
        else:
            connection_options["hashfield"] = partition_column
            connection_options[
                "hashexpression"
            ] = f"MOD(UNIX_TIMESTAMP({partition_column}), {hash_partitions})"

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


def get_hash_partition(configuration_options):
    glue_capacity = int(configuration_options["glue_capacity"])
    glue_worker = configuration_options["glue_worker"]
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
        "CONNECTION",
        "ARCHIVE_OPTIONS",
        "CONFIGURATION_OPTIONS",
    ],
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DEFAULT_PARTITION_KEYS = ["year", "month"]
DEFAULT_PARTITION_COLUMN = "created_at"
archive_options = json.loads(args["ARCHIVE_OPTIONS"])

configuration_options = json.loads(args["CONFIGURATION_OPTIONS"])
hash_partitions = get_hash_partition(configuration_options=configuration_options)

compression = configuration_options.get("compression", "snappy")
c_partition_keys = configuration_options.get("partition_keys")
if c_partition_keys:
    partition_keys = c_partition_keys.split(",")
else:
    partition_keys = DEFAULT_PARTITION_KEYS
primary_key = configuration_options.get("primary_key")
partition_column = configuration_options.get("partition_column")
related_table_details = configuration_options.get("related_table_details")
if not partition_column:
    if related_table_details:
        partition_column = related_table_details.get("fk_partition_column")
    else:
        partition_column = DEFAULT_PARTITION_COLUMN

print(f"partition_keys: {partition_keys} partition_column: {partition_column}")


# Script generated for node MySQL table
MySQLtable_node1 = directJDBCSource(
    glueContext,
    connectionName=args["CONNECTION"],
    connectionType="mysql",
    database=args["DATABASE"],
    table=args["TABLE"],
    partition_column=partition_column,
    related_table_details=related_table_details,
    hash_partitions=hash_partitions,
    archive_options=archive_options,
    primary_key=primary_key,
    redshiftTmpDir="",
    transformation_ctx="MySQLtable_node1",
)

# Script generated for node ApplyMapping
tuples = list(map(tuple, json.loads(args["MAPPINGS"])))

ApplyMapping_node2 = ApplyMapping.apply(
    frame=MySQLtable_node1,
    mappings=tuples,
    transformation_ctx="ApplyMapping_node2",
)

# Script to partition the data
df_with_partition = ApplyMapping_node2.toDF()
print(f"df partition columns: {df_with_partition.columns}")
partition_column_exists = partition_column.upper() in (
    name.upper() for name in df_with_partition.columns
)
print(f"partition_column_exists: {partition_column_exists}")
if partition_column_exists:
    df_with_partition = df_with_partition.withColumn(
        "year", year(ApplyMapping_node2[partition_column])
    ).withColumn("month", month(ApplyMapping_node2[partition_column]))
    df_with_partition = df_with_partition.repartition(1, "year", "month")

if related_table_details:
    print(f"DF count: {df_with_partition.count()}")

AddPartition_node3 = DynamicFrame.fromDF(
    df_with_partition, glueContext, "AddPartition_node3"
)

# Script generated for node S3 bucket
partition_keys = partition_keys if partition_column_exists else []
path = "s3://" + args["BUCKET"] + "/" + args["DATABASE"] + "/" + args["TABLE"] + "/"

try:
    WriteSink_node4 = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        enableUpdateCatalog=True,
        transformation_ctx="WriteSink_node4",
    )
    WriteSink_node4.setCatalogInfo(
        catalogDatabase=f"{args['DATABASE']}-database",
        catalogTableName=f"{args['DATABASE']}-{args['TABLE']}-table",
    )
    WriteSink_node4.setFormat("glueparquet", compression=compression)
    WriteSink_node4.writeFrame(AddPartition_node3)
except Exception as e:
    print(f"Exception occurred while updating datalog, error: {str(e)}")

job.commit()
