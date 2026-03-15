"""
AWS Glue job that transforms raw Oklahoma General Ledger CSV files into a cleaned
silver-layer Parquet dataset partitioned by fiscal year.

Reads all CSVs from RAW_PATH using a fixed schema, casts FISCAL_YEAR and
ACCOUNTING_PERIOD to integers (stripping spurious .0 suffixes), extracts the
source quarter from the file name, and drops columns that are historically null.
The cleaned data is written to SILVER_PATH as Parquet, overwriting existing
partitions dynamically. A newline-delimited list of the dropped columns is also
written to METADATA_PATH (must be an s3:// URI) for reference by downstream
consumers.

Runtime parameters (passed via Glue job details):
    JOB_NAME      -- Glue job name (required by the Glue SDK).
    RAW_PATH      -- S3 path (prefix or glob) for raw CSV input files.
    SILVER_PATH   -- S3 destination path for the output Parquet dataset.
    METADATA_PATH -- S3 URI where the dropped-columns manifest is written.
"""

# ruff: noqa
# type: ignore
import sys

import boto3
from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext  # type: ignore
from pyspark.sql.functions import (
    col,
    expr,
    input_file_name,
    regexp_extract,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Job parameters - passed in at runtime via Glue job details
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "SILVER_PATH", "METADATA_PATH"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sparkContext.setLogLevel("ERROR")

raw_data_path = args["RAW_PATH"]
silver_path = args["SILVER_PATH"]
metadata_path = args["METADATA_PATH"]

schema = StructType(
    [
        StructField("AGENCYNBR", StringType(), True),
        StructField("AGENCYNAME", StringType(), True),
        StructField("LEDGER", StringType(), True),
        StructField("FISCAL_YEAR", StringType(), True),
        StructField("ACCOUNTING_PERIOD", StringType(), True),
        StructField("FUND_CODE", StringType(), True),
        StructField("FUNDDESCR", StringType(), True),
        StructField("CLASS_FLD", StringType(), True),
        StructField("CLASSDESCR", StringType(), True),
        StructField("DEPTID", StringType(), True),
        StructField("DEPTDESCR", StringType(), True),
        StructField("ACCOUNT", StringType(), True),
        StructField("ACCTDESCR", StringType(), True),
        StructField("OPERATING_UNIT", StringType(), True),
        StructField("OPERUNITDESCR", StringType(), True),
        StructField("PRODUCT", StringType(), True),
        StructField("PRODUCTDESCR", StringType(), True),
        StructField("PROGRAM_CODE", StringType(), True),
        StructField("PGMDESCR", StringType(), True),
        StructField("BUDGET_REF", StringType(), True),
        StructField("CHARTFIELD1", StringType(), True),
        StructField("CF1DESCR", StringType(), True),
        StructField("CHARTFIELD2", StringType(), True),
        StructField("CF2DESCR", StringType(), True),
        StructField("PROJECT_ID", StringType(), True),
        StructField("PROJDESCR", StringType(), True),
        StructField("POSTED_TOTAL_AMT", DoubleType(), True),
        StructField("ACTIVITY", StringType(), True),
        StructField("ACTVDESCR", StringType(), True),
        StructField("RESTYPE", StringType(), True),
        StructField("RESDESCR", StringType(), True),
        StructField("RCAT", StringType(), True),
        StructField("RCATDESCR", StringType(), True),
        StructField("RSUBCAT", StringType(), True),
        StructField("RSUBCATDESCR", StringType(), True),
        StructField("ROWID", StringType(), True),
    ]
)

df = (
    spark.read.option("header", True)
    # Keep malformed rows with PERMISSIVE
    .option("mode", "PERMISSIVE")
    .option("multiLine", False)
    .option("escape", '"')
    .option("quote", '"')
    .schema(schema)
    .csv(raw_data_path)
    .withColumn("source_file", input_file_name())
)

# confirmed nulls historically (notebook 02)
# revisit if needed later
null_columns = [
    "PGMDESCR",
    "OPERUNITDESCR",
    "RSUBCATDESCR",
    "RSUBCAT",
    "RCATDESCR",
    "RCAT",
    "RESDESCR",
    "RESTYPE",
    "ACTVDESCR",
    "CF1DESCR",
    "CF2DESCR",
    "PROJDESCR",
    "PRODUCTDESCR",
]

df = (
    df.withColumn("FISCAL_YEAR", expr("try_cast(regexp_replace(FISCAL_YEAR, '\\\\.0+$', '') as int)"))
    .withColumn("ACCOUNTING_PERIOD", expr("try_cast(regexp_replace(ACCOUNTING_PERIOD, '\\\\.0+$', '') as int)"))
    .withColumn("EXTRACTED_QUARTER", regexp_extract(col("source_file"), r"QTR(\d)", 1))
    .drop(*null_columns)
)

(df.write.mode("overwrite").partitionBy("FISCAL_YEAR").parquet(silver_path))

# Keep a record of dropped columns for downstream consumers
if not metadata_path.startswith("s3://"):
    raise ValueError(f"METADATA_PATH must be an s3:// path, got: {metadata_path}")

bucket_key = metadata_path.replace("s3://", "", 1)
bucket = bucket_key.split("/", 1)[0]
key = bucket_key.split("/", 1)[1]

s3 = boto3.client("s3")
s3.put_object(Bucket=bucket, Key=key, Body=("\n".join(null_columns) + "\n").encode("utf-8"))

job.commit()
