import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "OUTPUT_PATH",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"]

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

required_columns = {"city", "state", "country"}
actual_columns = {column.strip().lower() for column in df.columns}
missing = required_columns - actual_columns

if missing:
    raise ValueError(f"Missing required columns: {sorted(missing)}")

# Normalize the partition columns without changing the other data columns.
for column in ["city", "state", "country"]:
    matching_column = next(
        c for c in df.columns if c.strip().lower() == column
    )
    df = df.withColumn(
        matching_column,
        F.trim(F.col(matching_column).cast("string")),
    )

# The output is physically partitioned as:
# output/city=<city>/state=<state>/country=<country>/part-*.parquet
(
    df.write
    .mode("append")
    .format("parquet")
    .partitionBy("city", "state", "country")
    .save(output_path)
)

job.commit()
