import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Job arguments
# ---------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "INPUT_PATH",
        "OUTPUT_PATH",
    ],
)

job_name = args["JOB_NAME"]
input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"].rstrip("/")


# ---------------------------------------------------------
# Start Glue / Spark
# ---------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)


# ---------------------------------------------------------
# Read input CSV
# ---------------------------------------------------------
print(f"Reading input file: {input_path}")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print(f"Input columns: {df.columns}")


# ---------------------------------------------------------
# Validate required columns
# ---------------------------------------------------------
required_columns = {"city", "state", "country"}

column_map = {
    column.strip().lower(): column
    for column in df.columns
}

missing_columns = required_columns - set(column_map.keys())

if missing_columns:
    raise ValueError(
        f"Missing required columns: {sorted(missing_columns)}"
    )


# ---------------------------------------------------------
# Get actual column names
# ---------------------------------------------------------
city_column = column_map["city"]
state_column = column_map["state"]
country_column = column_map["country"]


# ---------------------------------------------------------
# Clean required columns
# ---------------------------------------------------------
df = (
    df
    .withColumn(
        city_column,
        F.trim(F.col(city_column).cast("string"))
    )
    .withColumn(
        state_column,
        F.trim(F.col(state_column).cast("string"))
    )
    .withColumn(
        country_column,
        F.trim(F.col(country_column).cast("string"))
    )
)


# ---------------------------------------------------------
# Output paths
# ---------------------------------------------------------
city_output = f"{output_path}/city"
state_output = f"{output_path}/state"
country_output = f"{output_path}/country"


# ---------------------------------------------------------
# Write CITY output
# ---------------------------------------------------------
print(f"Writing city output: {city_output}")

(
    df.select(city_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(city_output)
)


# ---------------------------------------------------------
# Write STATE output
# ---------------------------------------------------------
print(f"Writing state output: {state_output}")

(
    df.select(state_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(state_output)
)


# ---------------------------------------------------------
# Write COUNTRY output
# ---------------------------------------------------------
print(f"Writing country output: {country_output}")

(
    df.select(country_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(country_output)
)


# ---------------------------------------------------------
# Finish Glue job
# ---------------------------------------------------------
print("Glue ETL job completed successfully.")

job.commit()
