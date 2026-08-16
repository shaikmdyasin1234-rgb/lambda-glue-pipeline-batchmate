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

job_name = args["JOB_NAME"]
input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"].rstrip("/")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

print(f"Reading input file: {input_path}")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print(f"Input columns: {df.columns}")

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

city_column = column_map["city"]
state_column = column_map["state"]
country_column = column_map["country"]

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

city_output = f"{output_path}/city"
state_output = f"{output_path}/state"
country_output = f"{output_path}/country"

print(f"Writing city output: {city_output}")

(
    df.select(city_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(city_output)
)

print(f"Writing state output: {state_output}")

(
    df.select(state_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(state_output)
)

print(f"Writing country output: {country_output}")

(
    df.select(country_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(country_output)
)

print("Glue ETL job completed successfully.")

job.commit()
