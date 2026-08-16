```python
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

# Read the input CSV.
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

# Validate required columns.
required_columns = {"city", "state", "country"}
actual_columns = {column.strip().lower() for column in df.columns}
missing = required_columns - actual_columns

if missing:
    raise ValueError(f"Missing required columns: {sorted(missing)}")

# Find the actual column names while allowing different capitalization.
column_map = {
    column.strip().lower(): column
    for column in df.columns
}

city_column = column_map["city"]
state_column = column_map["state"]
country_column = column_map["country"]

# Clean the three required columns.
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

# Write city output.
(
    df.select(city_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(f"{output_path}/city")
)

# Write state output.
(
    df.select(state_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(f"{output_path}/state")
)

# Write country output.
(
    df.select(country_column)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(f"{output_path}/country")
)

job.commit()
```
