import csv
import io
import json
import logging
import os
from typing import List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
REQUIRED_COLUMNS = {
    column.strip().lower()
    for column in os.environ.get(
        "REQUIRED_COLUMNS", "city,state,country"
    ).split(",")
    if column.strip()
}


def _read_header(bucket: str, key: str) -> List[str]:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"]

    # Read enough data to obtain the CSV header. The first line is expected
    # to contain the column names.
    first_line = body.readline().decode("utf-8-sig").strip()
    if not first_line:
        raise ValueError("CSV file is empty or has no header")

    reader = csv.reader(io.StringIO(first_line))
    return [column.strip().lower() for column in next(reader)]


def _start_glue_job(bucket: str, key: str) -> str:
    input_path = f"s3://{bucket}/{key}"
    output_path = f"s3://{bucket}/{OUTPUT_PREFIX}"

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--INPUT_PATH": input_path,
            "--OUTPUT_PATH": output_path,
        },
    )

    return response["JobRunId"]


def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    results = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"].replace("+", " ")

        logger.info("Processing s3://%s/%s", bucket, key)

        try:
            columns = set(_read_header(bucket, key))
            missing = sorted(REQUIRED_COLUMNS - columns)

            if missing:
                message = f"Missing required columns: {missing}"
                logger.error(message)
                results.append(
                    {"key": key, "status": "rejected", "reason": message}
                )
                continue

            job_run_id = _start_glue_job(bucket, key)

            logger.info(
                "Schema valid. Started Glue job %s for %s",
                job_run_id,
                key,
            )

            results.append(
                {
                    "key": key,
                    "status": "accepted",
                    "glue_job_run_id": job_run_id,
                }
            )

        except (ClientError, ValueError, UnicodeDecodeError) as exc:
            logger.exception("Failed to process %s", key)
            results.append(
                {
                    "key": key,
                    "status": "error",
                    "reason": str(exc),
                }
            )

    return {
        "statusCode": 200,
        "body": json.dumps(results),
    }
