"""
Processor Lambda
-----------------
Triggered by an SQS queue that receives S3 ObjectCreated notifications
directly (S3 -> SQS, no SNS fan-out needed for a single consumer).

For each message:
  1. Parse the wrapped S3 event out of the SQS message body.
  2. Call Rekognition DetectLabels on the S3 object.
  3. Write the results to DynamoDB.

Uses SQS's partial batch failure feature (ReportBatchItemFailures): only
messages that actually fail are returned for retry, instead of the whole
batch being retried on a single bad record. Messages that fail
`maxReceiveCount` times land in the Dead Letter Queue automatically.
"""

import json
import os
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLE_NAME = os.environ["TABLE_NAME"]
MAX_LABELS = int(os.environ.get("MAX_LABELS", "10"))
MIN_CONFIDENCE = float(os.environ.get("MIN_CONFIDENCE", "75"))

rekognition = boto3.client("rekognition", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def detect_labels(bucket, key):
    """Call Rekognition's DetectLabels API on an image stored in S3."""
    response = rekognition.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=MAX_LABELS,
        MinConfidence=MIN_CONFIDENCE,
    )
    return response["Labels"]


def to_item(bucket, key, labels):
    """Shape a Rekognition response into a DynamoDB item."""
    return {
        "image_key": key,
        "bucket": bucket,
        "labels": [
            {
                "name": label["Name"],
                # DynamoDB has no native float type; Decimal is required.
                "confidence": Decimal(str(round(label["Confidence"], 2))),
                "parents": [p["Name"] for p in label.get("Parents", [])],
            }
            for label in labels
        ],
        "label_count": len(labels),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def process_s3_record(s3_record):
    """Detect labels for one S3 object and persist them."""
    bucket = s3_record["s3"]["bucket"]["name"]
    # S3 event keys are URL-encoded (spaces become '+', etc.)
    raw_key = s3_record["s3"]["object"]["key"]
    key = urllib.parse.unquote_plus(raw_key)

    print(f"Processing s3://{bucket}/{key}")
    labels = detect_labels(bucket, key)
    item = to_item(bucket, key, labels)
    table.put_item(Item=item)
    print(f"Saved {len(labels)} label(s) for {key}")


def process_sqs_message(record):
    """Unwrap an SQS message body into the S3 event(s) it contains."""
    body = json.loads(record["body"])
    for s3_record in body.get("Records", []):
        process_s3_record(s3_record)


def handler(event, context):
    """
    Lambda entry point. `event` is an SQS batch:
        {"Records": [{"messageId": ..., "body": ...}, ...]}

    Returns a partial-batch-failure response so only failed messages
    are retried, per:
    https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
    """
    batch_item_failures = []

    for record in event.get("Records", []):
        try:
            process_sqs_message(record)
        except Exception as exc:  # noqa: BLE001 - report and keep processing the batch
            print(f"Failed to process message {record.get('messageId')}: {exc}")
            batch_item_failures.append({"itemIdentifier": record["messageId"]})

    return {"batchItemFailures": batch_item_failures}
