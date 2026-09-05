"""
API Lambda
----------
Backs two HTTP API routes:
    GET /images              -> list recently processed images + labels
    GET /images/{image_key}  -> labels for one specific image

Reads DynamoDB directly; no other AWS calls needed.
"""

import json
import os
from decimal import Decimal

import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLE_NAME = os.environ["TABLE_NAME"]

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


class DecimalEncoder(json.JSONEncoder):
    """DynamoDB returns Decimal for numbers; JSON doesn't know that type."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def list_images(limit=50):
    """Scan is fine at portfolio scale; swap for a GSI query if this grows."""
    response = table.scan(Limit=limit)
    return response.get("Items", [])


def get_image(image_key):
    response = table.get_item(Key={"image_key": image_key})
    return response.get("Item")


def handler(event, context):
    """Lambda entry point for an API Gateway HTTP API (payload format 2.0)."""
    path_params = event.get("pathParameters") or {}
    image_key = path_params.get("image_key")

    try:
        if image_key:
            item = get_image(image_key)
            if not item:
                return _response(404, {"error": f"No labels found for '{image_key}'"})
            return _response(200, item)

        items = list_images()
        return _response(200, {"count": len(items), "images": items})

    except Exception as exc:  # noqa: BLE001
        print(f"API error: {exc}")
        return _response(500, {"error": "Internal server error"})
