"""
Local logic test for both Lambda handlers, using unittest.mock to stand in
for boto3 so this runs with no AWS credentials or network access.
"""
import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, "processor")
sys.path.insert(0, "api")


def test_processor_success_and_partial_failure():
    os.environ["TABLE_NAME"] = "test-table"

    with patch("boto3.client") as mock_client, patch("boto3.resource") as mock_resource:
        mock_rekognition = MagicMock()
        mock_rekognition.detect_labels.return_value = {
            "Labels": [
                {"Name": "Dog", "Confidence": 98.712, "Parents": [{"Name": "Animal"}]},
                {"Name": "Golden Retriever", "Confidence": 95.4, "Parents": [{"Name": "Dog"}]},
            ]
        }
        mock_client.return_value = mock_rekognition

        mock_table = MagicMock()
        mock_dynamodb_resource = MagicMock()
        mock_dynamodb_resource.Table.return_value = mock_table
        mock_resource.return_value = mock_dynamodb_resource

        if "processor_lambda" in sys.modules:
            del sys.modules["processor_lambda"]
        import processor_lambda

        # One message that succeeds, one that fails (bad bucket triggers an exception)
        def detect_labels_side_effect(Image, MaxLabels, MinConfidence):
            if Image["S3Object"]["Bucket"] == "boom-bucket":
                raise RuntimeError("simulated Rekognition failure")
            return mock_rekognition.detect_labels.return_value

        mock_rekognition.detect_labels.side_effect = detect_labels_side_effect

        event = {
            "Records": [
                {
                    "messageId": "msg-1-ok",
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "my-bucket"},
                                        "object": {"key": "vacation+photo.jpg"},
                                    }
                                }
                            ]
                        }
                    ),
                },
                {
                    "messageId": "msg-2-fail",
                    "body": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "boom-bucket"},
                                        "object": {"key": "broken.jpg"},
                                    }
                                }
                            ]
                        }
                    ),
                },
            ]
        }

        result = processor_lambda.handler(event, None)

        assert result == {"batchItemFailures": [{"itemIdentifier": "msg-2-fail"}]}, result
        assert mock_table.put_item.call_count == 1, mock_table.put_item.call_count

        put_item_call = mock_table.put_item.call_args.kwargs["Item"]
        assert put_item_call["image_key"] == "vacation photo.jpg", put_item_call  # '+' decoded to space
        assert put_item_call["bucket"] == "my-bucket"
        assert put_item_call["label_count"] == 2
        assert put_item_call["labels"][0]["name"] == "Dog"
        assert float(put_item_call["labels"][0]["confidence"]) == 98.71

        print("processor_lambda: OK")
        print("  batchItemFailures ->", result)
        print("  saved item ->", json.dumps({k: str(v) for k, v in put_item_call.items()}, indent=2)[:300])


def test_api_list_and_get():
    os.environ["TABLE_NAME"] = "test-table"

    with patch("boto3.resource") as mock_resource:
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            "Items": [{"image_key": "vacation photo.jpg", "label_count": 2}]
        }
        mock_table.get_item.return_value = {
            "Item": {"image_key": "vacation photo.jpg", "label_count": 2}
        }
        mock_dynamodb_resource = MagicMock()
        mock_dynamodb_resource.Table.return_value = mock_table
        mock_resource.return_value = mock_dynamodb_resource

        if "api_lambda" in sys.modules:
            del sys.modules["api_lambda"]
        import api_lambda

        # GET /images
        list_resp = api_lambda.handler({"pathParameters": None}, None)
        assert list_resp["statusCode"] == 200
        body = json.loads(list_resp["body"])
        assert body["count"] == 1

        # GET /images/{image_key} - found
        get_resp = api_lambda.handler({"pathParameters": {"image_key": "vacation photo.jpg"}}, None)
        assert get_resp["statusCode"] == 200

        # GET /images/{image_key} - not found
        mock_table.get_item.return_value = {}
        notfound_resp = api_lambda.handler({"pathParameters": {"image_key": "missing.jpg"}}, None)
        assert notfound_resp["statusCode"] == 404

        print("api_lambda: OK")
        print("  list ->", list_resp["body"])
        print("  get (found) ->", get_resp["statusCode"])
        print("  get (not found) ->", notfound_resp["statusCode"])


if __name__ == "__main__":
    test_processor_success_and_partial_failure()
    test_api_list_and_get()
    print("\nAll tests passed.")
