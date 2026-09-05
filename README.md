# Build an AI-Powered Serverless Image Labeling Pipeline

This extends your S3 + Rekognition project into a fully event-driven, decoupled, production-shaped pipeline: upload an image and labels appear automatically, queryable through a REST API — no manual script to run.

---

## 1. Project Overview

**What changes from your original project:** instead of running a Python script by hand, the whole thing reacts to uploads automatically, survives failures gracefully, and exposes results over HTTP.

**Architecture:**

```
   Image uploaded          S3 Event           SQS Queue           Processor Lambda
   to S3 bucket    ---->   Notification ---->  (decouples   ---->  - DetectLabels
                                                 S3 from            - Write to DynamoDB
                                                 processing)              |
                                                     |                    v
                                              (after 3 failed      DynamoDB Table
                                               attempts)                 ^
                                                     v                    |
                                              Dead Letter Queue     API Lambda  <---- GET /images
                                              (for inspection)      (reads table)    GET /images/{key}
                                                                          ^
                                                                          |
                                                                    API Gateway
                                                                    (HTTP API)
```

**Why this is a stronger portfolio piece than the original:**
- **Decoupling** — S3 doesn't call Rekognition directly. It drops a message on a queue, and the processor pulls from that queue. If Rekognition is slow or the processor is down, uploads don't get lost — they just wait.
- **Resilience** — SQS retries failed messages automatically (up to 3 times here), then routes them to a Dead Letter Queue instead of silently dropping them. This is the difference between a script and a system.
- **Partial batch failure handling** — if 1 image in a batch of 5 fails, only that 1 gets retried, not all 5. This is a real AWS Lambda feature (`ReportBatchItemFailures`) that a lot of tutorials skip.
- **Infrastructure as Code** — the entire stack (bucket, queue, table, two functions, API) is defined in one CloudFormation/SAM template, not clicked together in the console.
- **A real API** — results are queryable over HTTP, not just printed to a terminal.

**AWS services used:** S3, SQS, Lambda (x2), Rekognition, DynamoDB, API Gateway (HTTP API), IAM.

**Cost:** all of these have generous free tiers and this project runs at near-zero request volume, so expect this to cost cents at most during development — see the clean-up section to make sure nothing keeps running after you're done.

---

## 2. Prerequisites

- Your original S3 + Rekognition project working (you already have this)
- AWS CLI installed and configured (from your first project — `aws configure`)
- Python 3.12 locally, for testing
- **AWS SAM CLI** — this is new. It's the tool that turns `template.yaml` into deployed AWS resources.

Install SAM CLI:
```bash
# macOS
brew install aws-sam-cli

# Windows: download the MSI from
# https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html

# Linux (via pip)
pip install aws-sam-cli --break-system-packages
```

Verify:
```bash
sam --version
```

---

## 3. Set Up: What the Template Provisions

Rather than clicking through the console for 6 different services, everything lives in one `template.yaml` (AWS SAM, which is CloudFormation with shorthand for serverless resources). Here's what it creates and why each piece exists:

| Resource | Purpose |
|---|---|
| `ImagesTable` (DynamoDB) | Stores `image_key`, labels, and confidence scores. Pay-per-request billing — no capacity to plan for at this scale. |
| `ProcessingQueue` (SQS) | Receives S3 upload notifications. Decouples upload from processing. |
| `ProcessingDLQ` (SQS) | Catches messages that fail 3 times, so you can inspect *why* instead of losing them. |
| `ProcessingQueuePolicy` | Grants S3 permission to send messages to the queue — required before S3 will accept the notification config. |
| `ImagesBucket` (S3) | Where images are uploaded; configured to notify the queue on every new object. |
| `ProcessorFunction` (Lambda) | Consumes the queue, calls Rekognition, writes to DynamoDB. |
| `ApiFunction` (Lambda) | Serves `GET /images` and `GET /images/{key}` by reading DynamoDB. |
| HTTP API (implicit) | SAM auto-creates this from the `Events` on `ApiFunction`. |

One CloudFormation detail worth knowing for interviews: the bucket has `DependsOn: ProcessingQueuePolicy`. Without it, CloudFormation may try to configure the bucket's notification before the queue's policy exists, and S3 will reject the config because it can't verify it has permission to send messages yet.

---

## 4. IAM Permissions

Both functions get **least-privilege** access via SAM's built-in policy templates rather than broad managed policies:

- `ProcessorFunction`: `DynamoDBWritePolicy` (write only, scoped to this table), `S3ReadPolicy` (read only, scoped to this bucket), plus a custom statement for `rekognition:DetectLabels`.
- `ApiFunction`: `DynamoDBReadPolicy` (read only, scoped to this table).

Rekognition doesn't support resource-level ARNs for `DetectLabels`, so that one statement has to use `Resource: "*"` — everything else is scoped to the specific bucket/table SAM creates. Being able to explain *why* one permission is broader than the others is exactly the kind of thing that comes up in interviews.

---

## 5. Import Libraries

Both functions only need `boto3` — everything else is standard library:

```python
# processor_lambda.py
import json
import os
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
import boto3
```

```python
# api_lambda.py
import json
import os
from decimal import Decimal
import boto3
```

`Decimal` matters here: DynamoDB has no native float type, so Rekognition's confidence scores (which are floats) have to be converted to `Decimal` before being written, and back to `float` when read out for JSON.

---

## 6. Define Functions

**Processor — unwrap the SQS message and detect labels:**

```python
def detect_labels(bucket, key):
    response = rekognition.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=MAX_LABELS,
        MinConfidence=MIN_CONFIDENCE,
    )
    return response["Labels"]


def process_s3_record(s3_record):
    bucket = s3_record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_record["s3"]["object"]["key"])
    labels = detect_labels(bucket, key)
    item = to_item(bucket, key, labels)
    table.put_item(Item=item)
```

Note the `unquote_plus` — S3 event keys are URL-encoded (a space becomes `+`), which is a classic gotcha that silently breaks lookups if you skip it.

**Processor — partial batch failure, so one bad image doesn't retry the whole batch:**

```python
def handler(event, context):
    batch_item_failures = []
    for record in event.get("Records", []):
        try:
            process_sqs_message(record)
        except Exception as exc:
            print(f"Failed to process message {record.get('messageId')}: {exc}")
            batch_item_failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": batch_item_failures}
```

**API — list and get, with a 404 for missing images:**

```python
def handler(event, context):
    path_params = event.get("pathParameters") or {}
    image_key = path_params.get("image_key")

    if image_key:
        item = get_image(image_key)
        if not item:
            return _response(404, {"error": f"No labels found for '{image_key}'"})
        return _response(200, item)

    items = list_images()
    return _response(200, {"count": len(items), "images": items})
```

---

## 7. Your Final Code

Three files make up the deployable project (included alongside this guide):

- **`processor/processor_lambda.py`** — the queue consumer
- **`api/api_lambda.py`** — the API backend
- **`template.yaml`** — the full infrastructure definition
- **`test_handlers.py`** — local tests for both handlers using mocked AWS calls (no credentials needed to run these)

Full listing of `template.yaml`:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: >
  Event-driven image label pipeline: S3 upload -> SQS -> Lambda -> Rekognition
  -> DynamoDB -> API Gateway (HTTP API). Failed messages retry up to 3 times
  before landing in a Dead Letter Queue.

Parameters:
  BucketName:
    Type: String
    Description: Globally unique name for the image upload bucket (must not already exist).

Globals:
  Function:
    Runtime: python3.12
    Timeout: 30
    MemorySize: 256

Resources:

  ImagesTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "${AWS::StackName}-image-labels"
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: image_key
          AttributeType: S
      KeySchema:
        - AttributeName: image_key
          KeyType: HASH

  ProcessingDLQ:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-processing-dlq"
      MessageRetentionPeriod: 1209600

  ProcessingQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-processing-queue"
      VisibilityTimeout: 60
      RedrivePolicy:
        deadLetterTargetArn: !GetAtt ProcessingDLQ.Arn
        maxReceiveCount: 3

  ProcessingQueuePolicy:
    Type: AWS::SQS::QueuePolicy
    Properties:
      Queues:
        - !Ref ProcessingQueue
      PolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: s3.amazonaws.com
            Action: sqs:SendMessage
            Resource: !GetAtt ProcessingQueue.Arn
            Condition:
              ArnLike:
                aws:SourceArn: !Sub "arn:aws:s3:::${BucketName}"

  ImagesBucket:
    Type: AWS::S3::Bucket
    DependsOn: ProcessingQueuePolicy
    Properties:
      BucketName: !Ref BucketName
      NotificationConfiguration:
        QueueConfigurations:
          - Event: s3:ObjectCreated:*
            Queue: !GetAtt ProcessingQueue.Arn

  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: processor/
      Handler: processor_lambda.handler
      Environment:
        Variables:
          TABLE_NAME: !Ref ImagesTable
          MAX_LABELS: "10"
          MIN_CONFIDENCE: "75"
      Policies:
        - DynamoDBWritePolicy:
            TableName: !Ref ImagesTable
        - S3ReadPolicy:
            BucketName: !Ref BucketName
        - Statement:
            - Effect: Allow
              Action: rekognition:DetectLabels
              Resource: "*"
      Events:
        SQSTrigger:
          Type: SQS
          Properties:
            Queue: !GetAtt ProcessingQueue.Arn
            BatchSize: 5
            FunctionResponseTypes:
              - ReportBatchItemFailures

  ApiFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: api/
      Handler: api_lambda.handler
      Environment:
        Variables:
          TABLE_NAME: !Ref ImagesTable
      Policies:
        - DynamoDBReadPolicy:
            TableName: !Ref ImagesTable
      Events:
        ListImages:
          Type: HttpApi
          Properties:
            Path: /images
            Method: GET
        GetImage:
          Type: HttpApi
          Properties:
            Path: /images/{image_key}
            Method: GET

Outputs:
  ApiUrl:
    Value: !Sub "https://${ServerlessHttpApi}.execute-api.${AWS::Region}.amazonaws.com"
  BucketNameOutput:
    Value: !Ref ImagesBucket
  TableNameOutput:
    Value: !Ref ImagesTable
  DLQUrlOutput:
    Value: !Ref ProcessingDLQ
```

*(This template passed `cfn-lint` with zero errors, and both Lambda handlers passed local tests covering the success path, a simulated failure with correct partial-batch-failure reporting, URL-decoding of keys, and API list/get/404 behavior — see `test_handlers.py`.)*

---

## 8. Running Your Project

**Deploy:**

```bash
cd label-pipeline
sam build
sam deploy --guided
```

`--guided` will walk you through: a stack name, your region, the `BucketName` parameter (must be globally unique), and confirmation to create IAM roles (`CAPABILITY_IAM`). It saves your answers to `samconfig.toml`, so future deploys are just `sam deploy`.

**Test it end to end:**

```bash
# Upload an image — this triggers the whole pipeline automatically
aws s3 cp ./photo1.jpg s3://your-bucket-name/

# Give it a few seconds, then check the API (get the URL from the deploy output)
curl https://<api-id>.execute-api.<region>.amazonaws.com/images

curl https://<api-id>.execute-api.<region>.amazonaws.com/images/photo1.jpg
```

**Run the local tests (no AWS needed):**

```bash
pip install boto3 --break-system-packages
python3 test_handlers.py
```

**If an image doesn't show up in the API:**
1. Check CloudWatch Logs for `ProcessorFunction` — the print statements will show exactly which step failed.
2. Check the DLQ (`ProcessingDLQ` in the SQS console) — if a message is sitting there, it failed 3 times and is waiting for you to inspect it.

---

## 9. Conclusion & Clean-up

You've gone from "a script that calls an API" to a system that reacts to events, survives partial failures, and serves results over HTTP — entirely defined as code. In an interview, this project gives you real things to talk about: why you chose SQS over invoking Lambda directly from S3, why the DLQ matters, and how partial batch failures avoid retrying an entire batch for one bad image.

**Where to take it further:**
- Add a small static frontend (S3 + CloudFront) that calls the API and displays images with their labels.
- Fan out with SNS so the same upload also triggers `DetectModerationLabels` for content moderation, without touching the processor function.
- Add a Global Secondary Index on a label name so you can query "show me all images labeled 'Dog'" instead of just listing everything.

**Clean up to avoid ongoing charges:**

```bash
# Empty the bucket first - SAM/CloudFormation won't delete a non-empty bucket
aws s3 rm s3://your-bucket-name --recursive

# Tear down every resource the template created
sam delete
```

`sam delete` removes the CloudFormation stack — Lambda functions, the queue, the DLQ, the DynamoDB table, and the API Gateway — in one command, which is one of the practical advantages of having deployed via IaC in the first place.
