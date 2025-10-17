import asyncio
import json

import boto3
from celery import shared_task
from fastapi import HTTPException


QUEUE_URL: str = "https://sqs.eu-north-1.amazonaws.com/733796381340/xml_upload"

sqs = boto3.client("sqs")

async def poll_sqs_messages():
    """
    Poll SQS for messages (alternative to webhook)
    """
    try:
        # Receive messages from SQS
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # Long polling
            MessageAttributeNames=['All']
        )

        messages = response.get('Messages', [])
        processed_count = 0

        for message in messages:
            try:
                # Parse message body
                message_body = json.loads(message['Body'])

                # Handle S3 notification
                if 'Records' in message_body:
                    for record in message_body['Records']:
                        if record.get('eventSource') == 'aws:s3':
                            bucket_name = record['s3']['bucket']['name']
                            object_key = record['s3']['object']['key']

                            # Enqueue Celery task
                            task = process_uploaded_file.delay(bucket_name, object_key)
                            print(task)
                            processed_count += 1

                # Delete message from queue after processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

            except Exception as e:
                raise

        return {
            "messages_processed": processed_count,
            "total_messages": len(messages),
            "messages": messages
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@shared_task()
async def poll_sqs_task(expiration_seconds: int):
    for _ in range(expiration_seconds // 5):
        await poll_sqs_messages()
        await asyncio.sleep(5)