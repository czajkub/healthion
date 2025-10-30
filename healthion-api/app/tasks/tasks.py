import asyncio
import json
import random
import string
import os

import boto3
from celery import shared_task
from fastapi import HTTPException

from seler.app.client import s3_client
from seler.app.ducky.duckdb_importer import ParquetImporter


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



@shared_task
def process_uploaded_file(bucket_name: str, object_key: str):
    """
    Process file uploaded to S3
    """


    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body']

    importer = ParquetImporter()

    try:
        length = 8
        db_file = ''.join(random.choices(string.ascii_letters + string.digits, k=length))

        importer.xml_path = file_content
        importer.path = db_file
        importer.export_xml()
    except:
        os.remove(db_file)


    uid = object_key.split('/')[0]
    filename = object_key.split('/')[-1]
    full_name = uid + '/processed/' + filename.replace('.xml', '.duckdb')

    s3_client.upload_file(db_file, bucket_name, full_name)

    os.remove(db_file)

    result = {
        "bucket": bucket_name,
        "key": object_key,
        "new_filename": full_name,
        "status": "processed"
    }

    return result

@shared_task()
async def poll_sqs_task(expiration_seconds: int):
    for _ in range(expiration_seconds // 5):
        await poll_sqs_messages()
        await asyncio.sleep(5)