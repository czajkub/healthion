from datetime import datetime, UTC
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request

from app.services.aws_service import s3_client, AWS_BUCKET_NAME
from app.schemas.aws import PresignedURLRequest, PresignedURLResponse
from seler.app.celery_factory import celery_app
from seler.app.tasks import poll_sqs_task, poll_sqs_messages


QUEUE_URL: str = "https://sqs.eu-north-1.amazonaws.com/733796381340/xml_upload"

def generate_file_key(
    user_id: str, filename: Optional[str] = None
) -> str:
    """Generate a unique file key for S3 storage"""
    timestamp = datetime.now(UTC)

    if filename:
        # Clean filename and preserve extension
        clean_filename = "".join(c for c in filename if c.isalnum() or c in ".-_")
        file_key = f"{user_id}/raw/{clean_filename}"
    else:
        file_key = f"{user_id}/raw/{timestamp}.xml"

    return file_key


def validate_bucket_exists() -> bool:
    """Check if the S3 bucket exists and is accessible"""
    try:
        s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            raise HTTPException(status_code=500, detail="S3 bucket not found")
        elif error_code == "403":
            raise HTTPException(status_code=500, detail="Access denied to S3 bucket")
        else:
            raise HTTPException(
                status_code=500, detail=f"S3 bucket error: {error_code}"
            )

app = FastAPI()
sqs = boto3.client("sqs")


@app.post("/import-data/apple/presigned_url", response_model=PresignedURLResponse)
async def create_presigned_upload_url(request: PresignedURLRequest):
    """
    Generate a presigned URL for direct file upload to S3

    This endpoint creates a secure, time-limited URL that allows clients to upload
    files directly to S3 without going through this server.
    """

    # Validate bucket accessibility
    validate_bucket_exists()

    # Generate unique file key
    file_key = generate_file_key(
        user_id=request.user_id,
        filename=request.filename,
    )

    try:
        # Create presigned URL with conditions
        conditions = [
            ["content-length-range", 1, request.max_file_size],
            {"Content-Type": request.file_type.value},
        ]

        presigned_post = s3_client.generate_presigned_post(
            Bucket=AWS_BUCKET_NAME,
            Key=file_key,
            Fields={"Content-Type": request.file_type.value},
            Conditions=conditions,
            ExpiresIn=request.expiration_seconds,
        )

        poll_sqs_task.delay(request.expiration_seconds)

        return PresignedURLResponse(
            upload_url=presigned_post["url"],
            form_fields=presigned_post["fields"],
            file_key=file_key,
            expires_in=request.expiration_seconds,
            max_file_size=request.max_file_size,
            content_type=request.file_type.value,
            bucket=AWS_BUCKET_NAME,
        )

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        raise HTTPException(
            status_code=500, detail=f"Failed to generate presigned URL: {error_code}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.get("/import-data/apple/poll_sqs")
async def poll_sqs():
    return await poll_sqs_messages()


