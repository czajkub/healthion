import os
import subprocess
import tempfile
from celery import shared_task
from sqlalchemy.orm import Session

from app.services.aws_service import s3_client
from app.services.xml_service import XMLExporter
from app.database import SessionLocal
from app.config import settings


@shared_task
def process_uploaded_xml_file(bucket_name: str, object_key: str, user_id: str = None):
    """
    Process XML file uploaded to S3 and import to Postgres database.

    Args:
        bucket_name: S3 bucket name
        object_key: S3 object key (path)
        user_id: User ID to associate with imported data (optional, extracted from object_key if not provided)
    """
    temp_xml_file = None
    dump_file = None

    try:
        # Extract user_id from object_key if not provided
        if not user_id:
            user_id = object_key.split('/')[0]

        # Create temporary directory for files
        temp_dir = tempfile.gettempdir()
        temp_xml_file = os.path.join(temp_dir, f"temp_import_{object_key.split('/')[-1]}")

        # Download XML file from S3
        s3_client.download_file(bucket_name, object_key, temp_xml_file)

        # Import data to Postgres
        db: Session = SessionLocal()
        try:
            response = xml_import_service.import_xml(
                db_session=db,
                user_id=user_id,
                xml_path=temp_xml_file,
            )
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()

        # Create Postgres dump file
        filename = object_key.split('/')[-1]
        dump_file = os.path.join(temp_dir, f"export_{filename.replace('.xml', '.sql')}")
        _create_postgres_dump(dump_file)

        # Upload dump file to S3
        output_key = f"{user_id}/processed/{filename.replace('.xml', '.sql')}"
        s3_client.upload_file(dump_file, bucket_name, output_key)

        result = {
            "bucket": bucket_name,
            "input_key": object_key,
            "output_key": output_key,
            "user_id": user_id,
            "status": "success",
            "message": response.response if hasattr(response, 'response') else str(response),
        }

        return result

    except Exception as e:
        result = {
            "bucket": bucket_name,
            "input_key": object_key,
            "user_id": user_id,
            "status": "failed",
            "error": str(e),
        }
        return result

    finally:
        # Clean up temporary files
        if temp_xml_file and os.path.exists(temp_xml_file):
            os.remove(temp_xml_file)
        if dump_file and os.path.exists(dump_file):
            os.remove(dump_file)


def _create_postgres_dump(dump_file: str) -> None:
    """
    Create a PostgreSQL dump file of the entire database.

    Args:
        dump_file: Path where to save the SQL dump file
    """
    try:
        subprocess.run(
            ["pg_dump", settings.db_uri, f"--file={dump_file}"],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        raise Exception(f"pg_dump failed: {e.stderr.decode()}")