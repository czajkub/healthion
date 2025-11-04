import os
import subprocess
import tempfile
from pathlib import Path
from celery import shared_task
from sqlalchemy.orm import Session
import pandas as pd

from app.services.apple.apple_xml.aws_service import s3_client
from app.services.xml_service import XMLExporter
from app.services.xml_service import (
    record_service,
    xml_workout_service,
    workout_statistic_service
)
from app.database import SessionLocal
from app.config import settings
from app.schemas import RecordCreate, XMLWorkoutCreate, WorkoutStatisticCreate
from decimal import Decimal
from uuid import UUID


@shared_task
def process_uploaded_file(bucket_name: str, object_key: str, user_id: str = None):
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

        # Parse and import data to Postgres
        db: Session = SessionLocal()
        try:
            _import_xml_data(db, temp_xml_file, user_id)
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
            "message": "Import completed successfully",
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


def _import_xml_data(db: Session, xml_path: str, user_id: str) -> None:
    """
    Parse XML file and import data to database using XMLExporter.

    Args:
        db: Database session
        xml_path: Path to the XML file
        user_id: User ID to associate with the data
    """
    exporter = XMLExporter(Path(xml_path))
    user_uuid = UUID(user_id)

    for df in exporter.parse_xml():
        if df.empty:
            continue

        # Detect DataFrame type and process accordingly
        if "value" in df.columns:
            _process_records_df(db, df, user_uuid)
        elif "duration" in df.columns:
            _process_workouts_df(db, df, user_uuid)
        elif "sum" in df.columns:
            _process_statistics_df(db, df, user_uuid)


def _process_records_df(db: Session, df: pd.DataFrame, user_id: UUID) -> None:
    """Process Records DataFrame and insert to database."""
    for _, row in df.iterrows():
        try:
            record_data = {
                "user_id": user_id,
                "type": str(row.get("type", ""))[:50],
                "sourceVersion": str(row.get("sourceVersion", ""))[:100],
                "sourceName": str(row.get("sourceName", ""))[:100],
                "deviceId": str(row.get("device", ""))[:100],
                "startDate": row.get("startDate"),
                "endDate": row.get("endDate"),
                "creationDate": row.get("creationDate"),
                "unit": str(row.get("unit", ""))[:10],
                "value": _safe_decimal(row.get("value")),
            }
            record_create = RecordCreate(**record_data)
            record_service.create(db, record_create)
        except Exception as e:
            raise Exception(f"Failed to process record: {str(e)}")


def _process_workouts_df(db: Session, df: pd.DataFrame, user_id: UUID) -> None:
    """Process Workouts DataFrame and insert to database."""
    for _, row in df.iterrows():
        try:
            workout_data = {
                "user_id": user_id,
                "type": str(row.get("type", ""))[:50],
                "duration": _safe_decimal(row.get("duration")),
                "durationUnit": str(row.get("durationUnit", ""))[:10],
                "sourceName": str(row.get("sourceName", ""))[:100],
                "startDate": row.get("startDate"),
                "endDate": row.get("endDate"),
                "creationDate": row.get("creationDate"),
            }
            workout_create = XMLWorkoutCreate(**workout_data)
            xml_workout_service.create(db, workout_create)
        except Exception as e:
            raise Exception(f"Failed to process workout: {str(e)}")


def _process_statistics_df(db: Session, df: pd.DataFrame, user_id: UUID) -> None:
    """Process WorkoutStatistics DataFrame and insert to database."""
    for _, row in df.iterrows():
        try:
            stat_data = {
                "user_id": user_id,
                "type": str(row.get("type", ""))[:50],
                "startDate": row.get("startDate"),
                "endDate": row.get("endDate"),
                "creationDate": row.get("creationDate"),
                "sum": _safe_decimal(row.get("sum")),
                "average": _safe_decimal(row.get("average")),
                "maximum": _safe_decimal(row.get("maximum")),
                "minimum": _safe_decimal(row.get("minimum")),
                "unit": str(row.get("unit", ""))[:10],
            }
            stat_create = WorkoutStatisticCreate(**stat_data)
            workout_statistic_service.create(db, stat_create)
        except Exception as e:
            raise Exception(f"Failed to process statistic: {str(e)}")


@staticmethod
def _safe_decimal(value) -> Decimal | None:
    """Safely convert value to Decimal."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


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
