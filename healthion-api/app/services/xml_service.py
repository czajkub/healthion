from decimal import Decimal
from logging import Logger, getLogger
from uuid import UUID
from pathlib import Path

import pandas as pd

from app.database import DbSession
from app.services.xml_helpers import (
    record_service,
    xml_workout_service,
    workout_statistic_service,
)
from app.schemas import (
    RecordCreate,
    XMLWorkoutCreate,
    WorkoutStatisticCreate,
    UploadDataResponse,
)
from app.utils.exceptions import handle_exceptions
from app.utils.xml_exporter import XMLExporter


class XMLImportService:
    """Service to import health data from XML files into postgres."""

    def __init__(self, log: Logger, xml_path: Path, **kwargs):
        self.log = log
        self.xml_exporter = XMLExporter(xml_path)
        self.record_service = record_service
        self.xml_workout_service = xml_workout_service
        self.workout_statistic_service = workout_statistic_service
        super().__init__(**kwargs)

    def _process_records_df(
            self,
            db_session: DbSession,
            df: pd.DataFrame,
            user_id: str,
    ) -> int:
        """Process a DataFrame of Record type data."""
        if df.empty or "type" not in df.columns:
            return 0

        created_count = 0
        for _, row in df.iterrows():
            try:
                record_data = {
                    "user_id": UUID(user_id),
                    "type": str(row.get("type", ""))[:50],
                    "sourceVersion": str(row.get("sourceVersion", ""))[:100],
                    "sourceName": str(row.get("sourceName", ""))[:100],
                    "deviceId": str(row.get("device", ""))[:100],
                    "startDate": pd.to_datetime(row.get("startDate")),
                    "endDate": pd.to_datetime(row.get("endDate")),
                    "creationDate": pd.to_datetime(row.get("creationDate")),
                    "unit": str(row.get("unit", ""))[:10],
                    "value": self._safe_decimal(row.get("value")),
                }

                record_create = RecordCreate(**record_data)
                self.record_service.create(db_session, record_create)
                created_count += 1

            except Exception as e:
                self.log.error(f"Failed to process record row: {str(e)}, row: {row.to_dict()}")
                raise

        return created_count

    def _process_workouts_df(
            self,
            db_session: DbSession,
            df: pd.DataFrame,
            user_id: str,
    ) -> int:
        """Process a DataFrame of XMLWorkout type data."""
        if df.empty:
            return 0

        created_count = 0
        for _, row in df.iterrows():
            try:
                workout_data = {
                    "user_id": UUID(user_id),
                    "type": str(row.get("type", "Unknown Workout"))[:50],
                    "duration": self._safe_decimal(row.get("duration")),
                    "durationUnit": str(row.get("durationUnit", ""))[:10],
                    "sourceName": str(row.get("sourceName", ""))[:100],
                    "startDate": pd.to_datetime(row.get("startDate")),
                    "endDate": pd.to_datetime(row.get("endDate")),
                    "creationDate": pd.to_datetime(row.get("creationDate")),
                }

                workout_create = XMLWorkoutCreate(**workout_data)
                self.xml_workout_service.create(db_session, workout_create)
                created_count += 1
                self.log.debug(f"Created XMLWorkout: {workout_data['type']}")

            except Exception as e:
                self.log.error(f"Failed to process workout row: {str(e)}, row: {row.to_dict()}")
                raise

        return created_count

    def _process_statistics_df(
            self,
            db_session: DbSession,
            df: pd.DataFrame,
            user_id: str,
    ) -> int:
        """Process a DataFrame of WorkoutStatistic type data."""
        if df.empty:
            return 0

        created_count = 0
        for _, row in df.iterrows():
            try:
                stat_data = {
                    "user_id": UUID(user_id),
                    "type": str(row.get("type", ""))[:50],
                    "startDate": pd.to_datetime(row.get("startDate")),
                    "endDate": pd.to_datetime(row.get("endDate")),
                    "creationDate": pd.to_datetime(row.get("creationDate")),
                    "sum": self._safe_decimal(row.get("sum")),
                    "average": self._safe_decimal(row.get("average")),
                    "maximum": self._safe_decimal(row.get("maximum")),
                    "minimum": self._safe_decimal(row.get("minimum")),
                    "unit": str(row.get("unit", ""))[:10],
                }

                stat_create = WorkoutStatisticCreate(**stat_data)
                self.workout_statistic_service.create(db_session, stat_create)
                created_count += 1

            except Exception as e:
                self.log.error(f"Failed to process statistic row: {str(e)}, row: {row.to_dict()}")
                raise

        return created_count

    @staticmethod
    def _safe_decimal(value) -> Decimal | None:
        """Safely convert value to Decimal."""
        if value is None:
            return None
        if isinstance(value, float) and value == 0.0:
            return Decimal("0")
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None

    def _detect_dataframe_type(self, df: pd.DataFrame) -> str:
        """Determine DataFrame type by inspecting columns."""
        if df.empty:
            return "unknown"

        columns = set(df.columns)

        # Check for stats (has sum, average, etc.)
        if {"sum", "average", "maximum", "minimum"}.issubset(columns):
            return "statistic"

        # Check for workout (has duration)
        if "duration" in columns and "durationUnit" in columns:
            return "workout"

        # Check for record (has value)
        if "value" in columns:
            return "record"

        return "unknown"

    @handle_exceptions
    async def import_xml(
            self,
            db_session: DbSession,
            user_id: str,
            xml_path: str = None,
    ) -> UploadDataResponse:
        """Import data from XML file."""
        try:
            if xml_path:
                self.xml_exporter.xml_path = Path(xml_path)

            stats = {
                "records": 0,
                "workouts": 0,
                "statistics": 0,
                "chunks_processed": 0,
            }

            # Process DataFrames yielded by the XML parser
            for df in self.xml_exporter.parse_xml():
                if df.empty:
                    continue

                df_type = self._detect_dataframe_type(df)

                if df_type == "record":
                    stats["records"] += self._process_records_df(db_session, df, user_id)
                elif df_type == "workout":
                    stats["workouts"] += self._process_workouts_df(db_session, df, user_id)
                elif df_type == "statistic":
                    stats["statistics"] += self._process_statistics_df(db_session, df, user_id)

                stats["chunks_processed"] += 1
                self.log.debug(f"Processed chunk {stats['chunks_processed']} (type: {df_type})")

            message = (
                f"XML import successful. "
                f"Records: {stats['records']}, "
                f"Workouts: {stats['workouts']}, "
                f"Statistics: {stats['statistics']}"
            )
            self.log.info(message)
            return UploadDataResponse(response=message)

        except Exception as e:
            self.log.error(f"XML import failed: {str(e)}")
            return UploadDataResponse(response=f"XML import failed: {str(e)}")

