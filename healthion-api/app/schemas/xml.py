from decimal import Decimal
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class RecordCreate(BaseModel):
    user_id: UUID
    type: str
    sourceVersion: str
    sourceName: str
    deviceId: str
    startDate: datetime
    endDate: datetime
    creationDate: datetime
    unit: str
    value: Decimal | None

class XMLWorkoutCreate(BaseModel):
    user_id: UUID
    type: str
    duration: Decimal | None
    durationUnit: str
    sourceName: str
    startDate: datetime
    endDate: datetime
    creationDate: datetime

class WorkoutStatisticCreate(BaseModel):
    user_id: UUID
    type: str
    startDate: datetime
    endDate: datetime
    creationDate: datetime
    sum: Decimal | None
    average: Decimal | None
    maximum: Decimal | None
    minimum: Decimal | None
    unit: str


class RecordUpdate(BaseModel):
    """Update schema for Record model. All fields are optional."""
    type: str | None = Field(None, max_length=50)
    sourceVersion: str | None = Field(None, max_length=100)
    sourceName: str | None = Field(None, max_length=100)
    deviceId: str | None = Field(None, max_length=100)
    startDate: datetime | None = None
    endDate: datetime | None = None
    unit: str | None = Field(None, max_length=10)
    value: Decimal | None = None


class XMLWorkoutUpdate(BaseModel):
    """Update schema for XMLWorkout model. All fields are optional."""
    type: str | None = Field(None, max_length=50)
    duration: Decimal | None = None
    durationUnit: str | None = Field(None, max_length=10)
    sourceName: str | None = Field(None, max_length=100)
    startDate: datetime | None = None
    endDate: datetime | None = None


class WorkoutStatisticUpdate(BaseModel):
    """Update schema for WorkoutStatistic model. All fields are optional."""
    type: str | None = Field(None, max_length=50)
    startDate: datetime | None = None
    endDate: datetime | None = None
    sum: Decimal | None = None
    average: Decimal | None = None
    maximum: Decimal | None = None
    minimum: Decimal | None = None
    unit: str | None = Field(None, max_length=10)