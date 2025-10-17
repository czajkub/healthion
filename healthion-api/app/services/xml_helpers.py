from logging import getLogger
from app.repositories import CrudRepository
from app.services import AppService
from app.models import Record, XMLWorkout, WorkoutStatistic
from app.schemas import RecordCreate, RecordUpdate

record_crud = CrudRepository[Record, RecordCreate, RecordUpdate](Record)
record_service = AppService(
    crud_model=CrudRepository,
    model=Record,
    log=getLogger(__name__),
)

xml_workout_service = AppService(
    crud_model=CrudRepository,
    model=XMLWorkout,
    log=getLogger(__name__),
)

workout_statistic_service = AppService(
    crud_model=CrudRepository,
    model=WorkoutStatistic,
    log=getLogger(__name__),
)