from celery import Celery
from celery import current_app as current_celery_app


CELERY_BROKER_URL="redis://redis:6379/0"
CELERY_RESULT_BACKEND="redis://redis:6379/0"

def create_celery() -> Celery:
    celery_app: Celery = current_celery_app  # type: ignore[assignment]
    celery_app.conf.update(
        broker_url=CELERY_BROKER_URL,
        result_backend=CELERY_RESULT_BACKEND,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="Europe/Warsaw",
        enable_utc=True,
        task_default_queue="default",
        task_default_exchange="default",
        result_expires=3 * 24 * 3600,
        include=['app.tasks']
    )

    celery_app.autodiscover_tasks(["app.tasks.process_uploaded_file", "app.tasks.poll_sqs_task"])

    return celery_app


celery_app = create_celery()

