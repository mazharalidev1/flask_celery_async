from unittest.mock import patch

from celery import Celery

from flask_async_celery.django import DjangoAsyncCelery


def test_django_extension_initializes():
    celery = DjangoAsyncCelery(
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=5,
    )

    assert celery.core.max_tasks == 5
    assert celery.celery.conf.worker_concurrency == 5


def test_django_extension_accepts_existing_celery():
    app = Celery("test_django")

    extension = DjangoAsyncCelery(
        celery=app,
        max_tasks=3,
    )

    assert extension.celery is app
    assert extension.celery.conf.worker_concurrency == 3


def test_django_task_uses_async_task():
    extension = DjangoAsyncCelery(
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
    )

    @extension.task
    async def example():
        return "ok"

    from flask_async_celery.task import AsyncTask

    assert isinstance(example, AsyncTask)