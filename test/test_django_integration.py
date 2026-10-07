from __future__ import annotations

import time

import django
import pytest
from celery.contrib.testing.worker import start_worker
from flask_async_celery.django import DjangoAsyncCelery


@pytest.fixture(scope="module", autouse=True)
def django_setup():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv(
        "DJANGO_SETTINGS_MODULE",
        "test.django_settings",
    )

    try:
        django.setup()
        yield
    finally:
        monkeypatch.undo()


@pytest.fixture
def django_celery():
    return DjangoAsyncCelery(
        broker_url="redis://127.0.0.1:6379/0",
        result_backend="redis://127.0.0.1:6379/1",
        max_tasks=2,
    )


def test_django_initializes(django_celery):
    assert django_celery.celery is not None
    assert django_celery.core.max_tasks == 2


def test_django_task_uses_async_task(django_celery):
    from flask_async_celery.task import AsyncTask

    @django_celery.task
    async def django_task():
        return "success"

    assert isinstance(django_task, AsyncTask)


def test_django_async_task_with_real_worker(django_celery):
    @django_celery.task
    async def add_numbers(a, b):
        return a + b

    with start_worker(
        django_celery.celery,
        pool="flask_async_celery.pool:AsyncIOPool",
        concurrency=2,
        perform_ping_check=False,
        loglevel="WARNING",
    ):
        result = add_numbers.delay(10, 32)

        deadline = time.monotonic() + 10

        while not result.ready() and time.monotonic() < deadline:
            time.sleep(0.1)

        assert result.ready()
        assert result.get(timeout=1) == 42


def test_django_async_task_can_use_settings(django_celery):
    from django.conf import settings

    @django_celery.task
    async def get_secret_key():
        return settings.SECRET_KEY

    with start_worker(
        django_celery.celery,
        pool="flask_async_celery.pool:AsyncIOPool",
        concurrency=2,
        perform_ping_check=False,
        loglevel="WARNING",
    ):
        result = get_secret_key.delay()

        deadline = time.monotonic() + 10

        while not result.ready() and time.monotonic() < deadline:
            time.sleep(0.1)

        assert result.ready()
        assert result.get(timeout=1) == "test-secret-key"


def test_django_async_task_can_use_orm(django_celery):
    from django.db import connection
    from test.django_app.models import TestItem

    with connection.schema_editor() as schema_editor:
        schema_editor.create_model(TestItem)

    try:
        TestItem.objects.create(name="before-worker")

        @django_celery.task
        async def count_items():
            return await TestItem.objects.acount()

        with start_worker(
            django_celery.celery,
            pool="flask_async_celery.pool:AsyncIOPool",
            concurrency=2,
            perform_ping_check=False,
            loglevel="WARNING",
        ):
            result = count_items.delay()

            deadline = time.monotonic() + 10

            while not result.ready() and time.monotonic() < deadline:
                time.sleep(0.1)

            assert result.ready()
            assert result.get(timeout=1) == 1

    finally:
        with connection.schema_editor() as schema_editor:
            schema_editor.delete_model(TestItem)