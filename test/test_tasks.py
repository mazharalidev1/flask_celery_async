from flask_async_celery.task import (
    AsyncTask,
    get_async_executor,
    reset_async_executor,
    set_async_executor,
    update_progress,
)
import time
from flask_async_celery.monitoring import TaskMonitor
from unittest.mock import PropertyMock, patch

class TestAsyncTask(AsyncTask):
    name = "test.async_failure"

    async def run(self):
        await asyncio.sleep(0.1)
        raise ValueError("async task failed")


class ProgressAsyncTask(AsyncTask):
    name = "test.async_progress"

    async def run(self):
        await self.update_progress(
            current=25,
            total=100,
            message="Quarter complete",
        )

        await asyncio.sleep(0.01)

        await self.update_progress(
            current=100,
            total=100,
            message="Complete",
        )

        return "success"


def test_async_task_exception():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    token = set_async_executor(executor)

    try:
        task = TestAsyncTask()

        with pytest.raises(ValueError, match="async task failed"):
            task()

    finally:
        reset_async_executor(token)
        executor.shutdown()


import asyncio

import pytest

from flask_async_celery.executor import AsyncExecutor
from flask_async_celery.task import (
    AsyncTask,
    reset_async_executor,
    set_async_executor,
)


class RetryAsyncTask(AsyncTask):
    name = "test.async_retry"

    def __init__(self):
        self.attempts = 0

    async def run(self):
        self.attempts += 1
        await asyncio.sleep(0.1)

        if self.attempts == 1:
            raise self.retry(
                exc=ValueError("temporary failure"),
                countdown=0,
            )

        return "success"


def test_async_task_retry():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    token = set_async_executor(executor)

    try:
        task = RetryAsyncTask()

        with pytest.raises(Exception):
            task()

    finally:
        reset_async_executor(token)
        executor.shutdown()


def test_async_task_progress():
    executor = AsyncExecutor(max_tasks=3)
    executor.start()

    token = set_async_executor(executor)

    try:
        task = ProgressAsyncTask()

        request = type(
            "Request",
            (),
            {"id": "progress-task"},
        )()

        with patch.object(
            AsyncTask,
            "request",
            new_callable=PropertyMock,
            return_value=request,
        ):
            async def run_progress():
                await task.update_progress(
                    current=25,
                    total=100,
                    message="Quarter complete",
                )

                await asyncio.sleep(0.01)

                await task.update_progress(
                    current=100,
                    total=100,
                    message="Complete",
                )

            asyncio.run(run_progress())

        result = executor.monitor.get_task("progress-task")

        assert result is not None
        assert result["progress"]["current"] == 100
        assert result["progress"]["total"] == 100
        assert result["progress"]["percentage"] == 100
        assert result["progress"]["message"] == "Complete"

    finally:
        reset_async_executor(token)
        executor.shutdown()


def test_async_task_progress_requires_executor():
    task = ProgressAsyncTask()

    async def run():
        await task.update_progress(
            current=50,
            total=100,
        )

    with pytest.raises(
            RuntimeError,
            match="requires an AsyncExecutor",
    ):
        asyncio.run(run())


def test_update_progress():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    token = set_async_executor(executor)

    try:
        async def work():
            await update_progress(
                current=25,
                total=100,
                message="Quarter complete",
            )

            await asyncio.sleep(0.01)

            await update_progress(
                current=100,
                total=100,
                message="Complete",
            )

            return "done"

        future = executor.submit(
            work(),
            task_id="progress-helper-task",
            task_name="test.progress_helper",
        )

        assert future.result(timeout=2) == "done"

        history = executor.monitor.history()

        assert len(history) == 1

        task = history[0]

        assert task["task_id"] == "progress-helper-task"
        assert task["task_name"] == "test.progress_helper"
        assert task["state"] == "SUCCESS"

        assert task["progress"]["current"] == 100
        assert task["progress"]["total"] == 100
        assert task["progress"]["percentage"] == 100
        assert task["progress"]["message"] == "Complete"

    finally:
        reset_async_executor(token)
        executor.shutdown()


def test_update_progress_requires_executor():
    async def work():
        await update_progress(
            current=50,
            total=100,
        )

    with pytest.raises(
        RuntimeError,
        match="update_progress\\(\\) requires an AsyncExecutor",
    ):
        asyncio.run(work())

    def test_update_progress_requires_active_task():
        executor = AsyncExecutor(max_tasks=1)
        executor.start()

        token = set_async_executor(executor)

        try:
            async def work():
                await update_progress(
                    current=50,
                    total=100,
                )

            with pytest.raises(
                    RuntimeError,
                    match="must be called from an active async task",
            ):
                asyncio.run(work())

        finally:
            reset_async_executor(token)
            executor.shutdown()


def test_received_records_worker():
    monitor = TaskMonitor()

    monitor.record_received(
        "worker-task",
        task_name="test_task",
        worker="celery@freeman",
    )

    task = monitor.get_task("worker-task")

    assert task is not None
    assert task["worker"] == "celery@freeman"

def test_executor_records_worker():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def work():
        await asyncio.sleep(0.05)

    future = executor.submit(
        work(),
        task_id="worker-task",
        task_name="test_work",
        worker="celery@freeman",
    )

    try:
        for _ in range(100):
            task = executor.monitor.get_task("worker-task")
            if task is not None and task["state"] == "RUNNING":
                break
            time.sleep(0.01)

        assert task is not None
        assert task["worker"] == "celery@freeman"

        future.result(timeout=2)
    finally:
        executor.shutdown()