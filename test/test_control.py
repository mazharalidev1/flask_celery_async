import asyncio
import time

from flask_async_celery.control import async_task_info
from flask_async_celery.executor import AsyncExecutor

class FakeConsumer:
    def __init__(self, pool):
        self.pool = pool


class FakeState:
    def __init__(self, pool):
        self.consumer = FakeConsumer(pool)

def test_async_task_info_running_task():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def work():
        executor.monitor.record_progress(
            "running-task",
            current=50,
            total=100,
            message="Halfway",
        )
        await asyncio.sleep(0.5)

    future = executor.submit(
        work(),
        task_id="running-task",
        task_name="test_work",
    )

    try:
        for _ in range(100):
            task = executor.monitor.get_task("running-task")
            if task is not None and task["state"] == "RUNNING":
                break
            time.sleep(0.01)

        state = FakeState(
            type("Pool", (), {"async_executor": executor})()
        )

        result = async_task_info(
            state,
            task_id="running-task",
        )

        assert result["asyncio-enabled"] is True
        assert result["task"]["task_id"] == "running-task"
        assert result["task"]["state"] == "RUNNING"
        assert result["task"]["progress"]["current"] == 50
        assert result["task"]["progress"]["total"] == 100
        assert result["task"]["progress"]["percentage"] == 50
        assert result["task"]["progress"]["message"] == "Halfway"

        future.result(timeout=2)
    finally:
        executor.shutdown()


def test_async_task_info_completed_task():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def work():
        return "done"

    future = executor.submit(
        work(),
        task_id="completed-task",
        task_name="test_work",
    )

    try:
        assert future.result(timeout=2) == "done"

        state = FakeState(
            type("Pool", (), {"async_executor": executor})()
        )

        result = async_task_info(
            state,
            task_id="completed-task",
        )

        assert result["asyncio-enabled"] is True
        assert result["task"] is not None
        assert result["task"]["task_id"] == "completed-task"
        assert result["task"]["state"] == "SUCCESS"
    finally:
        executor.shutdown()


def test_async_task_info_unknown_task():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    try:
        state = FakeState(
            type("Pool", (), {"async_executor": executor})()
        )

        result = async_task_info(
            state,
            task_id="does-not-exist",
        )

        assert result == {
            "asyncio-enabled": True,
            "task": None,
        }
    finally:
        executor.shutdown()


def test_async_task_info_missing_task_id():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    try:
        state = FakeState(
            type("Pool", (), {"async_executor": executor})()
        )

        result = async_task_info(state)

        assert result["asyncio-enabled"] is True
        assert result["task"] is None
        assert result["error"] == "task_id is required"
    finally:
        executor.shutdown()



def test_async_task_info_non_asyncio_pool():
    pool = object()
    state = FakeState(pool)

    result = async_task_info(
        state,
        task_id="some-task",
    )

    assert result == {
        "asyncio-enabled": False,
        "task": None,
    }