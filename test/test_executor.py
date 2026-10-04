import asyncio
import time
from concurrent.futures import CancelledError

import pytest

from flask_async_celery.executor import (
    AsyncExecutor,
)


async def slow_task(number):
    await asyncio.sleep(2)

    return number


def test_tasks_run_concurrently():
    executor = AsyncExecutor(
        max_tasks=5
    )

    executor.start()

    try:

        started = time.monotonic()

        futures = [
            executor.submit(
                slow_task(i)
            )
            for i in range(5)
        ]

        results = [
            future.result(timeout=5)
            for future in futures
        ]

        elapsed = (
                time.monotonic() - started
        )

        assert results == [
            0, 1, 2, 3, 4
        ]

        # Five 2-second tasks should finish
        # in roughly 2 seconds, not 10.
        assert elapsed < 4

    finally:

        executor.shutdown()


@pytest.fixture
def executor():
    executor = AsyncExecutor(max_tasks=2)
    executor.start()

    yield executor

    executor.shutdown()


def test_exception_releases_execution_slot(executor):
    async def failing_task():
        await asyncio.sleep(0.05)
        raise RuntimeError("expected failure")

    future = executor.submit(failing_task())

    with pytest.raises(RuntimeError, match="expected failure"):
        future.result(timeout=5)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_multiple_exceptions_do_not_deadlock(executor):
    async def failing_task(number):
        await asyncio.sleep(0.02)
        raise RuntimeError(f"failure-{number}")

    futures = [
        executor.submit(failing_task(number))
        for number in range(10)
    ]

    for future in futures:
        with pytest.raises(RuntimeError):
            future.result(timeout=5)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_recovers_after_exception(executor):
    async def failing_task():
        raise RuntimeError("boom")

    async def successful_task():
        await asyncio.sleep(0.05)
        return "success"

    failed = executor.submit(failing_task())

    with pytest.raises(RuntimeError, match="boom"):
        failed.result(timeout=5)

    assert executor.running == 0

    successful = executor.submit(successful_task())

    assert successful.result(timeout=5) == "success"

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_concurrency_never_exceeds_max_tasks(executor):
    max_seen = 0
    lock = asyncio.Lock()

    async def tracked_task():
        nonlocal max_seen

        async with lock:
            max_seen = max(
                max_seen,
                executor.running,
            )

        await asyncio.sleep(0.1)

    futures = [
        executor.submit(tracked_task())
        for _ in range(10)
    ]

    for future in futures:
        future.result(timeout=5)

    assert max_seen <= executor.max_tasks
    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_handles_success_and_failure_together(executor):
    async def successful_task():
        await asyncio.sleep(0.03)
        return "ok"

    async def failing_task():
        await asyncio.sleep(0.03)
        raise ValueError("failure")

    futures = []

    for _ in range(5):
        futures.append(
            executor.submit(successful_task())
        )
        futures.append(
            executor.submit(failing_task())
        )

    successes = 0
    failures = 0

    for future in futures:
        try:
            result = future.result(timeout=5)

            assert result == "ok"
            successes += 1

        except ValueError as exc:
            assert str(exc) == "failure"
            failures += 1

    assert successes == 5
    assert failures == 5

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_cancelled_task_releases_execution_slot(executor):
    async def long_running_task():
        await asyncio.sleep(30)

    future = executor.submit(long_running_task())

    for _ in range(100):
        if executor.running == 1:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 1

    assert future.cancel()

    with pytest.raises(CancelledError):
        future.result(timeout=5)

    for _ in range(100):
        if executor.running == 0:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 0
    assert executor.available == executor.max_tasks


def test_executor_recovers_after_cancellation(executor):
    async def long_running_task():
        await asyncio.sleep(30)

    async def successful_task():
        return "success"

    future = executor.submit(long_running_task())

    for _ in range(100):
        if executor.running == 1:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 1

    assert future.cancel()

    with pytest.raises(CancelledError):
        future.result(timeout=5)

    for _ in range(100):
        if executor.running == 0:
            break

        import time
        time.sleep(0.01)

    assert executor.running == 0
    assert executor.available == executor.max_tasks

    result = executor.submit(successful_task())

    assert result.result(timeout=5) == "success"

    assert executor.running == 0
    assert executor.available == executor.max_tasks



def test_long_running_tasks():
    executor = AsyncExecutor(
        max_tasks=2,
        slow_task_threshold=0.1,
    )
    executor.start()

    async def slow_task():
        await asyncio.sleep(0.3)
        return "done"

    try:
        future = executor.submit(
            slow_task(),
            task_id="slow-1",
            task_name="test.slow_task",
        )

        # Give the coroutine enough time to cross the threshold.
        time.sleep(0.15)

        tasks = executor.long_running_tasks()

        assert len(tasks) == 1
        assert tasks[0]["task_id"] == "slow-1"
        assert tasks[0]["task_name"] == "test.slow_task"
        assert tasks[0]["state"] == "RUNNING"
        assert tasks[0]["duration"] >= 0.1

        stats = executor.stats()

        assert stats["running"] == 1
        assert stats["long_running"] == 1
        assert stats["slow_task_threshold"] == 0.1

        assert future.result(timeout=2) == "done"

        assert executor.long_running_tasks() == []

    finally:
        executor.shutdown()

def test_task_cancellation():
    executor = AsyncExecutor(
        max_tasks=1,
        slow_task_threshold=1,
    )
    executor.start()

    async def cancellable_task():
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise

    future = executor.submit(
        cancellable_task(),
        task_id="cancel-1",
        task_name="test.cancellable",
    )

    try:
        time.sleep(0.1)

        # Cancel the asyncio coroutine through the asyncio loop.
        executor.loop.call_soon_threadsafe(
            lambda: [
                task.cancel()
                for task in asyncio.all_tasks(executor.loop)
                if task.get_name() != "Task-1"
            ]
        )

        with pytest.raises(CancelledError):
            future.result(timeout=2)

        stats = executor.stats()

        assert stats["cancelled"] == 1
        assert stats["running"] == 0
        assert stats["available"] == 1
        assert stats["total"] == 1

    finally:
        executor.shutdown()




def test_monitor_tracks_successful_task():
    executor = AsyncExecutor(max_tasks=1)

    executor.start()

    async def work():
        await asyncio.sleep(0.01)
        return "done"

    future = executor.submit(
        work(),
        task_id="monitor-success",
        task_name="work",
    )

    assert future.result(timeout=2) == "done"

    history = executor.monitor.history()

    assert len(history) == 1
    assert history[0]["task_id"] == "monitor-success"
    assert history[0]["task_name"] == "work"
    assert history[0]["state"] == "SUCCESS"
    assert history[0]["runtime"] is not None

    executor.shutdown()


def test_monitor_tracks_failed_task():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def failing_work():
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    future = executor.submit(
        failing_work(),
        task_id="monitor-failure",
        task_name="failing_work",
    )

    with pytest.raises(ValueError, match="boom"):
        future.result(timeout=2)

    history = executor.monitor.history()

    assert len(history) == 1
    assert history[0]["task_id"] == "monitor-failure"
    assert history[0]["state"] == "FAILURE"
    assert history[0]["exception"] == "boom"

    executor.shutdown()


def test_monitor_tracks_running_cancellation():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def slow_work():
        await asyncio.sleep(10)

    future = executor.submit(
        slow_work(),
        task_id="monitor-cancel",
        task_name="slow_work",
    )

    time.sleep(0.05)

    assert executor.cancel_task(
        "monitor-cancel",
        reason="test cancellation",
    )

    with pytest.raises(CancelledError):
        future.result(timeout=2)

    history = executor.monitor.history()

    assert len(history) == 1
    assert history[0]["task_id"] == "monitor-cancel"
    assert history[0]["state"] == "CANCELLED"
    assert history[0]["exception"] == "test cancellation"

    executor.shutdown()

def test_monitor_tracks_queued_cancellation():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def slow_work():
        await asyncio.sleep(1)

    first = executor.submit(
        slow_work(),
        task_id="monitor-first",
        task_name="first",
    )

    time.sleep(0.05)

    second = executor.submit(
        slow_work(),
        task_id="monitor-second",
        task_name="second",
    )

    time.sleep(0.05)

    assert executor.cancel_task(
        "monitor-second",
        reason="queued cancellation",
    )

    with pytest.raises(CancelledError):
        second.result(timeout=2)

    history = executor.monitor.history()

    cancelled = next(
        task for task in history
        if task["task_id"] == "monitor-second"
    )

    assert cancelled["state"] == "CANCELLED"
    assert cancelled["exception"] == "queued cancellation"

    first.result(timeout=2)

    executor.shutdown()


def test_monitor_tracks_task_progress():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def work():
        executor.monitor.record_progress(
            "progress-task",
            current=25,
            total=100,
            message="Quarter complete",
        )

        await asyncio.sleep(0.01)

        executor.monitor.record_progress(
            "progress-task",
            current=100,
            total=100,
            message="Complete",
        )

        return "done"

    future = executor.submit(
        work(),
        task_id="progress-task",
        task_name="progress_work",
    )

    assert future.result(timeout=2) == "done"

    history = executor.monitor.history()

    assert len(history) == 1

    task = history[0]

    assert task["task_id"] == "progress-task"
    assert task["state"] == "SUCCESS"
    assert task["progress"]["current"] == 100
    assert task["progress"]["total"] == 100
    assert task["progress"]["percentage"] == 100
    assert task["progress"]["message"] == "Complete"

    executor.shutdown()


def test_running_tasks_includes_progress():
    executor = AsyncExecutor(max_tasks=1)
    executor.start()

    async def work():
        executor.monitor.record_progress(
            "running-progress",
            current=50,
            total=100,
            message="Halfway",
        )

        await asyncio.sleep(0.2)

    future = executor.submit(
        work(),
        task_id="running-progress",
        task_name="progress_work",
    )

    try:
        for _ in range(100):
            tasks = executor.running_tasks()

            if tasks:
                break

            time.sleep(0.01)

        assert len(tasks) == 1

        task = tasks[0]

        assert task["task_id"] == "running-progress"
        assert task["state"] == "RUNNING"
        assert task["progress"]["current"] == 50
        assert task["progress"]["total"] == 100
        assert task["progress"]["percentage"] == 50
        assert task["progress"]["message"] == "Halfway"

        future.result(timeout=2)

    finally:
        executor.shutdown()
