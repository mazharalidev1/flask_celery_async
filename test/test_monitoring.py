import time

import pytest

from flask_async_celery.monitoring import TaskMonitor


def test_monitor_initial_state():
    monitor = TaskMonitor()

    assert monitor.active_count == 0
    assert monitor.history_count == 0
    assert monitor.list_tasks() == []
    assert monitor.history() == []


def test_record_received():
    monitor = TaskMonitor()

    task = monitor.record_received(
        "task-1",
        task_name="example_task",
        worker="worker-1",
    )

    assert task.task_id == "task-1"
    assert task.task_name == "example_task"
    assert task.worker == "worker-1"
    assert task.state == "RECEIVED"
    assert task.received_at is not None


def test_record_started():
    monitor = TaskMonitor()

    monitor.record_received("task-1")

    time.sleep(0.001)

    task = monitor.record_started("task-1")

    assert task.state == "RUNNING"
    assert task.started_at is not None
    assert task.queue_time is not None
    assert task.queue_time >= 0


def test_record_started_without_received():
    monitor = TaskMonitor()

    task = monitor.record_started(
        "task-1",
        task_name="example_task",
    )

    assert task.state == "RUNNING"
    assert task.received_at is not None
    assert task.started_at is not None


def test_record_progress():
    monitor = TaskMonitor()

    monitor.record_started("task-1")

    task = monitor.record_progress(
        "task-1",
        current=25,
        total=100,
        message="Processing files",
    )

    assert task.progress_current == 25
    assert task.progress_total == 100
    assert task.progress_message == "Processing files"
    assert task.progress_percentage == 25


def test_record_success_moves_task_to_history():
    monitor = TaskMonitor()

    monitor.record_started("task-1")

    task = monitor.record_success("task-1")

    assert task.state == "SUCCESS"
    assert task.finished_at is not None
    assert task.runtime is not None

    assert monitor.get_task("task-1") is None
    assert monitor.active_count == 0
    assert monitor.history_count == 1


def test_record_failure():
    monitor = TaskMonitor()

    monitor.record_started("task-1")

    task = monitor.record_failure(
        "task-1",
        ValueError("something went wrong"),
    )

    assert task.state == "FAILURE"
    assert task.exception == "something went wrong"
    assert task.finished_at is not None

    assert monitor.get_task("task-1") is None
    assert monitor.history_count == 1


def test_record_cancelled():
    monitor = TaskMonitor()

    monitor.record_started("task-1")

    task = monitor.record_cancelled(
        "task-1",
        reason="user requested cancellation",
    )

    assert task.state == "CANCELLED"
    assert task.exception == "user requested cancellation"
    assert task.finished_at is not None

    assert monitor.get_task("task-1") is None
    assert monitor.history_count == 1


def test_list_active_tasks():
    monitor = TaskMonitor()

    monitor.record_started(
        "task-1",
        task_name="first",
    )

    monitor.record_started(
        "task-2",
        task_name="second",
    )

    tasks = monitor.list_tasks()

    assert len(tasks) == 2
    assert {task["task_id"] for task in tasks} == {
        "task-1",
        "task-2",
    }


def test_get_task():
    monitor = TaskMonitor()

    monitor.record_started(
        "task-1",
        task_name="example_task",
    )

    task = monitor.get_task("task-1")

    assert task is not None
    assert task["task_id"] == "task-1"
    assert task["task_name"] == "example_task"
    assert task["state"] == "RUNNING"


def test_get_unknown_task():
    monitor = TaskMonitor()

    assert monitor.get_task("does-not-exist") is None


def test_history_is_newest_first():
    monitor = TaskMonitor()

    monitor.record_started("task-1")
    monitor.record_success("task-1")

    monitor.record_started("task-2")
    monitor.record_success("task-2")

    history = monitor.history()

    assert [task["task_id"] for task in history] == [
        "task-2",
        "task-1",
    ]


def test_history_is_bounded():
    monitor = TaskMonitor(max_history=2)

    for task_id in ("task-1", "task-2", "task-3"):
        monitor.record_started(task_id)
        monitor.record_success(task_id)

    history = monitor.history()

    assert len(history) == 2
    assert [task["task_id"] for task in history] == [
        "task-3",
        "task-2",
    ]


def test_invalid_max_history():
    with pytest.raises(ValueError, match="max_history"):
        TaskMonitor(max_history=0)


def test_task_to_dict_contains_expected_fields():
    monitor = TaskMonitor()

    monitor.record_started(
        "task-1",
        task_name="example_task",
        worker="worker-1",
    )

    monitor.record_progress(
        "task-1",
        current=50,
        total=200,
        message="Halfway",
    )

    task = monitor.get_task("task-1")

    assert task is not None
    assert task["task_id"] == "task-1"
    assert task["task_name"] == "example_task"
    assert task["worker"] == "worker-1"
    assert task["state"] == "RUNNING"

    assert task["progress"]["current"] == 50
    assert task["progress"]["total"] == 200
    assert task["progress"]["percentage"] == 25
    assert task["progress"]["message"] == "Halfway"


def test_clear():
    monitor = TaskMonitor()

    monitor.record_started("task-1")
    monitor.record_success("task-1")

    monitor.record_started("task-2")

    monitor.clear()

    assert monitor.active_count == 0
    assert monitor.history_count == 0
    assert monitor.list_tasks() == []
    assert monitor.history() == []


def test_metrics_track_successful_task():
    monitor = TaskMonitor()

    monitor.record_received(
        "task-1",
        task_name="example.task",
        worker="celery@worker1",
    )
    monitor.record_started(
        "task-1",
        task_name="example.task",
        worker="celery@worker1",
    )
    monitor.record_success("task-1")

    metrics = monitor.metrics()

    assert len(metrics) == 1

    metric = metrics[0]

    assert metric["task_name"] == "example.task"
    assert metric["worker"] == "celery@worker1"
    assert metric["received"] == 1
    assert metric["started"] == 1
    assert metric["completed"] == 1
    assert metric["failed"] == 0
    assert metric["cancelled"] == 0
    assert metric["total_runtime"] >= 0
    assert metric["total_queue_time"] >= 0


def test_metrics_track_failures_and_cancellations():
    monitor = TaskMonitor()

    monitor.record_received(
        "task-1",
        task_name="example.task",
        worker="celery@worker1",
    )
    monitor.record_started("task-1")
    monitor.record_failure("task-1", ValueError("test"))

    monitor.record_received(
        "task-2",
        task_name="example.task",
        worker="celery@worker1",
    )
    monitor.record_started("task-2")
    monitor.record_cancelled("task-2", reason="SIGTERM")

    metrics = monitor.metrics()

    assert len(metrics) == 1

    metric = metrics[0]

    assert metric["received"] == 2
    assert metric["started"] == 2
    assert metric["completed"] == 0
    assert metric["failed"] == 1
    assert metric["cancelled"] == 1


def test_metrics_are_separated_by_task_name_and_worker():
    monitor = TaskMonitor()

    monitor.record_received(
        "task-1",
        task_name="example.task_a",
        worker="celery@worker1",
    )
    monitor.record_started("task-1")
    monitor.record_success("task-1")

    monitor.record_received(
        "task-2",
        task_name="example.task_b",
        worker="celery@worker2",
    )
    monitor.record_started("task-2")
    monitor.record_success("task-2")

    metrics = monitor.metrics()

    assert len(metrics) == 2

    keys = {
        (metric["task_name"], metric["worker"])
        for metric in metrics
    }

    assert keys == {
        ("example.task_a", "celery@worker1"),
        ("example.task_b", "celery@worker2"),
    }


def test_metrics_survive_history_limit():
    monitor = TaskMonitor(max_history=1)

    for index in range(3):
        task_id = f"task-{index}"

        monitor.record_received(
            task_id,
            task_name="example.task",
            worker="celery@worker1",
        )
        monitor.record_started(task_id)
        monitor.record_success(task_id)

    assert len(monitor.history()) == 1

    metrics = monitor.metrics()

    assert len(metrics) == 1
    assert metrics[0]["completed"] == 3