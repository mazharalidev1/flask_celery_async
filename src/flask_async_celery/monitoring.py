from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class TaskRecord:
    """Runtime information about an async Celery task."""

    task_id: str
    task_name: str | None = None
    worker: str | None = None

    state: str = "PENDING"

    received_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None

    progress_current: int | float | None = None
    progress_total: int | float | None = None
    progress_message: str | None = None

    exception: str | None = None

    @property
    def runtime(self) -> float | None:
        """Return task runtime in seconds."""
        if self.started_at is None:
            return None

        end = self.finished_at or time.monotonic()
        return end - self.started_at

    @property
    def queue_time(self) -> float | None:
        """Return time spent waiting before execution."""
        if self.received_at is None or self.started_at is None:
            return None

        return self.started_at - self.received_at

    @property
    def progress_percentage(self) -> float | None:
        """Return progress percentage when total is available."""
        if (
            self.progress_current is None
            or self.progress_total is None
            or self.progress_total <= 0
        ):
            return None

        return (self.progress_current / self.progress_total) * 100

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "worker": self.worker,
            "state": self.state,
            "received_at": self.received_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "runtime": self.runtime,
            "queue_time": self.queue_time,
            "progress": {
                "current": self.progress_current,
                "total": self.progress_total,
                "percentage": self.progress_percentage,
                "message": self.progress_message,
            },
            "exception": self.exception,
        }

@dataclass
class TaskMetricRecord:
    """Aggregate metrics for a task name and worker."""

    task_name: str
    worker: str | None = None

    received: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0

    total_runtime: float = 0.0
    total_queue_time: float = 0.0

    @property
    def average_runtime(self) -> float:
        if self.completed + self.failed + self.cancelled == 0:
            return 0.0

        return self.total_runtime / (
            self.completed + self.failed + self.cancelled
        )

    @property
    def average_queue_time(self) -> float:
        if self.started == 0:
            return 0.0

        return self.total_queue_time / self.started

class TaskMonitor:
    """Thread-safe in-memory monitor for async Celery tasks."""

    def __init__(
        self,
        *,
        max_history: int = 1000,
        worker: str | None = None,
    ) -> None:
        if max_history < 1:
            raise ValueError("max_history must be greater than zero")

        self.max_history = max_history
        self.worker = worker

        self._lock = threading.RLock()

        self._tasks: dict[str, TaskRecord] = {}
        self._history: list[TaskRecord] = []

        self._metrics: dict[tuple[str, str | None], TaskMetricRecord] = {}

    def _get_or_create(
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        worker: str | None = None,
    ) -> TaskRecord:
        task = self._tasks.get(task_id)

        if task is None:
            task = TaskRecord(
                task_id=task_id,
                task_name=task_name,
                worker=worker or self.worker,
            )
            self._tasks[task_id] = task
        else:
            if task_name is not None:
                task.task_name = task_name

            if worker is not None:
                task.worker = worker

        return task

    def record_received(
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        worker: str | None = None,
    ) -> TaskRecord:
        """Record that a task was received."""
        with self._lock:
            task = self._get_or_create(
                task_id,
                task_name=task_name,
                worker=worker,
            )

            task.state = "RECEIVED"
            task.received_at = time.monotonic()

            metric = self._get_metric_record(
                task.task_name,
                task.worker,
            )
            metric.received += 1

            return task

    def record_started(
        self,
        task_id: str,
        *,
        task_name: str | None = None,
        worker: str | None = None,
    ) -> TaskRecord:
        """Record that a task started executing."""
        with self._lock:
            task = self._get_or_create(
                task_id,
                task_name=task_name,
                worker=worker,
            )

            task.state = "RUNNING"

            if task.received_at is None:
                task.received_at = time.monotonic()

            task.started_at = time.monotonic()

            metric = self._get_metric_record(
                task.task_name,
                task.worker,
            )

            metric.started += 1

            if task.queue_time is not None:
                metric.total_queue_time += task.queue_time

            return task

    def record_progress(
        self,
        task_id: str,
        *,
        current: int | float,
        total: int | float | None = None,
        message: str | None = None,
    ) -> TaskRecord:
        """Record task progress."""
        with self._lock:
            task = self._get_or_create(task_id)

            task.progress_current = current
            task.progress_total = total
            task.progress_message = message

            return task

    def record_success(self, task_id: str) -> TaskRecord:
        """Record successful task completion."""
        return self._record_finished(task_id, "SUCCESS")

    def record_failure(
            self,
            task_id: str,
            exception: BaseException | str,
    ) -> TaskRecord:
        """Record task failure."""
        with self._lock:
            task = self._get_or_create(task_id)

            task.state = "FAILURE"
            task.finished_at = time.monotonic()
            task.exception = str(exception)

            metric = self._get_metric_record(
                task.task_name,
                task.worker,
            )

            metric.failed += 1

            runtime = task.runtime
            if runtime is not None:
                metric.total_runtime += runtime

            self._move_to_history(task)

            return task

    def record_cancelled(
            self,
            task_id: str,
            reason: str | None = None,
    ) -> TaskRecord:
        """Record task cancellation."""
        with self._lock:
            task = self._get_or_create(task_id)

            task.state = "CANCELLED"
            task.finished_at = time.monotonic()

            if reason:
                task.exception = reason

            metric = self._get_metric_record(
                task.task_name,
                task.worker,
            )

            metric.cancelled += 1

            runtime = task.runtime
            if runtime is not None:
                metric.total_runtime += runtime

            self._move_to_history(task)

            return task

    def _record_finished(
            self,
            task_id: str,
            state: str,
    ) -> TaskRecord:
        with self._lock:
            task = self._get_or_create(task_id)

            task.state = state
            task.finished_at = time.monotonic()

            metric = self._get_metric_record(
                task.task_name,
                task.worker,
            )

            if state == "SUCCESS":
                metric.completed += 1

            runtime = task.runtime
            if runtime is not None:
                metric.total_runtime += runtime

            self._move_to_history(task)

            return task

    def _move_to_history(self, task: TaskRecord) -> None:
        """Move a completed task into bounded history."""
        self._tasks.pop(task.task_id, None)

        self._history.append(task)

        if len(self._history) > self.max_history:
            del self._history[
                : len(self._history) - self.max_history
            ]

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        """Return a running task."""
        with self._lock:
            task = self._tasks.get(task_id)

            if task is None:
                return None

            return task.to_dict()

    def list_tasks(self) -> list[dict[str, Any]]:
        """Return currently running/active tasks."""
        with self._lock:
            return [
                task.to_dict()
                for task in self._tasks.values()
            ]

    def history(self) -> list[dict[str, Any]]:
        """Return completed task history."""
        with self._lock:
            return [
                task.to_dict()
                for task in reversed(self._history)
            ]

    def clear(self) -> None:
        """Clear active tasks and history."""
        with self._lock:
            self._tasks.clear()
            self._history.clear()
            self._metrics.clear()

    @property
    def active_count(self) -> int:
        """Return number of active tasks."""
        with self._lock:
            return len(self._tasks)

    @property
    def history_count(self) -> int:
        """Return number of historical tasks."""
        with self._lock:
            return len(self._history)

    def _get_metric_record(
            self,
            task_name: str | None,
            worker: str | None,
    ) -> TaskMetricRecord:
        """Return the aggregate metric record for a task."""
        name = task_name or "unknown"

        key = (name, worker)

        metric = self._metrics.get(key)

        if metric is None:
            metric = TaskMetricRecord(
                task_name=name,
                worker=worker,
            )
            self._metrics[key] = metric

        return metric

    def metrics(self) -> list[dict[str, Any]]:
        """Return aggregate task metrics."""
        with self._lock:
            return [
                {
                    "task_name": metric.task_name,
                    "worker": metric.worker,
                    "received": metric.received,
                    "started": metric.started,
                    "completed": metric.completed,
                    "failed": metric.failed,
                    "cancelled": metric.cancelled,
                    "total_runtime": metric.total_runtime,
                    "average_runtime": metric.average_runtime,
                    "total_queue_time": metric.total_queue_time,
                    "average_queue_time": metric.average_queue_time,
                }
                for metric in self._metrics.values()
            ]