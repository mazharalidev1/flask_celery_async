from __future__ import annotations

from prometheus_client import CollectorRegistry
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


class AsyncCeleryCollector:
    """Collect AsyncIO Celery worker metrics."""

    def __init__(self, celery_app):
        self.celery_app = celery_app

    @staticmethod
    def _gauge(name, documentation, worker, value):
        metric = GaugeMetricFamily(
            name,
            documentation,
            labels=["worker"],
        )
        metric.add_metric([worker], value)
        return metric

    @staticmethod
    def _counter(name, documentation, worker, value):
        metric = CounterMetricFamily(
            name,
            documentation,
            labels=["worker"],
        )
        metric.add_metric([worker], value)
        return metric

    @staticmethod
    def _task_counter(
            name,
            documentation,
            task_name,
            worker,
            value,
    ):
        metric = CounterMetricFamily(
            name,
            documentation,
            labels=["task_name", "worker"],
        )
        metric.add_metric(
            [task_name, worker or "unknown"],
            value,
        )
        return metric

    @staticmethod
    def _task_gauge(
            name,
            documentation,
            task_name,
            worker,
            value,
    ):
        metric = GaugeMetricFamily(
            name,
            documentation,
            labels=["task_name", "worker"],
        )
        metric.add_metric(
            [task_name, worker or "unknown"],
            value,
        )
        return metric

    def collect(self):
        replies = self.celery_app.control.broadcast(
            "async_stats",
            reply=True,
        )

        for reply in replies:
            for worker, stats in reply.items():
                yield self._gauge(
                    "flask_async_celery_running_tasks",
                    "Number of currently running async tasks.",
                    worker,
                    stats.get("running", 0),
                )

                yield self._gauge(
                    "flask_async_celery_available_slots",
                    "Number of available AsyncIO execution slots.",
                    worker,
                    stats.get("available", 0),
                )

                yield self._gauge(
                    "flask_async_celery_max_tasks",
                    "Maximum number of concurrent async tasks.",
                    worker,
                    stats.get("max_tasks", 0),
                )

                yield self._counter(
                    "flask_async_celery_completed",
                    "Total number of completed async tasks.",
                    worker,
                    stats.get("completed", 0),
                )

                yield self._counter(
                    "flask_async_celery_failed",
                    "Total number of failed async tasks.",
                    worker,
                    stats.get("failed", 0),
                )

                yield self._counter(
                    "flask_async_celery_cancelled",
                    "Total number of cancelled async tasks.",
                    worker,
                    stats.get("cancelled", 0),
                )

                yield self._gauge(
                    "flask_async_celery_average_duration_seconds",
                    "Average duration of completed async tasks in seconds.",
                    worker,
                    stats.get("average_duration", 0.0),
                )

                yield self._gauge(
                    "flask_async_celery_long_running_tasks",
                    "Number of async tasks currently above the slow-task threshold.",
                    worker,
                    stats.get("long_running", 0),
                )

                yield self._gauge(
                    "flask_async_celery_slow_task_threshold_seconds",
                    "Configured slow-task threshold in seconds.",
                    worker,
                    stats.get("slow_task_threshold", 0.0),
                )

                yield self._gauge(
                    "flask_async_celery_event_loop_running",
                    "Whether the AsyncIO event loop is running.",
                    worker,
                    int(stats.get("event_loop_running", False)),
                )

                yield self._gauge(
                    "flask_async_celery_stopping",
                    "Whether the AsyncIO executor is stopping.",
                    worker,
                    int(stats.get("stopping", False)),
                )
                for task_metric in stats.get("task_metrics", []):
                    task_name = task_metric.get(
                        "task_name",
                        "unknown",
                    )
                    task_worker = task_metric.get(
                        "worker",
                    ) or worker

                    yield self._task_counter(
                        "flask_async_celery_task_received",
                        "Total number of async tasks received by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("received", 0),
                    )

                    yield self._task_counter(
                        "flask_async_celery_task_started",
                        "Total number of async tasks started by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("started", 0),
                    )

                    yield self._task_counter(
                        "flask_async_celery_task_completed",
                        "Total number of async tasks completed successfully by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("completed", 0),
                    )

                    yield self._task_counter(
                        "flask_async_celery_task_failed",
                        "Total number of async tasks failed by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("failed", 0),
                    )

                    yield self._task_counter(
                        "flask_async_celery_task_cancelled",
                        "Total number of async tasks cancelled by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("cancelled", 0),
                    )

                    yield self._task_gauge(
                        "flask_async_celery_task_average_duration_seconds",
                        "Average execution duration of async tasks by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("average_runtime", 0.0),
                    )

                    yield self._task_gauge(
                        "flask_async_celery_task_average_queue_time_seconds",
                        "Average queue wait time of async tasks by task name.",
                        task_name,
                        task_worker,
                        task_metric.get("average_queue_time", 0.0),
                    )


def create_registry(celery_app) -> CollectorRegistry:
    """Create a Prometheus registry containing AsyncIO Celery metrics."""

    registry = CollectorRegistry()
    registry.register(AsyncCeleryCollector(celery_app))
    return registry
