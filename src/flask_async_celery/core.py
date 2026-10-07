from __future__ import annotations

from typing import Any

from celery import Celery

from .bootstep import AsyncIOBootStep
from .hub_bootstep import AsyncIOHubWakeupBootStep


class AsyncCeleryCore:
    """Framework-independent Celery configuration for async execution."""

    def __init__(
        self,
        *,
        celery: Celery | None = None,
        app_name: str = "flask_async_celery",
        broker_url: str | None = None,
        result_backend: str | None = None,
        max_tasks: int = 20,
        disable_prefetch: bool = True,
        slow_task_threshold: float = 30.0,
        **celery_options: Any,
    ) -> None:
        if max_tasks < 1:
            raise ValueError("max_tasks must be greater than zero")

        self.max_tasks = max_tasks
        self.disable_prefetch = disable_prefetch
        self.slow_task_threshold = slow_task_threshold

        if celery is not None:
            self.celery = celery
        else:
            self.celery = Celery(
                app_name,
                broker=broker_url,
                backend=result_backend,
                **celery_options,
            )

        self.configure()

    def configure(self) -> None:
        self.celery.conf.update(
            worker_pool="flask_async_celery.pool:AsyncIOPool",
            worker_concurrency=self.max_tasks,
            worker_disable_prefetch=self.disable_prefetch,
            ASYNC_CELERY_SLOW_TASK_THRESHOLD=self.slow_task_threshold,
        )

        self.celery.steps["worker"].add(AsyncIOBootStep)
        self.celery.steps["worker"].add(AsyncIOHubWakeupBootStep)

    def send_task(self, *args: Any, **kwargs: Any):
        return self.celery.send_task(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.celery, name)