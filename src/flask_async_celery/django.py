from __future__ import annotations

from celery import Celery

from .core import AsyncCeleryCore
from .context import TaskContext

class DjangoAsyncCelery:
    """
    Django integration for flask-async-celery.

    Django application initialization and ORM lifecycle are
    intentionally kept outside the core async executor.
    """

    def __init__(
        self,
        *,
        celery: Celery | None = None,
        broker_url: str | None = None,
        result_backend: str | None = None,
        max_tasks: int = 20,
        disable_prefetch: bool = True,
        slow_task_threshold: float = 30.0,
        **celery_options,
    ) -> None:
        self.core = AsyncCeleryCore(
            celery=celery,
            app_name="django_async_celery",
            broker_url=broker_url,
            result_backend=result_backend,
            max_tasks=max_tasks,
            disable_prefetch=disable_prefetch,
            slow_task_threshold=slow_task_threshold,
            **celery_options,
        )

        self.celery = self.core.celery
        self.celery.task_context = TaskContext()

    def task(self, *args, **kwargs):
        from .task import AsyncTask

        kwargs.setdefault("base", AsyncTask)
        return self.celery.task(*args, **kwargs)

    def send_task(self, *args, **kwargs):
        return self.celery.send_task(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.celery, name)