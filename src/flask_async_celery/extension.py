from __future__ import annotations

from typing import Any

from celery import Celery
from flask import Flask
from flask import Response

from .context import TaskContext
from .core import AsyncCeleryCore
from .task import AsyncTask


class AsyncCelery:
    """
    Flask integration for flask-async-celery.

    Configures a Celery application to use AsyncIOPool and
    optionally enables Celery consumer-side backpressure.
    """

    def __init__(
            self,
            app: Flask | None = None,
            *,
            celery: Celery | None = None,
            broker_url: str | None = None,
            result_backend: str | None = None,
            max_tasks: int = 20,
            disable_prefetch: bool = True,
            slow_task_threshold: float = 30.0,
            **celery_options: Any,
    ) -> None:
        if max_tasks < 1:
            raise ValueError(
                "max_tasks must be greater than zero"
            )

        self.app: Flask | None = None

        self.max_tasks = max_tasks
        self.disable_prefetch = disable_prefetch
        self.slow_task_threshold = slow_task_threshold

        self.core = AsyncCeleryCore(
            celery=celery,
            app_name=app.import_name if app is not None else "flask_async_celery",
            broker_url=broker_url,
            result_backend=result_backend,
            max_tasks=max_tasks,
            disable_prefetch=disable_prefetch,
            slow_task_threshold=slow_task_threshold,
            **celery_options,
        )

        self.celery = self.core.celery

        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        self.app = app

        app.extensions["async_celery"] = self

        # Make the Flask application available to AsyncTask.
        self.celery.flask_app = app
        self.celery.task_context = TaskContext(app.app_context)

        app.config.setdefault(
            "ASYNC_CELERY_MAX_TASKS",
            self.max_tasks,
        )

        app.config.setdefault(
            "ASYNC_CELERY_DISABLE_PREFETCH",
            self.disable_prefetch,
        )

        app.config.setdefault(
            "ASYNC_CELERY_SLOW_TASK_THRESHOLD",
            self.slow_task_threshold,
        )

        app.config.setdefault(
            "PROMETHEUS_ENABLED",
            False,
        )

        self.max_tasks = int(
            app.config["ASYNC_CELERY_MAX_TASKS"]
        )

        self.disable_prefetch = bool(
            app.config["ASYNC_CELERY_DISABLE_PREFETCH"]
        )

        self.slow_task_threshold = float(
            app.config["ASYNC_CELERY_SLOW_TASK_THRESHOLD"]
        )

        if self.max_tasks < 1:
            raise ValueError(
                "ASYNC_CELERY_MAX_TASKS must be greater than zero"
            )

        self.core.max_tasks = self.max_tasks
        self.core.disable_prefetch = self.disable_prefetch
        self.core.slow_task_threshold = self.slow_task_threshold

        # Reconfigure Celery using Flask's final configuration.
        self.core.configure()

        self._register_metrics_endpoint()

    def task(self, *args: Any, **kwargs: Any):
        """
        Register a Celery task.

        Async tasks use AsyncTask by default.

        Usage:

            @async_celery.task
            async def task():
                ...

        A custom task base can still be provided:

            @async_celery.task(base=CustomTask)
            def task():
                ...
        """

        kwargs.setdefault("base", AsyncTask)

        return self.celery.task(*args, **kwargs)

    def send_task(self, *args: Any, **kwargs: Any):
        return self.celery.send_task(*args, **kwargs)

    def _register_metrics_endpoint(self) -> None:
        """Register the Prometheus metrics endpoint."""

        app = self.app

        if app is None:
            return

        if not app.config.get("PROMETHEUS_ENABLED", False):
            return

        try:
            from .metrics import create_registry
            from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        except ImportError as exc:
            raise RuntimeError(
                "Prometheus support requires prometheus-client. "
                "Install it with: pip install flask-async-celery[prometheus]"
            ) from exc

        registry = create_registry(self.celery)

        @app.route("/metrics")
        def metrics():
            return Response(
                generate_latest(registry),
                mimetype=CONTENT_TYPE_LATEST,
            )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.celery, name)
