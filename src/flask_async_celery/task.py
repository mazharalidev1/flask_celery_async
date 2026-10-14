from __future__ import annotations

import asyncio
import inspect
import time
from concurrent.futures import CancelledError
from contextvars import ContextVar
from typing import Any

from celery import Task
from celery.exceptions import Ignore
from celery.exceptions import Retry

from .tracing import (
    extract_trace_context,
    start_as_current_span,
)
from .context import TaskContext

_async_executor: ContextVar[Any | None] = ContextVar(
    "async_celery_executor",
    default=None,
)


def set_async_executor(executor: Any):
    return _async_executor.set(executor)


def reset_async_executor(token) -> None:
    _async_executor.reset(token)


def get_async_executor() -> Any | None:
    return _async_executor.get()


async def update_progress(
        *,
        current: int | float,
        total: int | float | None = None,
        message: str | None = None,
) -> None:
    """
    Update progress for the currently executing async task.
    """
    executor = get_async_executor()

    if executor is None:
        raise RuntimeError(
            "update_progress() requires an AsyncExecutor. "
            "Make sure the task is running inside AsyncIOPool."
        )

    task_id = executor.current_task_id

    if task_id is None:
        raise RuntimeError(
            "update_progress() must be called from an active async task."
        )

    executor.monitor.record_progress(
        task_id,
        current=current,
        total=total,
        message=message,
    )


class AsyncTask(Task):
    """
    Celery Task base for native async tasks.

    Async tasks are executed on the persistent asyncio event loop
    managed by AsyncExecutor.

    When registered through AsyncCelery, the Flask application
    is made available through the Celery application and an
    application context is pushed while the async task executes.
    """

    abstract = True

    async def update_progress(
            self,
            *,
            current: int | float,
            total: int | float | None = None,
            message: str | None = None,
    ) -> None:
        """Update progress for the currently executing async task."""
        executor = get_async_executor()

        if executor is None:
            raise RuntimeError(
                "AsyncTask progress reporting requires an AsyncExecutor."
            )

        executor.monitor.record_progress(
            self.request.id,
            current=current,
            total=total,
            message=message,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not inspect.iscoroutinefunction(self.run):
            return super().__call__(*args, **kwargs)

        executor = get_async_executor()

        if executor is None:
            raise RuntimeError(
                "AsyncTask requires an AsyncExecutor. "
                "Make sure the task is running inside AsyncIOPool."
            )

        # Capture the real Celery request while we're still on
        # the Celery worker/bridge thread.
        request = self.request

        # The Flask application is attached to the Celery app by AsyncCelery.
        task_context = getattr(
            self._get_app(),
            "task_context",
            None,
        )

        async def execute_with_request() -> Any:
            # Restore the Celery request inside the asyncio thread.
            self.push_request(**request.__dict__)

            try:
                attributes = {
                    "celery.task.name": self.name,
                    "celery.task.id": request.id,
                }

                worker = getattr(request, "hostname", None)
                if worker is not None:
                    attributes["celery.worker"] = worker

                trace_context = extract_trace_context(
                    getattr(request, "headers", None)
                )

                with start_as_current_span(
                        "celery.task",
                        attributes=attributes,
                        context=trace_context,
                ) as span:
                    monitor = getattr(executor, "monitor", None)
                    task_record = (
                        monitor.get_task(request.id)
                        if monitor is not None
                        else None
                    )

                    if span is not None and task_record is not None:
                        wait_seconds = task_record.get("queue_time")
                        if wait_seconds is not None:
                            span.set_attribute(
                                "flask_async_celery.executor_wait_seconds",
                                wait_seconds,
                            )

                    execution_started_at = time.monotonic()

                    try:
                        if task_context is not None:
                            with task_context():
                                result = await self.run(*args, **kwargs)
                        else:
                            result = await self.run(*args, **kwargs)

                    except asyncio.CancelledError:
                        if span is not None:
                            span.set_attribute(
                                "flask_async_celery.cancelled",
                                True,
                            )
                            span.set_attribute(
                                "celery.task.outcome",
                                "cancelled",
                            )
                        raise

                    except Retry:
                        if span is not None:
                            span.set_attribute(
                                "celery.task.outcome",
                                "retry",
                            )
                            span.set_attribute(
                                "celery.task.retry_requested",
                                True,
                            )
                        raise

                    except Exception as exc:
                        if span is not None:
                            span.set_attribute(
                                "celery.task.outcome",
                                "failure",
                            )
                            span.record_exception(exc)

                            try:
                                from opentelemetry.trace import (
                                    Status,
                                    StatusCode,
                                )

                                span.set_status(
                                    Status(
                                        StatusCode.ERROR,
                                        description=str(exc),
                                    )
                                )
                            except ImportError:
                                pass

                        raise

                    else:
                        if span is not None:
                            span.set_attribute(
                                "celery.task.outcome",
                                "success",
                            )
                        return result

                    finally:
                        if span is not None:
                            span.set_attribute(
                                "flask_async_celery.execution_seconds",
                                time.monotonic() - execution_started_at,
                            )

            finally:
                self.pop_request()

        coroutine = execute_with_request()

        try:
            future = executor.submit(
                coroutine,
                task_id=self.request.id,
                task_name=self.name,
                worker=getattr(request, "hostname", None),

            )
        except BaseException:
            if inspect.iscoroutine(coroutine):
                coroutine.close()
            raise

        try:
            return future.result()
        except CancelledError as exc:
            if executor.get_cancel_reason(request.id) is not None:
                raise Ignore("Task cancelled") from exc
            raise
        finally:
            executor.clear_cancel_reason(request.id)
