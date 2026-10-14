import asyncio
from concurrent.futures import CancelledError, Future
from contextlib import nullcontext

import pytest
from celery import Celery
from celery.exceptions import Ignore
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import flask_async_celery.tracing as tracing
from flask_async_celery.task import AsyncTask, set_async_executor
from flask_async_celery.tracing import (
    extract_trace_context,
    inject_trace_context,
)
from flask_async_celery.tracing import (
    get_tracer,
    start_as_current_span,
)


class FakeExecutor:
    def __init__(self, cancel_reason=None):
        self.cancel_reason = cancel_reason
        self.monitor = FakeMonitor()

    def submit(self, coroutine, **kwargs):
        future = Future()
        task_id = kwargs["task_id"]

        self.monitor.record_received(task_id)
        self.monitor.record_started(task_id)

        try:
            future.set_result(asyncio.run(coroutine))
        except asyncio.CancelledError:
            future.set_exception(CancelledError())
        except BaseException as exc:
            future.set_exception(exc)

        return future

    def get_cancel_reason(self, task_id):
        return self.cancel_reason

    def clear_cancel_reason(self, task_id):
        self.cancel_reason = None


import time


class FakeMonitor:
    def __init__(self):
        self.tasks = {}

    def record_received(self, task_id, **kwargs):
        self.tasks[task_id] = {
            "queue_time": None,
            "runtime": None,
            "started_at": None,
        }

    def record_started(self, task_id, **kwargs):
        task = self.tasks[task_id]
        task["started_at"] = time.monotonic()
        task["queue_time"] = 0.0

    def get_task(self, task_id):
        task = self.tasks.get(task_id)
        if task is None:
            return None

        runtime = (
            time.monotonic() - task["started_at"]
            if task["started_at"] is not None
            else None
        )

        return {
            **task,
            "runtime": runtime,
        }


@pytest.fixture
def span_exporter(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    monkeypatch.setattr(
        tracing,
        "get_tracer",
        lambda: provider.get_tracer("test"),
    )

    yield exporter

    provider.shutdown()


def run_async_task(
        coroutine_function,
        *,
        executor=None,
        request_headers=None,
        bind=False,
):
    app = Celery(
        "otel_test",
        broker="memory://",
        backend="cache+memory://",
    )

    task = app.task(
        base=AsyncTask,
        name="test.async_task",
        bind=bind,
    )(coroutine_function)

    token = set_async_executor(executor or FakeExecutor())

    task.push_request(
        id="task-123",
        hostname="worker@test",
        headers=request_headers,
    )

    try:
        return task()
    finally:
        task.pop_request()
        from flask_async_celery.task import reset_async_executor
        reset_async_executor(token)


def test_async_task_creates_success_span(span_exporter):
    async def successful_task():
        return "ok"

    result = run_async_task(successful_task)

    assert result == "ok"

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.name == "celery.task"
    assert span.attributes["celery.task.name"] == "test.async_task"
    assert span.attributes["celery.task.id"] == "task-123"
    assert span.attributes["celery.worker"] == "worker@test"
    assert span.attributes["celery.task.outcome"] == "success"
    assert (
            "flask_async_celery.executor_wait_seconds"
            in span.attributes
    )

    assert (
            "flask_async_celery.execution_seconds"
            in span.attributes
    )

    assert span.attributes[
               "flask_async_celery.executor_wait_seconds"
           ] >= 0

    assert span.attributes[
               "flask_async_celery.execution_seconds"
           ] >= 0


def test_async_task_records_failure(span_exporter):
    async def failing_task():
        raise ValueError("task failed")

    with pytest.raises(ValueError, match="task failed"):
        run_async_task(failing_task)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.attributes["celery.task.outcome"] == "failure"
    assert span.status.status_code.name == "ERROR"
    assert any(
        event.name == "exception"
        for event in span.events
    )


def test_async_task_records_cancellation(span_exporter):
    async def cancelled_task():
        raise asyncio.CancelledError()

    executor = FakeExecutor(cancel_reason="revoked")

    with pytest.raises(Ignore, match="Task cancelled"):
        run_async_task(cancelled_task, executor=executor)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]
    assert span.attributes["flask_async_celery.cancelled"] is True
    assert span.attributes["celery.task.outcome"] == "cancelled"


def test_tracing_helpers_import():
    assert callable(get_tracer)
    assert callable(start_as_current_span)


def test_span_context_manager_works():
    with start_as_current_span("test.operation"):
        pass


def test_missing_opentelemetry_uses_noop(monkeypatch):
    import builtins

    original_import = builtins.__import__

    def import_without_otel(name, *args, **kwargs):
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ImportError("OpenTelemetry intentionally unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_otel)

    assert get_tracer() is None

    manager = start_as_current_span("test.noop")
    assert isinstance(manager, nullcontext)

    with manager:
        pass


def test_trace_context_helpers_work():
    carrier = {}
    inject_trace_context(carrier)

    # With no active span, an empty carrier is valid.
    assert isinstance(carrier, dict)

    context = extract_trace_context(carrier)
    assert context is not None


def test_extract_trace_context_from_empty_headers():
    context = extract_trace_context({})
    assert context is not None


def test_inject_trace_context_without_otel(monkeypatch):
    import builtins

    original_import = builtins.__import__

    def import_without_otel(name, *args, **kwargs):
        if name == "opentelemetry" or name.startswith("opentelemetry."):
            raise ImportError("OpenTelemetry unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_otel)

    carrier = {}
    inject_trace_context(carrier)
    assert carrier == {}

    assert extract_trace_context(carrier) is None


def test_celery_publish_hook_propagates_trace_context():
    from celery import Celery
    from celery.signals import before_task_publish
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    from flask_async_celery.tracing import (
        extract_trace_context,
        setup_celery_tracing,
    )

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("propagation-test")

    set_global_textmap(TraceContextTextMapPropagator())

    app = Celery("propagation_test", broker="memory://")
    setup_celery_tracing(app)

    headers = {}

    try:
        with tracer.start_as_current_span("producer") as producer:
            producer_context = producer.get_span_context()

            before_task_publish.send(
                sender="test.async_task",
                headers=headers,
            )

            assert "traceparent" in headers

            extracted = extract_trace_context(headers)

            with tracer.start_as_current_span(
                    "celery.task",
                    context=extracted,
            ):
                pass

        spans = {
            span.name: span
            for span in exporter.get_finished_spans()
        }

        child = spans["celery.task"]
        assert child.parent is not None
        assert child.parent.trace_id == producer_context.trace_id
        assert child.parent.span_id == producer_context.span_id

    finally:
        provider.shutdown()


def test_async_task_span_uses_producer_trace_context(span_exporter):
    from celery import Celery
    from celery.signals import before_task_publish

    import flask_async_celery.tracing as tracing
    from flask_async_celery.tracing import setup_celery_tracing

    app = Celery("end_to_end_trace_test", broker="memory://")
    setup_celery_tracing(app)

    headers = {}

    with tracing.get_tracer().start_as_current_span("producer") as producer:
        producer_context = producer.get_span_context()

        before_task_publish.send(
            sender="test.async_task",
            headers=headers,
        )

    assert "traceparent" in headers

    async def sample_task():
        return "ok"

    result = run_async_task(
        sample_task,
        request_headers=headers,
    )

    assert result == "ok"

    spans = {
        span.name: span
        for span in span_exporter.get_finished_spans()
    }

    task_span = spans["celery.task"]

    assert task_span.parent is not None
    assert task_span.parent.trace_id == producer_context.trace_id
    assert task_span.parent.span_id == producer_context.span_id


def test_async_task_records_retry(span_exporter):
    async def retrying_task(self):
        self.retry(countdown=0, max_retries=3)

    with pytest.raises(Exception) as exc_info:
        run_async_task(
            retrying_task,
            bind=True,
        )

    from celery.exceptions import Retry

    assert isinstance(exc_info.value, Retry)

    spans = span_exporter.get_finished_spans()
    assert len(spans) == 1

    span = spans[0]

    assert span.attributes["celery.task.outcome"] == "retry"
    assert span.attributes["celery.task.retry_requested"] is True
