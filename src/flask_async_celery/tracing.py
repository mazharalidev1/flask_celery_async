"""Optional OpenTelemetry helpers for flask-async-celery."""

from contextlib import nullcontext
from typing import Any, Mapping

INSTRUMENTATION_NAME = "flask_async_celery"


def get_tracer():
    """Return the package tracer, or None if OpenTelemetry is unavailable."""
    try:
        from opentelemetry import trace
    except ImportError:
        return None

    return trace.get_tracer(INSTRUMENTATION_NAME)


def start_as_current_span(
    name: str,
    *,
    attributes: Mapping[str, Any] | None = None,
    context: Any = None,
    kind: Any = None,
):
    """Start a span when OpenTelemetry is available."""
    tracer = get_tracer()

    if tracer is None:
        return nullcontext()

    return tracer.start_as_current_span(
        name,
        attributes=attributes,
        context=context,
        kind=kind,
    )


def inject_trace_context(carrier: dict[str, Any]) -> None:
    """Inject the current OpenTelemetry context into a message carrier."""
    try:
        from opentelemetry.propagate import inject
    except ImportError:
        return

    inject(carrier)


def extract_trace_context(carrier: Mapping[str, Any] | None):
    """Extract an OpenTelemetry context from message headers."""
    try:
        from opentelemetry.propagate import extract
    except ImportError:
        return None

    return extract(dict(carrier or {}))

def setup_celery_tracing(celery_app: Any) -> None:
    """Register the global Celery trace-context injection hook once."""
    try:
        from celery.signals import before_task_publish
    except ImportError:
        return

    if getattr(celery_app, "_async_celery_otel_configured", False):
        return

    def inject_task_context(sender=None, headers=None, **kwargs):
        if headers is not None:
            inject_trace_context(headers)

    before_task_publish.connect(
        inject_task_context,
        weak=False,
        dispatch_uid="flask_async_celery_otel_trace_injection",
    )

    celery_app._async_celery_otel_configured = True
