import asyncio

from opentelemetry import trace

from app import async_celery

tracer = trace.get_tracer("otel-demo.tasks")


@async_celery.task
async def generate_report():
    with tracer.start_as_current_span("report.fetch_data"):
        await asyncio.sleep(0.2)

    with tracer.start_as_current_span("report.build_pdf"):
        await asyncio.sleep(0.3)

    return {"status": "completed"}


@async_celery.task
async def failing_report():
    with tracer.start_as_current_span("report.fetch_data"):
        await asyncio.sleep(0.1)

    raise RuntimeError("Example report generation failure")


@async_celery.task(bind=True, max_retries=1)
async def retry_report(self):
    should_retry = self.request.retries == 0

    with tracer.start_as_current_span("report.process"):
        await asyncio.sleep(0.1)

    if should_retry:
        raise self.retry(
            exc=RuntimeError("Simulated temporary failure"),
            countdown=2,
        )

    return {"status": "completed after retry"}