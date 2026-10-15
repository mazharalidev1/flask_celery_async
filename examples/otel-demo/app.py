import os

from flask import Flask, jsonify
from opentelemetry.instrumentation.flask import FlaskInstrumentor

from flask_async_celery import AsyncCelery
from tracing import configure_tracing

configure_tracing("otel-demo-api")

app = Flask(__name__)

async_celery = AsyncCelery(
    app,
    broker_url=os.getenv(
        "CELERY_BROKER_URL",
        "redis://localhost:6379/0",
    ),
    result_backend=os.getenv(
        "CELERY_RESULT_BACKEND",
        "redis://localhost:6379/1",
    ),
    max_tasks=10,
)

FlaskInstrumentor().instrument_app(app)


@app.get("/")
def index():
    return jsonify({
        "service": "otel-demo-api",
        "message": "Try /run/success, /run/failure, or /run/retry",
    })


@app.post("/run/success")
def run_success():
    from tasks import generate_report

    result = generate_report.delay()
    return jsonify({"task_id": result.id}), 202


@app.post("/run/failure")
def run_failure():
    from tasks import failing_report

    result = failing_report.delay()
    return jsonify({"task_id": result.id}), 202


@app.post("/run/retry")
def run_retry():
    from tasks import retry_report

    result = retry_report.delay()
    return jsonify({"task_id": result.id}), 202


# Import tasks so Celery registers them when this module loads.
import tasks  # noqa: E402, F401

celery = async_celery.celery