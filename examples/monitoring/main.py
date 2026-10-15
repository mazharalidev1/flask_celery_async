import asyncio

from flask import Flask

from flask_async_celery import AsyncCelery

app = Flask(__name__)

app.config["PROMETHEUS_ENABLED"] = True

celery = AsyncCelery(
    app,
    broker_url="redis://127.0.0.1:6379/0",
    result_backend="redis://127.0.0.1:6379/1",
    max_tasks=5,
)

@celery.task
async def slow_task(seconds=30):
    await asyncio.sleep(seconds)

    return {
        "status": "completed",
        "seconds": seconds,
    }


@celery.task
async def successful_task():
    await asyncio.sleep(1)
    return "success"


@celery.task
async def failing_task():
    await asyncio.sleep(1)
    raise ValueError("intentional test failure")


from flask_async_celery import AsyncCelery, update_progress

@celery.task
async def progress_task(total=10):
    for current in range(1, total + 1):
        await asyncio.sleep(1)

        await update_progress(
            current=current,
            total=total,
            message=f"Processed {current} of {total}",
        )

    return {"status": "completed", "total": total}


celery_app = celery.celery