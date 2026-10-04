from __future__ import annotations

from celery.worker.control import inspect_command


@inspect_command()
def async_stats(state, **kwargs):
    """Return AsyncIO executor statistics."""
    pool = state.consumer.pool
    executor = getattr(pool, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
        }

    return {
        "asyncio-enabled": True,
        **executor.stats(),
    }


@inspect_command()
def async_tasks(state, **kwargs):
    """Return currently running AsyncIO tasks."""
    pool = state.consumer.pool
    executor = getattr(pool, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
            "tasks": [],
        }

    return {
        "asyncio-enabled": True,
        "tasks": executor.running_tasks(),
    }

@inspect_command()
def async_task_info(state, task_id=None, **kwargs):
    pool = state.consumer.pool
    executor = getattr(pool, "async_executor", None)

    if executor is None:
        return {
            "asyncio-enabled": False,
            "task": None,
        }

    if not task_id:
        return {
            "asyncio-enabled": True,
            "task": None,
            "error": "task_id is required",
        }

    return {
        "asyncio-enabled": True,
        "task": executor.task_info(task_id),
    }