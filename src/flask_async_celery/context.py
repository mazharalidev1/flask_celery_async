from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable


class TaskContext:
    """
    Framework context provider used by AsyncTask.

    The provider returns a context manager that surrounds
    execution of an async task.
    """

    def __init__(
        self,
        factory: Callable[[], Any] | None = None,
    ) -> None:
        self.factory = factory

    def __call__(self):
        if self.factory is None:
            return nullcontext()

        return self.factory()