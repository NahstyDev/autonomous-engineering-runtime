"""
concurrency_manager.py — Concurrency Infrastructure (Step 1.5)

Provides:
  - Bounded semaphore for concurrency control.
  - Named task tracking with cancellation support.
  - Cooperative cancellation via CancellationToken.
  - Task group management for coordinated shutdown.
  - Runtime-safe async primitives.

Design principles:
- All tasks are tracked — nothing runs unaccounted.
- Cancellation is cooperative and explicit, never forceful.
- Concurrency limits are enforced by the semaphore, not by convention.
- Shutdown drains tracked tasks before proceeding.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Coroutine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cancellation token
# ---------------------------------------------------------------------------

class CancellationToken:
    """
    Cooperative cancellation signal.

    Tasks should periodically check token.is_cancelled and exit cleanly.
    Cancellation is a request, not a forceful termination.
    """

    def __init__(self) -> None:
        self._cancelled = False
        self._event = asyncio.Event()

    def cancel(self, reason: str = "") -> None:
        if not self._cancelled:
            self._cancelled = True
            self._event.set()
            logger.debug("CancellationToken: cancelled reason=%r", reason)

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    async def wait(self) -> None:
        """Await until cancelled."""
        await self._event.wait()

    def raise_if_cancelled(self) -> None:
        """Raise asyncio.CancelledError if the token is cancelled."""
        if self._cancelled:
            raise asyncio.CancelledError("CancellationToken: cancelled")


# ---------------------------------------------------------------------------
# Tracked task
# ---------------------------------------------------------------------------

@dataclass
class TrackedTask:
    task_id: str
    name: str
    task: asyncio.Task
    token: CancellationToken
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_done(self) -> bool:
        return self.task.done()

    @property
    def is_cancelled(self) -> bool:
        return self.task.cancelled()

    @property
    def failed(self) -> bool:
        return self.task.done() and not self.task.cancelled() and self.task.exception() is not None

    def cancel(self, reason: str = "") -> None:
        self.token.cancel(reason)
        self.task.cancel(f"TrackedTask cancelled: {reason}")


# ---------------------------------------------------------------------------
# Concurrency manager
# ---------------------------------------------------------------------------

class ConcurrencyManager:
    """
    Manages bounded async concurrency with full task lifecycle tracking.

    Usage:
        mgr = ConcurrencyManager(max_concurrent=8)
        await mgr.start()

        async with mgr.acquire():
            ...  # runs within concurrency budget

        task = await mgr.spawn("my-task", my_coro())
        await mgr.cancel_task(task.task_id)
        await mgr.drain()
        await mgr.stop()
    """

    def __init__(self, max_concurrent: int = 16, name: str = "default") -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self.name = name
        self._max_concurrent = max_concurrent
        self._semaphore: asyncio.Semaphore | None = None
        self._tasks: dict[str, TrackedTask] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._stopped = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._started = True
        logger.info("ConcurrencyManager[%s] started (max=%d)", self.name, self._max_concurrent)

    async def stop(self) -> None:
        """Cancel all tracked tasks and mark as stopped."""
        if self._stopped:
            return
        self._stopped = True
        await self.cancel_all("ConcurrencyManager stopping")
        logger.info("ConcurrencyManager[%s] stopped", self.name)

    # ------------------------------------------------------------------
    # Task spawning
    # ------------------------------------------------------------------

    async def spawn(
        self,
        name: str,
        coro: Coroutine,
        metadata: dict[str, Any] | None = None,
    ) -> TrackedTask:
        """
        Spawn a tracked async task.

        The task runs independently. Use drain() to await completion.
        Raises RuntimeError if the manager is stopped.
        """
        self._assert_running()
        token = CancellationToken()
        task_id = f"task-{uuid.uuid4().hex[:8]}"

        loop_task = asyncio.create_task(
            self._guarded(coro, token, task_id, name),
            name=f"{self.name}/{name}",
        )

        tracked = TrackedTask(
            task_id=task_id,
            name=name,
            task=loop_task,
            token=token,
            metadata=metadata or {},
        )

        async with self._lock:
            self._tasks[task_id] = tracked

        loop_task.add_done_callback(lambda _: self._on_task_done(task_id))
        logger.debug("Task spawned: %s name=%r", task_id, name)
        return tracked

    async def run_bounded(
        self,
        coro: Coroutine,
        timeout: float | None = None,
    ) -> Any:
        """
        Run a coroutine within the concurrency semaphore.
        Blocks until a slot is available, then runs with optional timeout.
        """
        self._assert_running()
        assert self._semaphore is not None

        async with self._semaphore:
            if timeout is not None:
                return await asyncio.wait_for(coro, timeout=timeout)
            return await coro

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel_task(self, task_id: str, reason: str = "") -> bool:
        """Cancel a specific task by ID. Returns True if found and cancelled."""
        async with self._lock:
            tracked = self._tasks.get(task_id)
        if tracked is None:
            return False
        tracked.cancel(reason)
        return True

    async def cancel_all(self, reason: str = "") -> None:
        """Cancel all tracked tasks."""
        async with self._lock:
            tasks = list(self._tasks.values())
        for tracked in tasks:
            if not tracked.is_done:
                tracked.cancel(reason)
        logger.info("ConcurrencyManager[%s]: cancelled %d tasks", self.name, len(tasks))

    # ------------------------------------------------------------------
    # Draining / awaiting
    # ------------------------------------------------------------------

    async def drain(self, timeout: float | None = None) -> None:
        """
        Wait for all tracked tasks to complete.
        If timeout is given, raises asyncio.TimeoutError if exceeded.
        """
        async with self._lock:
            pending = [t.task for t in self._tasks.values() if not t.is_done]

        if not pending:
            return

        logger.info("ConcurrencyManager[%s]: draining %d tasks", self.name, len(pending))
        if timeout is not None:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=timeout)
        else:
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.is_done)

    @property
    def max_concurrent(self) -> int:
        return self._max_concurrent

    def get_task(self, task_id: str) -> TrackedTask | None:
        return self._tasks.get(task_id)

    def all_tasks(self) -> list[TrackedTask]:
        return list(self._tasks.values())

    def summary(self) -> dict[str, Any]:
        tasks = list(self._tasks.values())
        return {
            "name": self.name,
            "max_concurrent": self._max_concurrent,
            "total_tasks": len(tasks),
            "active": sum(1 for t in tasks if not t.is_done),
            "completed": sum(1 for t in tasks if t.is_done and not t.failed and not t.is_cancelled),
            "failed": sum(1 for t in tasks if t.failed),
            "cancelled": sum(1 for t in tasks if t.is_cancelled),
            "started": self._started,
            "stopped": self._stopped,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _guarded(
        self,
        coro: Coroutine,
        token: CancellationToken,
        task_id: str,
        name: str,
    ) -> Any:
        """Wrapper that acquires the semaphore and handles exceptions."""
        assert self._semaphore is not None
        async with self._semaphore:
            try:
                return await coro
            except asyncio.CancelledError:
                logger.debug("Task cancelled: %s name=%r", task_id, name)
                raise
            except Exception:
                logger.exception("Task failed: %s name=%r", task_id, name)
                raise

    def _on_task_done(self, task_id: str) -> None:
        task = self._tasks.get(task_id)
        if task and task.failed:
            exc = task.task.exception()
            logger.warning("Task %s (%r) ended with exception: %s", task_id, task.name, exc)

    def _assert_running(self) -> None:
        if not self._started:
            raise RuntimeError("ConcurrencyManager not started — call start() first")
        if self._stopped:
            raise RuntimeError("ConcurrencyManager is stopped — cannot spawn new tasks")
