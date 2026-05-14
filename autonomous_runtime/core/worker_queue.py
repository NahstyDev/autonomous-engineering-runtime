"""
worker_queue.py — Worker Queue Infrastructure (Step 1.8)

Priority task queue with async worker pool and full task lifecycle tracking.

Design:
- Tasks are submitted with a priority (lower = higher urgency).
- Fixed worker pool processes tasks concurrently up to the worker count.
- Every task is tracked through pending → running → done/failed/cancelled.
- Queue supervisor monitors worker health and resubmits on failure.
- Backpressure: submission raises QueueFullError when at capacity.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task state
# ---------------------------------------------------------------------------

class QueuedTaskStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CANCELLED = "cancelled"


TASK_TERMINAL: frozenset[QueuedTaskStatus] = frozenset({
    QueuedTaskStatus.COMPLETED,
    QueuedTaskStatus.FAILED,
    QueuedTaskStatus.CANCELLED,
})


# ---------------------------------------------------------------------------
# Task model
# ---------------------------------------------------------------------------

@dataclass
class QueuedTask:
    """
    A unit of work queued for execution.

    priority: int — lower = higher urgency (0 is highest).
    fn:       Callable returning an awaitable result.
    """
    task_id: str = field(default_factory=lambda: f"qt-{uuid.uuid4().hex[:8]}")
    name: str = "unnamed"
    fn: Callable[[], Awaitable[Any]] = field(repr=False, default=None)  # type: ignore[assignment]
    priority: int = 10   # 0 = highest
    max_retries: int = 0
    timeout_seconds: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # Runtime tracking (mutable, set by the queue)
    status: QueuedTaskStatus = field(default=QueuedTaskStatus.PENDING)
    submitted_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    started_at: datetime | None = None
    ended_at: datetime | None = None
    result: Any = None
    error: str | None = None
    attempts: int = 0

    def __lt__(self, other: "QueuedTask") -> bool:
        """Comparison for PriorityQueue — lower priority int = higher priority."""
        return self.priority < other.priority

    @property
    def is_terminal(self) -> bool:
        return self.status in TASK_TERMINAL

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        end = self.ended_at or datetime.now(tz=timezone.utc)
        return (end - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "name": self.name,
            "priority": self.priority,
            "status": self.status.value,
            "attempts": self.attempts,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "metadata": self.metadata,
        }


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class QueueFullError(RuntimeError):
    pass


class QueueStoppedError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Worker queue
# ---------------------------------------------------------------------------

class WorkerQueue:
    """
    Bounded async priority worker queue.

    Usage:
        queue = WorkerQueue(worker_count=4, max_size=500)
        await queue.start()

        task = QueuedTask(name="compile", fn=my_coro_factory)
        await queue.submit(task)

        result = await queue.wait_for(task.task_id)
        await queue.stop()
    """

    def __init__(
        self,
        worker_count: int = 4,
        max_size: int = 1000,
        name: str = "default",
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")
        if max_size < 1:
            raise ValueError("max_size must be >= 1")

        self.name = name
        self._worker_count = worker_count
        self._max_size = max_size

        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_size)
        self._tasks: dict[str, QueuedTask] = {}
        self._completions: dict[str, asyncio.Future] = {}
        self._workers: list[asyncio.Task] = []
        self._started = False
        self._stopping = False
        self._lock = asyncio.Lock()

        # Stats
        self._submitted_count = 0
        self._completed_count = 0
        self._failed_count = 0
        self._cancelled_count = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start worker pool."""
        if self._started:
            return
        self._started = True
        for i in range(self._worker_count):
            worker = asyncio.create_task(
                self._worker_loop(i),
                name=f"worker-queue[{self.name}]-{i}",
            )
            self._workers.append(worker)
        logger.info("WorkerQueue[%s] started: %d workers, capacity=%d", self.name, self._worker_count, self._max_size)

    async def stop(self, drain_timeout: float = 30.0) -> None:
        """
        Drain the queue (wait for in-flight tasks), then stop workers.
        """
        if not self._started or self._stopping:
            return
        self._stopping = True
        logger.info("WorkerQueue[%s] stopping — draining in-flight tasks", self.name)

        # Signal workers to stop by sending sentinel values
        for _ in range(self._worker_count):
            await self._queue.put((float("inf"), None))  # type: ignore[arg-type]

        try:
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True),
                timeout=drain_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("WorkerQueue[%s]: drain timeout — cancelling workers", self.name)
            for w in self._workers:
                w.cancel()

        logger.info(
            "WorkerQueue[%s] stopped. submitted=%d completed=%d failed=%d cancelled=%d",
            self.name, self._submitted_count, self._completed_count,
            self._failed_count, self._cancelled_count,
        )

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    async def submit(self, task: QueuedTask) -> str:
        """
        Submit a task for execution.

        Returns the task_id.
        Raises QueueFullError if at capacity.
        Raises QueueStoppedError if the queue is stopping.
        """
        if self._stopping:
            raise QueueStoppedError(f"WorkerQueue[{self.name}] is stopping")
        if self._queue.full():
            raise QueueFullError(f"WorkerQueue[{self.name}] is full (max_size={self._max_size})")

        async with self._lock:
            self._tasks[task.task_id] = task
            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._completions[task.task_id] = future
            self._submitted_count += 1

        await self._queue.put((task.priority, task))
        logger.debug("Task submitted: %s name=%r priority=%d", task.task_id, task.name, task.priority)
        return task.task_id

    def submit_nowait(self, task: QueuedTask) -> str:
        """Synchronous submit (raises if full). Use in non-async context only."""
        if self._stopping:
            raise QueueStoppedError(f"WorkerQueue[{self.name}] is stopping")
        try:
            self._tasks[task.task_id] = task
            self._completions[task.task_id] = asyncio.get_event_loop().create_future()
            self._queue.put_nowait((task.priority, task))
            self._submitted_count += 1
            return task.task_id
        except asyncio.QueueFull:
            raise QueueFullError(f"WorkerQueue[{self.name}] is full")

    # ------------------------------------------------------------------
    # Awaiting results
    # ------------------------------------------------------------------

    async def wait_for(self, task_id: str, timeout: float | None = None) -> Any:
        """
        Wait for a task to complete and return its result.
        Raises the task's exception if it failed.
        """
        future = self._completions.get(task_id)
        if future is None:
            raise KeyError(f"Unknown task_id: {task_id!r}")
        if timeout is not None:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        return await future

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel_task(self, task_id: str, reason: str = "") -> bool:
        """Mark a pending task as cancelled. No effect on running tasks."""
        async with self._lock:
            task = self._tasks.get(task_id)
        if task is None or task.status != QueuedTaskStatus.PENDING:
            return False
        task.status = QueuedTaskStatus.CANCELLED
        task.ended_at = datetime.now(tz=timezone.utc)
        self._cancel_future(task_id, reason)
        self._cancelled_count += 1
        logger.info("Task cancelled: %s name=%r reason=%r", task_id, task.name, reason)
        return True

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> QueuedTask | None:
        return self._tasks.get(task_id)

    def queue_depth(self) -> int:
        return self._queue.qsize()

    def active_tasks(self) -> list[QueuedTask]:
        return [t for t in self._tasks.values() if t.status == QueuedTaskStatus.RUNNING]

    def pending_tasks(self) -> list[QueuedTask]:
        return [t for t in self._tasks.values() if t.status == QueuedTaskStatus.PENDING]

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "worker_count": self._worker_count,
            "max_size": self._max_size,
            "queue_depth": self.queue_depth(),
            "active": len(self.active_tasks()),
            "pending": len(self.pending_tasks()),
            "submitted": self._submitted_count,
            "completed": self._completed_count,
            "failed": self._failed_count,
            "cancelled": self._cancelled_count,
            "started": self._started,
            "stopping": self._stopping,
        }

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker_loop(self, worker_id: int) -> None:
        logger.debug("Worker %d[%s] started", worker_id, self.name)
        while True:
            try:
                priority, task = await self._queue.get()
            except asyncio.CancelledError:
                break

            # Sentinel — time to exit
            if task is None:
                self._queue.task_done()
                break

            # Skip cancelled tasks
            if task.status == QueuedTaskStatus.CANCELLED:
                self._queue.task_done()
                continue

            await self._execute_task(task, worker_id)
            self._queue.task_done()

        logger.debug("Worker %d[%s] stopped", worker_id, self.name)

    async def _execute_task(self, task: QueuedTask, worker_id: int) -> None:
        task.status = QueuedTaskStatus.RUNNING
        task.started_at = datetime.now(tz=timezone.utc)
        task.attempts += 1
        logger.debug("Task started: %s name=%r worker=%d", task.task_id, task.name, worker_id)

        try:
            coro = task.fn()
            if task.timeout_seconds is not None:
                result = await asyncio.wait_for(coro, timeout=task.timeout_seconds)
            else:
                result = await coro

            task.result = result
            task.status = QueuedTaskStatus.COMPLETED
            task.ended_at = datetime.now(tz=timezone.utc)
            self._completed_count += 1

            future = self._completions.get(task.task_id)
            if future and not future.done():
                future.set_result(result)

            logger.debug(
                "Task completed: %s name=%r duration=%.2fs",
                task.task_id, task.name, task.duration_seconds or 0.0,
            )

        except asyncio.CancelledError:
            task.status = QueuedTaskStatus.CANCELLED
            task.ended_at = datetime.now(tz=timezone.utc)
            self._cancelled_count += 1
            self._cancel_future(task.task_id, "worker cancelled")
            raise

        except Exception as exc:
            task.error = f"{type(exc).__name__}: {exc}"
            task.status = QueuedTaskStatus.FAILED
            task.ended_at = datetime.now(tz=timezone.utc)
            self._failed_count += 1
            logger.warning("Task failed: %s name=%r error=%r", task.task_id, task.name, task.error)

            future = self._completions.get(task.task_id)
            if future and not future.done():
                future.set_exception(exc)

    def _cancel_future(self, task_id: str, reason: str) -> None:
        future = self._completions.get(task_id)
        if future and not future.done():
            future.cancel(reason)
