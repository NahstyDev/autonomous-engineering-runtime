"""
workflow_scheduler.py — Workflow Scheduling (Step 1.9)

Manages the lifecycle of workflows from submission through completion.

Responsibilities:
  - Accept workflow submissions from the orchestration layer.
  - Assign execution priority and scheduling metadata.
  - Dispatch workflows to the WorkerQueue.
  - Track workflow status and provide query interfaces.
  - Emit workflow lifecycle events to the EventBus.
  - Support workflow cancellation and timeout enforcement.

Phase 2 will add persistence of workflow state for replay/resume.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable

from .event_bus import EventBus, WorkflowScheduledEvent, RuntimeEvent
from .worker_queue import WorkerQueue, QueuedTask, QueueFullError, QueueStoppedError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Workflow status
# ---------------------------------------------------------------------------

class WorkflowStatus(str, Enum):
    PENDING    = "pending"
    QUEUED     = "queued"
    RUNNING    = "running"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"
    TIMED_OUT  = "timed_out"


WORKFLOW_TERMINAL: frozenset[WorkflowStatus] = frozenset({
    WorkflowStatus.COMPLETED,
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
    WorkflowStatus.TIMED_OUT,
})


# ---------------------------------------------------------------------------
# Workflow definition
# ---------------------------------------------------------------------------

@dataclass
class WorkflowDefinition:
    """
    Descriptor for a schedulable unit of work.

    fn:               Async callable accepting no arguments.
    name:             Human-readable identifier.
    workflow_id:      Globally unique ID (auto-generated if not provided).
    priority:         Scheduling priority (0 = highest). Default: 10.
    timeout_seconds:  Overall workflow timeout. None = no limit.
    max_retries:      Retry attempts on failure.
    tags:             Optional classification tags.
    metadata:         Arbitrary key-value payload passed through.
    """
    fn: Callable[[], Awaitable[Any]]
    name: str = "unnamed-workflow"
    workflow_id: str = field(default_factory=lambda: f"wf-{uuid.uuid4().hex[:10]}")
    priority: int = 10
    timeout_seconds: float | None = None
    max_retries: int = 0
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Workflow record
# ---------------------------------------------------------------------------

@dataclass
class WorkflowRecord:
    """Runtime tracking record for a submitted workflow."""
    definition: WorkflowDefinition
    status: WorkflowStatus = WorkflowStatus.PENDING
    queued_task_id: str | None = None
    submitted_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    started_at: datetime | None = None
    ended_at: datetime | None = None
    result: Any = None
    error: str | None = None
    attempt: int = 0
    events: list[str] = field(default_factory=list)

    @property
    def workflow_id(self) -> str:
        return self.definition.workflow_id

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def is_terminal(self) -> bool:
        return self.status in WORKFLOW_TERMINAL

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        end = self.ended_at or datetime.now(tz=timezone.utc)
        return (end - self.started_at).total_seconds()

    def _record_event(self, event: str) -> None:
        self.events.append(f"{datetime.now(tz=timezone.utc).isoformat()} {event}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "status": self.status.value,
            "priority": self.definition.priority,
            "attempt": self.attempt,
            "max_retries": self.definition.max_retries,
            "submitted_at": self.submitted_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "tags": self.definition.tags,
            "metadata": self.definition.metadata,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Workflow scheduler events
# ---------------------------------------------------------------------------

@dataclass
class WorkflowCompletedEvent(RuntimeEvent):
    workflow_id: str = ""
    workflow_name: str = ""
    duration_seconds: float = 0.0
    source: str = "workflow_scheduler"


@dataclass
class WorkflowFailedEvent(RuntimeEvent):
    workflow_id: str = ""
    workflow_name: str = ""
    error: str = ""
    attempt: int = 0
    source: str = "workflow_scheduler"


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class WorkflowScheduler:
    """
    Workflow submission, scheduling, and lifecycle coordinator.

    Usage:
        scheduler = WorkflowScheduler(worker_queue, event_bus)
        await scheduler.start()

        record = await scheduler.schedule(WorkflowDefinition(fn=my_coro, name="task-1"))
        result = await scheduler.wait_for(record.workflow_id)

        await scheduler.stop()
    """

    def __init__(
        self,
        worker_queue: WorkerQueue,
        event_bus: EventBus,
    ) -> None:
        self._queue = worker_queue
        self._bus = event_bus
        self._records: dict[str, WorkflowRecord] = {}
        self._futures: dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._stopping = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._started = True
        logger.info("WorkflowScheduler started")

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        # Cancel pending workflows
        async with self._lock:
            pending = [
                r for r in self._records.values()
                if r.status in (WorkflowStatus.PENDING, WorkflowStatus.QUEUED)
            ]
        for record in pending:
            await self.cancel(record.workflow_id, reason="scheduler stopping")
        logger.info("WorkflowScheduler stopped")

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    async def schedule(self, definition: WorkflowDefinition) -> WorkflowRecord:
        """
        Submit a workflow for scheduling.

        Returns a WorkflowRecord tracking the submission.
        Raises QueueFullError or QueueStoppedError if the queue rejects it.
        """
        if self._stopping:
            raise QueueStoppedError("WorkflowScheduler is stopping")

        record = WorkflowRecord(definition=definition)
        record._record_event("submitted")

        async with self._lock:
            self._records[record.workflow_id] = record
            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._futures[record.workflow_id] = future

        # Wrap fn to update record state
        task = QueuedTask(
            name=definition.name,
            fn=self._wrap_fn(record),
            priority=definition.priority,
            timeout_seconds=definition.timeout_seconds,
            max_retries=definition.max_retries,
            metadata={"workflow_id": definition.workflow_id, **definition.metadata},
        )

        record.queued_task_id = task.task_id

        try:
            await self._queue.submit(task)
            record.status = WorkflowStatus.QUEUED
            record._record_event("queued")
        except (QueueFullError, QueueStoppedError):
            record.status = WorkflowStatus.FAILED
            record.error = "Queue rejected submission"
            record.ended_at = datetime.now(tz=timezone.utc)
            raise

        await self._bus.publish_nowait(WorkflowScheduledEvent(
            workflow_id=definition.workflow_id,
            workflow_name=definition.name,
        ))

        logger.info(
            "Workflow scheduled: %s name=%r priority=%d",
            record.workflow_id, record.name, definition.priority,
        )
        return record

    # ------------------------------------------------------------------
    # Awaiting results
    # ------------------------------------------------------------------

    async def wait_for(self, workflow_id: str, timeout: float | None = None) -> Any:
        """Wait for a workflow to complete. Returns result or raises on failure."""
        future = self._futures.get(workflow_id)
        if future is None:
            raise KeyError(f"Unknown workflow: {workflow_id!r}")
        if timeout is not None:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        return await future

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel(self, workflow_id: str, reason: str = "") -> bool:
        async with self._lock:
            record = self._records.get(workflow_id)
        if record is None or record.is_terminal:
            return False

        if record.queued_task_id:
            await self._queue.cancel_task(record.queued_task_id, reason)

        record.status = WorkflowStatus.CANCELLED
        record.ended_at = datetime.now(tz=timezone.utc)
        record._record_event(f"cancelled: {reason}")

        future = self._futures.get(workflow_id)
        if future and not future.done():
            future.cancel(reason)

        logger.info("Workflow cancelled: %s reason=%r", workflow_id, reason)
        return True

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_record(self, workflow_id: str) -> WorkflowRecord | None:
        return self._records.get(workflow_id)

    def active_workflows(self) -> list[WorkflowRecord]:
        return [r for r in self._records.values() if r.status == WorkflowStatus.RUNNING]

    def pending_workflows(self) -> list[WorkflowRecord]:
        return [r for r in self._records.values() if r.status in (WorkflowStatus.PENDING, WorkflowStatus.QUEUED)]

    def all_workflows(self) -> list[WorkflowRecord]:
        return list(self._records.values())

    def summary(self) -> dict[str, Any]:
        records = list(self._records.values())
        return {
            "total": len(records),
            "pending": sum(1 for r in records if r.status in (WorkflowStatus.PENDING, WorkflowStatus.QUEUED)),
            "running": sum(1 for r in records if r.status == WorkflowStatus.RUNNING),
            "completed": sum(1 for r in records if r.status == WorkflowStatus.COMPLETED),
            "failed": sum(1 for r in records if r.status == WorkflowStatus.FAILED),
            "cancelled": sum(1 for r in records if r.status == WorkflowStatus.CANCELLED),
            "started": self._started,
            "stopping": self._stopping,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _wrap_fn(self, record: WorkflowRecord) -> Callable[[], Awaitable[Any]]:
        """Wrap the workflow fn to update record state and resolve futures."""
        async def _execute() -> Any:
            record.status = WorkflowStatus.RUNNING
            record.started_at = datetime.now(tz=timezone.utc)
            record.attempt += 1
            record._record_event(f"started (attempt {record.attempt})")

            try:
                result = await record.definition.fn()
                record.result = result
                record.status = WorkflowStatus.COMPLETED
                record.ended_at = datetime.now(tz=timezone.utc)
                record._record_event("completed")

                future = self._futures.get(record.workflow_id)
                if future and not future.done():
                    future.set_result(result)

                await self._bus.publish_nowait(WorkflowCompletedEvent(
                    workflow_id=record.workflow_id,
                    workflow_name=record.name,
                    duration_seconds=record.duration_seconds or 0.0,
                ))

                logger.info(
                    "Workflow completed: %s name=%r duration=%.2fs",
                    record.workflow_id, record.name, record.duration_seconds or 0.0,
                )
                return result

            except Exception as exc:
                record.error = f"{type(exc).__name__}: {exc}"
                record.status = WorkflowStatus.FAILED
                record.ended_at = datetime.now(tz=timezone.utc)
                record._record_event(f"failed: {record.error}")

                future = self._futures.get(record.workflow_id)
                if future and not future.done():
                    future.set_exception(exc)

                await self._bus.publish_nowait(WorkflowFailedEvent(
                    workflow_id=record.workflow_id,
                    workflow_name=record.name,
                    error=record.error,
                    attempt=record.attempt,
                ))

                logger.warning(
                    "Workflow failed: %s name=%r attempt=%d error=%r",
                    record.workflow_id, record.name, record.attempt, record.error,
                )
                raise

        return _execute
