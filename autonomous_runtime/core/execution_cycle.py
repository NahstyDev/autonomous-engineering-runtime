"""
execution_cycle.py — Execution Lifecycle State Machine (Step 1.6)

Tracks a single execution unit through its full lifecycle.
Every action taken by the runtime maps to a deterministic transition
in this state machine. The audit trail is the foundation for replay.

States form a DAG — no cycles, no ambiguity.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Execution phases
# ---------------------------------------------------------------------------

class ExecutionPhase(str, Enum):
    """
    Execution lifecycle phases.

    CREATED       → execution registered, not yet started
    PLANNING      → assembling plan / decomposing task
    RETRIEVING    → gathering context from repository / memory
    EXECUTING     → actively running tools / generating output
    VERIFYING     → validating output against constraints
    REPAIRING     → autonomous repair loop
    FINALIZING    → committing results / cleanup
    COMPLETED     → successful terminal state
    FAILED        → unrecoverable failure
    CANCELLED     → externally cancelled
    TIMED_OUT     → exceeded time budget
    """
    CREATED    = "created"
    PLANNING   = "planning"
    RETRIEVING = "retrieving"
    EXECUTING  = "executing"
    VERIFYING  = "verifying"
    REPAIRING  = "repairing"
    FINALIZING = "finalizing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"
    TIMED_OUT  = "timed_out"


EXECUTION_TRANSITIONS: dict[ExecutionPhase, frozenset[ExecutionPhase]] = {
    ExecutionPhase.CREATED:    frozenset({ExecutionPhase.PLANNING, ExecutionPhase.CANCELLED}),
    ExecutionPhase.PLANNING:   frozenset({ExecutionPhase.RETRIEVING, ExecutionPhase.FAILED, ExecutionPhase.CANCELLED}),
    ExecutionPhase.RETRIEVING: frozenset({ExecutionPhase.EXECUTING, ExecutionPhase.FAILED, ExecutionPhase.CANCELLED}),
    ExecutionPhase.EXECUTING:  frozenset({ExecutionPhase.VERIFYING, ExecutionPhase.REPAIRING, ExecutionPhase.FAILED, ExecutionPhase.CANCELLED, ExecutionPhase.TIMED_OUT}),
    ExecutionPhase.VERIFYING:  frozenset({ExecutionPhase.FINALIZING, ExecutionPhase.REPAIRING, ExecutionPhase.FAILED, ExecutionPhase.CANCELLED}),
    ExecutionPhase.REPAIRING:  frozenset({ExecutionPhase.EXECUTING, ExecutionPhase.FAILED, ExecutionPhase.CANCELLED}),
    ExecutionPhase.FINALIZING: frozenset({ExecutionPhase.COMPLETED, ExecutionPhase.FAILED}),
    ExecutionPhase.COMPLETED:  frozenset(),
    ExecutionPhase.FAILED:     frozenset(),
    ExecutionPhase.CANCELLED:  frozenset(),
    ExecutionPhase.TIMED_OUT:  frozenset(),
}

TERMINAL_EXECUTION_PHASES: frozenset[ExecutionPhase] = frozenset({
    ExecutionPhase.COMPLETED,
    ExecutionPhase.FAILED,
    ExecutionPhase.CANCELLED,
    ExecutionPhase.TIMED_OUT,
})


class ExecutionTransitionError(RuntimeError):
    def __init__(self, from_phase: ExecutionPhase, to_phase: ExecutionPhase) -> None:
        allowed = [p.value for p in EXECUTION_TRANSITIONS.get(from_phase, frozenset())]
        super().__init__(
            f"Invalid execution transition: {from_phase.value!r} → {to_phase.value!r}. "
            f"Allowed: {allowed}"
        )


# ---------------------------------------------------------------------------
# Audit entry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExecutionAuditEntry:
    """Immutable record of a single phase transition. Foundation for replay."""
    sequence: int
    from_phase: ExecutionPhase
    to_phase: ExecutionPhase
    timestamp: datetime
    reason: str
    metadata: dict[str, Any]


# ---------------------------------------------------------------------------
# Execution cycle
# ---------------------------------------------------------------------------

@dataclass
class ExecutionCycle:
    """
    Per-execution state machine.

    Each execution (workflow run, task execution, repair cycle) gets its
    own ExecutionCycle instance. The audit trail is append-only and provides
    full replay fidelity in Phase 2.

    Usage:
        cycle = ExecutionCycle(name="implement-feature-x")
        cycle.transition(ExecutionPhase.PLANNING, reason="plan started")
        cycle.transition(ExecutionPhase.RETRIEVING)
        # ...
        cycle.transition(ExecutionPhase.COMPLETED)
    """
    execution_id: str = field(default_factory=lambda: f"exec-{uuid.uuid4().hex}")
    name: str = "unnamed"
    session_id: str | None = None
    workflow_id: str | None = None

    phase: ExecutionPhase = field(default=ExecutionPhase.CREATED)
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    started_at: datetime | None = None
    ended_at: datetime | None = None

    audit_trail: list[ExecutionAuditEntry] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: str | None = None

    # Repair tracking
    repair_attempts: int = 0
    max_repair_attempts: int = 3

    # Observers
    _observers: list[Callable[["ExecutionCycle", ExecutionPhase, ExecutionPhase], None]] = field(
        default_factory=list, repr=False
    )

    # ------------------------------------------------------------------
    # Transitions
    # ------------------------------------------------------------------

    def transition(
        self,
        target: ExecutionPhase,
        reason: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "ExecutionCycle":
        """
        Execute a phase transition with full audit recording.

        Returns self for fluent chaining.
        Raises ExecutionTransitionError on illegal transition.
        """
        allowed = EXECUTION_TRANSITIONS.get(self.phase, frozenset())
        if target not in allowed:
            raise ExecutionTransitionError(self.phase, target)

        from_phase = self.phase
        now = datetime.now(tz=timezone.utc)

        # Record before changing state
        entry = ExecutionAuditEntry(
            sequence=len(self.audit_trail),
            from_phase=from_phase,
            to_phase=target,
            timestamp=now,
            reason=reason,
            metadata=metadata or {},
        )
        self.audit_trail.append(entry)
        self.phase = target

        # Timestamp bookkeeping
        if from_phase == ExecutionPhase.CREATED and target == ExecutionPhase.PLANNING:
            self.started_at = now
        if target in TERMINAL_EXECUTION_PHASES:
            self.ended_at = now
        if target == ExecutionPhase.REPAIRING:
            self.repair_attempts += 1

        logger.info(
            "Execution %s: %s → %s | reason=%r repairs=%d",
            self.execution_id, from_phase.value, target.value, reason, self.repair_attempts,
        )

        # Notify observers
        for observer in self._observers:
            try:
                observer(self, from_phase, target)
            except Exception:
                logger.exception("Execution observer raised during transition")

        return self

    # ------------------------------------------------------------------
    # Guard helpers
    # ------------------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        return self.phase in TERMINAL_EXECUTION_PHASES

    @property
    def is_successful(self) -> bool:
        return self.phase == ExecutionPhase.COMPLETED

    @property
    def can_repair(self) -> bool:
        return self.repair_attempts < self.max_repair_attempts

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        end = self.ended_at or datetime.now(tz=timezone.utc)
        return (end - self.started_at).total_seconds()

    # ------------------------------------------------------------------
    # Terminal helpers (fluent)
    # ------------------------------------------------------------------

    def complete(self, result: Any = None) -> "ExecutionCycle":
        self.result = result
        return self.transition(ExecutionPhase.COMPLETED, reason="execution completed")

    def fail(self, error: str, reason: str = "") -> "ExecutionCycle":
        self.error = error
        return self.transition(ExecutionPhase.FAILED, reason=reason or error)

    def cancel(self, reason: str = "cancelled") -> "ExecutionCycle":
        return self.transition(ExecutionPhase.CANCELLED, reason=reason)

    def timeout(self) -> "ExecutionCycle":
        return self.transition(ExecutionPhase.TIMED_OUT, reason="execution timed out")

    # ------------------------------------------------------------------
    # Observers
    # ------------------------------------------------------------------

    def add_observer(
        self,
        observer: Callable[["ExecutionCycle", ExecutionPhase, ExecutionPhase], None],
    ) -> None:
        self._observers.append(observer)

    # ------------------------------------------------------------------
    # Serialization (Phase 2 persistence hook)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "name": self.name,
            "session_id": self.session_id,
            "workflow_id": self.workflow_id,
            "phase": self.phase.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "repair_attempts": self.repair_attempts,
            "max_repair_attempts": self.max_repair_attempts,
            "result": self.result,
            "error": self.error,
            "metadata": self.metadata,
            "audit_trail": [
                {
                    "seq": e.sequence,
                    "from": e.from_phase.value,
                    "to": e.to_phase.value,
                    "at": e.timestamp.isoformat(),
                    "reason": e.reason,
                }
                for e in self.audit_trail
            ],
        }

    def __repr__(self) -> str:
        return (
            f"ExecutionCycle(id={self.execution_id!r}, "
            f"name={self.name!r}, phase={self.phase.value!r})"
        )
