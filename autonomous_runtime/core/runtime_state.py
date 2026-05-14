"""
runtime_state.py — Runtime Phase & State Management (Step 1.2)

Defines the RuntimePhase enum, the legal transition graph, and a thread-safe
RuntimeStateStore that enforces those transitions and records history.

Design principles:
- All transitions are explicit and validated.
- State changes are logged with timestamps.
- No direct mutation from outside — callers use transition().
- Thread-safe for concurrent lifecycle coordination.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase definitions
# ---------------------------------------------------------------------------

class RuntimePhase(str, Enum):
    """
    Runtime lifecycle phases.

    Strict ordering enforced by VALID_TRANSITIONS. No phase may be entered
    from an arbitrary predecessor — the transition graph is the contract.
    """
    UNINITIALIZED = "uninitialized"
    BOOTSTRAPPING = "bootstrapping"
    READY         = "ready"
    RUNNING       = "running"
    PAUSED        = "paused"
    DRAINING      = "draining"    # accepting no new work, finishing existing
    STOPPING      = "stopping"
    STOPPED       = "stopped"
    FAULTED       = "faulted"


# Explicit transition graph. Any unlisted edge is forbidden.
VALID_TRANSITIONS: dict[RuntimePhase, frozenset[RuntimePhase]] = {
    RuntimePhase.UNINITIALIZED: frozenset({RuntimePhase.BOOTSTRAPPING}),
    RuntimePhase.BOOTSTRAPPING: frozenset({RuntimePhase.READY, RuntimePhase.FAULTED}),
    RuntimePhase.READY:         frozenset({RuntimePhase.RUNNING, RuntimePhase.STOPPING, RuntimePhase.FAULTED}),
    RuntimePhase.RUNNING:       frozenset({RuntimePhase.PAUSED, RuntimePhase.DRAINING, RuntimePhase.STOPPING, RuntimePhase.FAULTED}),
    RuntimePhase.PAUSED:        frozenset({RuntimePhase.RUNNING, RuntimePhase.STOPPING, RuntimePhase.FAULTED}),
    RuntimePhase.DRAINING:      frozenset({RuntimePhase.STOPPING, RuntimePhase.FAULTED}),
    RuntimePhase.STOPPING:      frozenset({RuntimePhase.STOPPED}),
    RuntimePhase.STOPPED:       frozenset(),
    RuntimePhase.FAULTED:       frozenset({RuntimePhase.STOPPING}),
}

# Terminal phases — no further transitions permitted.
TERMINAL_PHASES: frozenset[RuntimePhase] = frozenset({
    RuntimePhase.STOPPED,
})

# Phases where new work may be submitted.
ACCEPTING_PHASES: frozenset[RuntimePhase] = frozenset({
    RuntimePhase.READY,
    RuntimePhase.RUNNING,
})


class IllegalTransitionError(RuntimeError):
    """Raised when a requested phase transition violates the transition graph."""

    def __init__(self, from_phase: RuntimePhase, to_phase: RuntimePhase) -> None:
        super().__init__(
            f"Illegal runtime transition: {from_phase.value!r} → {to_phase.value!r}. "
            f"Allowed successors: {[p.value for p in VALID_TRANSITIONS.get(from_phase, frozenset())]}"
        )
        self.from_phase = from_phase
        self.to_phase = to_phase


# ---------------------------------------------------------------------------
# State record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PhaseRecord:
    """Immutable snapshot of a single phase transition."""
    phase: RuntimePhase
    entered_at: datetime
    reason: str = ""


# ---------------------------------------------------------------------------
# Thread-safe state store
# ---------------------------------------------------------------------------

class RuntimeStateStore:
    """
    Thread-safe runtime phase manager.

    Enforces the transition graph, records full history, and notifies
    registered observers on each successful transition.

    Observers receive (from_phase, to_phase, record) synchronously on the
    calling thread — keep them fast and non-blocking.
    """

    def __init__(self, initial_phase: RuntimePhase = RuntimePhase.UNINITIALIZED) -> None:
        self._phase = initial_phase
        self._lock = threading.RLock()
        self._history: list[PhaseRecord] = [
            PhaseRecord(phase=initial_phase, entered_at=_now(), reason="initialized")
        ]
        self._observers: list[Callable[[RuntimePhase, RuntimePhase, PhaseRecord], None]] = []

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def phase(self) -> RuntimePhase:
        """Current runtime phase (thread-safe read)."""
        with self._lock:
            return self._phase

    @property
    def history(self) -> list[PhaseRecord]:
        """Ordered snapshot of all phase transitions (defensive copy)."""
        with self._lock:
            return list(self._history)

    @property
    def is_terminal(self) -> bool:
        with self._lock:
            return self._phase in TERMINAL_PHASES

    @property
    def is_accepting(self) -> bool:
        """True if the runtime is in a state that accepts new work."""
        with self._lock:
            return self._phase in ACCEPTING_PHASES

    # ------------------------------------------------------------------
    # Transition
    # ------------------------------------------------------------------

    def transition(self, target: RuntimePhase, reason: str = "") -> PhaseRecord:
        """
        Attempt a phase transition.

        Args:
            target: The desired next phase.
            reason: Human-readable explanation (logged and stored).

        Returns:
            The PhaseRecord for the new phase.

        Raises:
            IllegalTransitionError: If the transition is not permitted.
        """
        with self._lock:
            current = self._phase
            allowed = VALID_TRANSITIONS.get(current, frozenset())

            if target not in allowed:
                raise IllegalTransitionError(current, target)

            record = PhaseRecord(phase=target, entered_at=_now(), reason=reason)
            self._phase = target
            self._history.append(record)

            logger.info(
                "Runtime phase transition: %s → %s | reason=%r",
                current.value, target.value, reason,
            )

            # Notify observers outside the lock to avoid deadlocks.
            observers = list(self._observers)

        for observer in observers:
            try:
                observer(current, target, record)
            except Exception:
                logger.exception("State observer raised during transition %s → %s", current, target)

        return record

    def transition_if_current(
        self,
        expected: RuntimePhase,
        target: RuntimePhase,
        reason: str = "",
    ) -> PhaseRecord | None:
        """
        Optimistic transition — only executes if current phase equals *expected*.
        Returns None if the guard fails (not an error).
        """
        with self._lock:
            if self._phase != expected:
                return None
        return self.transition(target, reason)

    # ------------------------------------------------------------------
    # Observers
    # ------------------------------------------------------------------

    def add_observer(
        self,
        observer: Callable[[RuntimePhase, RuntimePhase, PhaseRecord], None],
    ) -> None:
        """Register a transition observer. Called synchronously on transition."""
        with self._lock:
            self._observers.append(observer)

    def remove_observer(
        self,
        observer: Callable[[RuntimePhase, RuntimePhase, PhaseRecord], None],
    ) -> None:
        with self._lock:
            self._observers = [o for o in self._observers if o is not observer]

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        with self._lock:
            return {
                "current_phase": self._phase.value,
                "is_terminal": self._phase in TERMINAL_PHASES,
                "is_accepting": self._phase in ACCEPTING_PHASES,
                "transition_count": len(self._history),
                "history": [
                    {"phase": r.phase.value, "at": r.entered_at.isoformat(), "reason": r.reason}
                    for r in self._history
                ],
            }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)
