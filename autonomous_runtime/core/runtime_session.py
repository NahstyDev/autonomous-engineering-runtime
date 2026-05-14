"""
runtime_session.py — Runtime Session Management (Step 1.2)

A RuntimeSession represents a single bounded execution unit within the runtime.
Sessions are:
  - Created when work begins
  - Tracked through their lifecycle
  - Persisted for replay and audit (Phase 2)
  - Scoped to a single workflow execution or interaction

Sessions are distinct from the RuntimeContext (runtime-wide) —
they exist inside a running context and are created/destroyed repeatedly.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

class SessionStatus(str, Enum):
    PENDING    = "pending"
    ACTIVE     = "active"
    SUSPENDED  = "suspended"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"


SESSION_TERMINAL_STATUSES: frozenset[SessionStatus] = frozenset({
    SessionStatus.COMPLETED,
    SessionStatus.FAILED,
    SessionStatus.CANCELLED,
})

SESSION_VALID_TRANSITIONS: dict[SessionStatus, frozenset[SessionStatus]] = {
    SessionStatus.PENDING:    frozenset({SessionStatus.ACTIVE, SessionStatus.CANCELLED}),
    SessionStatus.ACTIVE:     frozenset({SessionStatus.SUSPENDED, SessionStatus.COMPLETED, SessionStatus.FAILED, SessionStatus.CANCELLED}),
    SessionStatus.SUSPENDED:  frozenset({SessionStatus.ACTIVE, SessionStatus.CANCELLED}),
    SessionStatus.COMPLETED:  frozenset(),
    SessionStatus.FAILED:     frozenset(),
    SessionStatus.CANCELLED:  frozenset(),
}


class SessionTransitionError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Session model
# ---------------------------------------------------------------------------

@dataclass
class RuntimeSession:
    """
    A bounded execution session within the runtime.

    Tracks timing, status, structured metadata, and a flat key-value
    store for session-scoped context. The session_id is the durable
    identity used for persistence, replay, and audit in Phase 2.
    """

    session_id: str = field(default_factory=lambda: f"sess-{uuid.uuid4().hex}")
    parent_session_id: str | None = None  # for nested/delegated sessions
    name: str = "unnamed-session"
    tags: list[str] = field(default_factory=list)

    status: SessionStatus = field(default=SessionStatus.PENDING)
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    started_at: datetime | None = None
    ended_at: datetime | None = None
    suspended_at: datetime | None = None

    # Session-scoped key-value context (e.g. workflow_id, task_type)
    context: dict[str, Any] = field(default_factory=dict)
    # Structured event log — lightweight, not a replacement for the event bus
    events: list[dict[str, Any]] = field(default_factory=list)
    # Final result or error summary
    result: Any = None
    error: str | None = None

    # ------------------------------------------------------------------
    # Lifecycle transitions
    # ------------------------------------------------------------------

    def activate(self) -> None:
        """Mark the session as active (work has started)."""
        self._transition(SessionStatus.ACTIVE)
        self.started_at = _now()
        self._log_event("session_activated")
        logger.info("Session activated: %s", self.session_id)

    def suspend(self, reason: str = "") -> None:
        """Suspend the session (paused, awaiting input or resource)."""
        self._transition(SessionStatus.SUSPENDED)
        self.suspended_at = _now()
        self._log_event("session_suspended", reason=reason)
        logger.info("Session suspended: %s reason=%r", self.session_id, reason)

    def resume(self) -> None:
        """Resume a suspended session."""
        self._transition(SessionStatus.ACTIVE)
        self.suspended_at = None
        self._log_event("session_resumed")
        logger.info("Session resumed: %s", self.session_id)

    def complete(self, result: Any = None) -> None:
        """Mark the session as successfully completed."""
        self._transition(SessionStatus.COMPLETED)
        self.result = result
        self.ended_at = _now()
        self._log_event("session_completed")
        logger.info("Session completed: %s", self.session_id)

    def fail(self, error: str) -> None:
        """Mark the session as failed with an error description."""
        self._transition(SessionStatus.FAILED)
        self.error = error
        self.ended_at = _now()
        self._log_event("session_failed", error=error)
        logger.warning("Session failed: %s error=%r", self.session_id, error)

    def cancel(self, reason: str = "") -> None:
        """Cancel the session (external or operator request)."""
        self._transition(SessionStatus.CANCELLED)
        self.ended_at = _now()
        self._log_event("session_cancelled", reason=reason)
        logger.info("Session cancelled: %s reason=%r", self.session_id, reason)

    # ------------------------------------------------------------------
    # Context helpers
    # ------------------------------------------------------------------

    def set_context(self, key: str, value: Any) -> None:
        self.context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        return self.context.get(key, default)

    @property
    def is_active(self) -> bool:
        return self.status == SessionStatus.ACTIVE

    @property
    def is_terminal(self) -> bool:
        return self.status in SESSION_TERMINAL_STATUSES

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at is None:
            return None
        end = self.ended_at or _now()
        return (end - self.started_at).total_seconds()

    # ------------------------------------------------------------------
    # Serialization (Phase 2 persistence hook)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "parent_session_id": self.parent_session_id,
            "name": self.name,
            "tags": self.tags,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "context": self.context,
            "events": self.events,
            "result": self.result,
            "error": self.error,
            "duration_seconds": self.duration_seconds,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _transition(self, target: SessionStatus) -> None:
        allowed = SESSION_VALID_TRANSITIONS.get(self.status, frozenset())
        if target not in allowed:
            raise SessionTransitionError(
                f"Session {self.session_id}: illegal transition "
                f"{self.status.value!r} → {target.value!r}"
            )
        self.status = target

    def _log_event(self, event_type: str, **kwargs: Any) -> None:
        self.events.append({
            "type": event_type,
            "at": _now().isoformat(),
            **kwargs,
        })

    def __repr__(self) -> str:
        return (
            f"RuntimeSession(id={self.session_id!r}, "
            f"name={self.name!r}, status={self.status.value!r})"
        )


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

class SessionFactory:
    """Creates and tracks RuntimeSession instances."""

    def __init__(self) -> None:
        self._sessions: dict[str, RuntimeSession] = {}

    def create(
        self,
        name: str = "unnamed-session",
        parent_session_id: str | None = None,
        tags: list[str] | None = None,
        context: dict[str, Any] | None = None,
    ) -> RuntimeSession:
        sess = RuntimeSession(
            name=name,
            parent_session_id=parent_session_id,
            tags=tags or [],
            context=context or {},
        )
        self._sessions[sess.session_id] = sess
        logger.debug("Session created: %s name=%r", sess.session_id, name)
        return sess

    def get(self, session_id: str) -> RuntimeSession | None:
        return self._sessions.get(session_id)

    def active_sessions(self) -> list[RuntimeSession]:
        return [s for s in self._sessions.values() if s.is_active]

    def all_sessions(self) -> list[RuntimeSession]:
        return list(self._sessions.values())

    def purge_terminal(self) -> int:
        """Remove terminal sessions from memory. Returns count removed."""
        terminal = [sid for sid, s in self._sessions.items() if s.is_terminal]
        for sid in terminal:
            del self._sessions[sid]
        return len(terminal)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)
