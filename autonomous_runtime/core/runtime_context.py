"""
runtime_context.py — Runtime Context (Step 1.2)

RuntimeContext is the shared, immutable envelope passed to every subsystem.
It carries the config, the state store, and the service registry.

Design principles:
- Created once at bootstrap, never re-created.
- Subsystems receive context via constructor injection.
- No mutable global state — the context IS the shared state boundary.
- Frozen at the top level; mutable state lives inside StateStore / Registry.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .config import RuntimeConfig
from .runtime_state import RuntimeStateStore, RuntimePhase

if TYPE_CHECKING:
    from .service_registry import ServiceRegistry

logger = logging.getLogger(__name__)


@dataclass
class RuntimeContext:
    """
    Shared runtime context — the single authoritative dependency envelope.

    All runtime subsystems are constructed with a reference to this object.
    It provides:
      - Immutable config
      - Mutable (but thread-safe) state store
      - Service registry for late-bound dependency resolution
      - Runtime metadata (id, started_at)

    RuntimeContext itself is not frozen because the registry is populated
    incrementally during bootstrap — but config and state_store are never
    replaced after construction.
    """

    config: RuntimeConfig
    state: RuntimeStateStore
    registry: "ServiceRegistry"
    context_id: str = field(default_factory=lambda: f"ctx-{uuid.uuid4().hex[:8]}")
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def runtime_id(self) -> str:
        return self.config.runtime_id

    @property
    def runtime_name(self) -> str:
        return self.config.runtime_name

    @property
    def environment(self) -> str:
        return self.config.environment

    @property
    def current_phase(self) -> RuntimePhase:
        return self.state.phase

    @property
    def is_running(self) -> bool:
        return self.state.phase == RuntimePhase.RUNNING

    @property
    def is_accepting_work(self) -> bool:
        return self.state.is_accepting

    @property
    def is_shutting_down(self) -> bool:
        return self.state.phase in (
            RuntimePhase.DRAINING,
            RuntimePhase.STOPPING,
            RuntimePhase.STOPPED,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def require_phase(self, *phases: RuntimePhase) -> None:
        """
        Assert that the runtime is in one of the given phases.
        Raises RuntimeError if not — use at operation entry points.
        """
        current = self.state.phase
        if current not in phases:
            allowed = ", ".join(p.value for p in phases)
            raise RuntimeError(
                f"Operation requires phase in [{allowed}], "
                f"but runtime is in {current.value!r}."
            )

    def summary(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "runtime_name": self.runtime_name,
            "environment": self.environment,
            "context_id": self.context_id,
            "created_at": self.created_at.isoformat(),
            "phase": self.current_phase.value,
            "metadata": self.metadata,
        }

    def __repr__(self) -> str:
        return (
            f"RuntimeContext("
            f"runtime_id={self.runtime_id!r}, "
            f"phase={self.current_phase.value!r}, "
            f"env={self.environment!r})"
        )
