"""
lifecycle_supervisor.py — Lifecycle Supervisor (Step 1.3)
shutdown_manager.py    — Shutdown Manager (Step 1.3)

LifecycleSupervisor: monitors runtime health, enforces phase transitions,
and publishes phase-change events.

ShutdownManager: coordinates ordered graceful shutdown across all registered
shutdown hooks, with timeout enforcement.
"""
from __future__ import annotations

import asyncio
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .runtime_state import RuntimeStateStore, RuntimePhase, IllegalTransitionError
from .event_bus import EventBus, RuntimePhaseChangedEvent, ShutdownRequestedEvent

logger = logging.getLogger(__name__)


# ===========================================================================
# Lifecycle Supervisor
# ===========================================================================

class LifecycleSupervisor:
    """
    Monitors runtime phase transitions and emits phase-change events.

    Responsibilities:
      - Validate and execute runtime phase transitions.
      - Emit RuntimePhaseChangedEvent on each transition.
      - Provide convenience wrappers for common transitions.
      - Optionally run a health-check loop (Phase 4).

    The supervisor is the single authority for phase changes — no other
    component should call state.transition() directly.
    """

    def __init__(
        self,
        state: RuntimeStateStore,
        event_bus: EventBus,
    ) -> None:
        self._state = state
        self._bus = event_bus
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._started = True
        logger.info("LifecycleSupervisor started. current_phase=%s", self._state.phase.value)

    async def stop(self) -> None:
        self._started = False
        logger.info("LifecycleSupervisor stopped")

    # ------------------------------------------------------------------
    # Phase transitions (supervised)
    # ------------------------------------------------------------------

    async def transition(self, target: RuntimePhase, reason: str = "") -> bool:
        """
        Attempt a supervised phase transition.

        Returns True on success, False if the transition was illegal.
        Emits RuntimePhaseChangedEvent on success.
        """
        from_phase = self._state.phase
        try:
            record = self._state.transition(target, reason=reason)
        except IllegalTransitionError as e:
            logger.warning("LifecycleSupervisor: rejected transition: %s", e)
            return False

        await self._bus.publish_nowait(RuntimePhaseChangedEvent(
            from_phase=from_phase.value,
            to_phase=target.value,
            reason=reason,
        ))
        return True

    async def mark_bootstrapping(self, reason: str = "bootstrap started") -> bool:
        return await self.transition(RuntimePhase.BOOTSTRAPPING, reason)

    async def mark_ready(self, reason: str = "bootstrap complete") -> bool:
        return await self.transition(RuntimePhase.READY, reason)

    async def mark_running(self, reason: str = "runtime started") -> bool:
        return await self.transition(RuntimePhase.RUNNING, reason)

    async def mark_paused(self, reason: str = "paused") -> bool:
        return await self.transition(RuntimePhase.PAUSED, reason)

    async def mark_draining(self, reason: str = "draining") -> bool:
        return await self.transition(RuntimePhase.DRAINING, reason)

    async def mark_stopping(self, reason: str = "stopping") -> bool:
        return await self.transition(RuntimePhase.STOPPING, reason)

    async def mark_stopped(self, reason: str = "stopped") -> bool:
        return await self.transition(RuntimePhase.STOPPED, reason)

    async def mark_faulted(self, reason: str = "faulted") -> bool:
        return await self.transition(RuntimePhase.FAULTED, reason)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def current_phase(self) -> RuntimePhase:
        return self._state.phase

    @property
    def is_running(self) -> bool:
        return self._state.phase == RuntimePhase.RUNNING

    @property
    def is_accepting_work(self) -> bool:
        return self._state.is_accepting


# ===========================================================================
# Shutdown Manager
# ===========================================================================

@dataclass
class ShutdownHook:
    """A registered shutdown callback with priority ordering."""
    name: str
    fn: Callable[[], Awaitable[None]]
    priority: int = 50   # lower = runs first
    timeout_seconds: float = 10.0
    registered_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


class ShutdownManager:
    """
    Coordinates graceful runtime shutdown.

    Features:
      - Hook registration with explicit priority ordering.
      - Timeout per hook — a slow hook doesn't block others.
      - OS signal handling (SIGINT / SIGTERM).
      - Publishes ShutdownRequestedEvent before executing hooks.
      - Idempotent: second shutdown() call is a no-op.

    Usage:
        shutdown_mgr.register_hook("queue", queue.stop, priority=10)
        shutdown_mgr.register_hook("scheduler", scheduler.stop, priority=20)
        shutdown_mgr.register_hook("bus", bus.stop, priority=90)

        await shutdown_mgr.shutdown(reason="SIGTERM received")
    """

    def __init__(
        self,
        event_bus: EventBus,
        supervisor: LifecycleSupervisor,
        global_timeout: float = 30.0,
    ) -> None:
        self._bus = event_bus
        self._supervisor = supervisor
        self._global_timeout = global_timeout
        self._hooks: list[ShutdownHook] = []
        self._shutdown_initiated = False
        self._shutdown_complete = asyncio.Event()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Signal registration
    # ------------------------------------------------------------------

    def install_signal_handlers(self) -> None:
        """
        Install OS-level signal handlers for SIGINT and SIGTERM.
        Must be called from the main async thread.
        """
        loop = asyncio.get_event_loop()

        def _handle_signal(sig: signal.Signals) -> None:
            logger.info("ShutdownManager: received signal %s", sig.name)
            asyncio.create_task(
                self.shutdown(reason=f"signal {sig.name}"),
                name="shutdown-from-signal",
            )

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal, sig)
                logger.debug("Installed signal handler: %s", sig.name)
            except (NotImplementedError, OSError):
                # Windows doesn't support loop.add_signal_handler
                logger.debug("Signal handler not supported for %s", sig.name)

    # ------------------------------------------------------------------
    # Hook registration
    # ------------------------------------------------------------------

    def register_hook(
        self,
        name: str,
        fn: Callable[[], Awaitable[None]],
        priority: int = 50,
        timeout_seconds: float = 10.0,
    ) -> None:
        """
        Register a shutdown hook.

        Hooks are executed in ascending priority order (lower priority = earlier).
        Each hook gets an individual timeout.
        """
        hook = ShutdownHook(name=name, fn=fn, priority=priority, timeout_seconds=timeout_seconds)
        self._hooks.append(hook)
        self._hooks.sort(key=lambda h: h.priority)
        logger.debug("Shutdown hook registered: %r priority=%d", name, priority)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self, reason: str = "") -> None:
        """
        Execute graceful shutdown sequence.

        Idempotent — safe to call multiple times; only first call runs.
        """
        async with self._lock:
            if self._shutdown_initiated:
                logger.debug("ShutdownManager: shutdown already in progress")
                return
            self._shutdown_initiated = True

        logger.info("ShutdownManager: initiating shutdown. reason=%r", reason)

        await self._bus.publish_nowait(ShutdownRequestedEvent(reason=reason))
        await self._supervisor.mark_draining(reason=reason)

        try:
            await asyncio.wait_for(
                self._run_hooks(),
                timeout=self._global_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                "ShutdownManager: global timeout (%.1fs) exceeded during shutdown",
                self._global_timeout,
            )

        await self._supervisor.mark_stopping(reason="hooks complete")
        await self._supervisor.mark_stopped(reason="shutdown complete")
        self._shutdown_complete.set()
        logger.info("ShutdownManager: shutdown complete")

    async def wait_for_shutdown(self) -> None:
        """Block until shutdown completes."""
        await self._shutdown_complete.wait()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_initiated

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _run_hooks(self) -> None:
        for hook in self._hooks:
            logger.debug("Running shutdown hook: %r (timeout=%.1fs)", hook.name, hook.timeout_seconds)
            try:
                await asyncio.wait_for(hook.fn(), timeout=hook.timeout_seconds)
                logger.info("Shutdown hook completed: %r", hook.name)
            except asyncio.TimeoutError:
                logger.warning(
                    "Shutdown hook timed out: %r (%.1fs)", hook.name, hook.timeout_seconds
                )
            except Exception:
                logger.exception("Shutdown hook raised: %r", hook.name)
                # Never abort shutdown due to a hook error
