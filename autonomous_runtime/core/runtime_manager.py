"""
runtime_manager.py — Runtime Manager (Step 1.3)

Top-level coordinator for the runtime. Owns the lifecycle of all
Phase 1 subsystems and exposes a clean operational interface:
start(), stop(), pause(), resume().

The RuntimeManager is the only component that directly orchestrates
subsystem startup/shutdown ordering. All other components are
passive — they respond to lifecycle events, not drive them.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .config import RuntimeConfig
from .runtime_state import RuntimeStateStore, RuntimePhase
from .runtime_context import RuntimeContext
from .runtime_session import SessionFactory
from .environment_manager import EnvironmentManager
from .service_registry import ServiceRegistry
from .dependency_container import DependencyContainer
from .concurrency_manager import ConcurrencyManager
from .event_bus import EventBus
from .worker_queue import WorkerQueue
from .workflow_scheduler import WorkflowScheduler
from .lifecycle_supervisor import LifecycleSupervisor, ShutdownManager
from .orchestration_engine import OrchestrationEngine

logger = logging.getLogger(__name__)


class RuntimeManager:
    """
    Top-level runtime coordinator.

    Owns and sequences:
      1. Service construction
      2. Service registration
      3. Ordered startup
      4. Operational control (pause/resume)
      5. Ordered shutdown

    This is the only place where subsystem construction order is encoded.
    Phase 2+ subsystems are registered via extend().

    Usage:
        manager = RuntimeManager(config)
        await manager.start()
        # ... runtime is now RUNNING
        await manager.stop()
    """

    def __init__(self, config: RuntimeConfig) -> None:
        self._config = config
        self._started = False

        # Core infrastructure — constructed early, before start()
        self._state = RuntimeStateStore()
        self._registry = ServiceRegistry()
        self._container = DependencyContainer()
        self._session_factory = SessionFactory()

        # Context — shared envelope for all subsystems
        self._context = RuntimeContext(
            config=config,
            state=self._state,
            registry=self._registry,
        )

        # These are wired during _build_services()
        self._event_bus: EventBus | None = None
        self._concurrency_manager: ConcurrencyManager | None = None
        self._worker_queue: WorkerQueue | None = None
        self._workflow_scheduler: WorkflowScheduler | None = None
        self._lifecycle_supervisor: LifecycleSupervisor | None = None
        self._shutdown_manager: ShutdownManager | None = None
        self._orchestration_engine: OrchestrationEngine | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def context(self) -> RuntimeContext:
        return self._context

    @property
    def is_running(self) -> bool:
        return self._state.phase == RuntimePhase.RUNNING

    async def start(self) -> None:
        """
        Full runtime startup sequence.

        Phase sequence: UNINITIALIZED → BOOTSTRAPPING → READY → RUNNING
        """
        if self._started:
            raise RuntimeError("RuntimeManager.start() called more than once")
        self._started = True

        logger.info(
            "RuntimeManager starting: %s [%s] env=%s",
            self._config.runtime_id,
            self._config.runtime_name,
            self._config.environment,
        )

        self._state.transition(RuntimePhase.BOOTSTRAPPING, reason="RuntimeManager.start()")
        self._build_services()
        await self._start_services()
        self._state.transition(RuntimePhase.READY, reason="services started")
        self._state.transition(RuntimePhase.RUNNING, reason="runtime operational")

        logger.info("RuntimeManager started — runtime is RUNNING")

    async def stop(self, reason: str = "stop requested") -> None:
        """Initiate graceful shutdown."""
        if self._shutdown_manager is None:
            logger.warning("RuntimeManager.stop() called before start()")
            return
        await self._shutdown_manager.shutdown(reason=reason)

    async def pause(self, reason: str = "") -> bool:
        """Pause the runtime (no new work accepted, existing work completes)."""
        if self._lifecycle_supervisor is None:
            return False
        success = await self._lifecycle_supervisor.mark_paused(reason=reason)
        if success:
            logger.info("RuntimeManager: paused. reason=%r", reason)
        return success

    async def resume(self, reason: str = "") -> bool:
        """Resume a paused runtime."""
        if self._lifecycle_supervisor is None:
            return False
        success = await self._lifecycle_supervisor.mark_running(reason=reason)
        if success:
            logger.info("RuntimeManager: resumed. reason=%r", reason)
        return success

    async def wait_for_shutdown(self) -> None:
        """Block until shutdown completes."""
        if self._shutdown_manager:
            await self._shutdown_manager.wait_for_shutdown()

    # ------------------------------------------------------------------
    # Service accessors (typed convenience)
    # ------------------------------------------------------------------

    @property
    def event_bus(self) -> EventBus:
        return self._require("event_bus", EventBus)

    @property
    def worker_queue(self) -> WorkerQueue:
        return self._require("worker_queue", WorkerQueue)

    @property
    def workflow_scheduler(self) -> WorkflowScheduler:
        return self._require("workflow_scheduler", WorkflowScheduler)

    @property
    def orchestration_engine(self) -> OrchestrationEngine:
        return self._require("orchestration_engine", OrchestrationEngine)

    @property
    def concurrency_manager(self) -> ConcurrencyManager:
        return self._require("concurrency_manager", ConcurrencyManager)

    @property
    def session_factory(self) -> SessionFactory:
        return self._session_factory

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "runtime_id": self._config.runtime_id,
            "runtime_name": self._config.runtime_name,
            "environment": self._config.environment,
            "phase": self._state.phase.value,
            "started": self._started,
        }
        if self._worker_queue:
            result["worker_queue"] = self._worker_queue.summary()
        if self._workflow_scheduler:
            result["workflow_scheduler"] = self._workflow_scheduler.summary()
        if self._concurrency_manager:
            result["concurrency"] = self._concurrency_manager.summary()
        if self._event_bus:
            result["event_bus"] = self._event_bus.summary()
        result["services"] = self._registry.summary()
        return result

    # ------------------------------------------------------------------
    # Extension point (Phase 2+)
    # ------------------------------------------------------------------

    def extend(
        self,
        name: str,
        service: Any,
        interface: type | None = None,
        tags: list[str] | None = None,
    ) -> None:
        """
        Register an additional service post-bootstrap.

        Used by Phase 2+ subsystems (persistence, tools, agents)
        to integrate without modifying this class.
        """
        self._registry.register(name, service, interface=interface, tags=tags)
        logger.debug("RuntimeManager: extended with service %r", name)

    # ------------------------------------------------------------------
    # Internal — service construction
    # ------------------------------------------------------------------

    def _build_services(self) -> None:
        """Construct and wire all Phase 1 services. Order matters."""
        cfg = self._config

        # --- Event Bus (Step 1.7) ---
        self._event_bus = EventBus()
        self._registry.register("event_bus", self._event_bus, EventBus)
        self._container.bind_instance(EventBus, self._event_bus)

        # --- Concurrency Manager (Step 1.5) ---
        self._concurrency_manager = ConcurrencyManager(
            max_concurrent=cfg.concurrency.max_concurrent_tasks,
            name="runtime",
        )
        self._registry.register("concurrency_manager", self._concurrency_manager, ConcurrencyManager)
        self._container.bind_instance(ConcurrencyManager, self._concurrency_manager)

        # --- Worker Queue (Step 1.8) ---
        self._worker_queue = WorkerQueue(
            worker_count=cfg.concurrency.worker_count,
            max_size=cfg.concurrency.queue_max_size,
            name="runtime",
        )
        self._registry.register("worker_queue", self._worker_queue, WorkerQueue)
        self._container.bind_instance(WorkerQueue, self._worker_queue)

        # --- Lifecycle Supervisor (Step 1.3) ---
        self._lifecycle_supervisor = LifecycleSupervisor(
            state=self._state,
            event_bus=self._event_bus,
        )
        self._registry.register("lifecycle_supervisor", self._lifecycle_supervisor, LifecycleSupervisor)
        self._container.bind_instance(LifecycleSupervisor, self._lifecycle_supervisor)

        # --- Workflow Scheduler (Step 1.9) ---
        self._workflow_scheduler = WorkflowScheduler(
            worker_queue=self._worker_queue,
            event_bus=self._event_bus,
        )
        self._registry.register("workflow_scheduler", self._workflow_scheduler, WorkflowScheduler)
        self._container.bind_instance(WorkflowScheduler, self._workflow_scheduler)

        # --- Orchestration Engine (Step 1.10) ---
        self._orchestration_engine = OrchestrationEngine(
            context=self._context,
            scheduler=self._workflow_scheduler,
            concurrency=self._concurrency_manager,
            event_bus=self._event_bus,
        )
        self._registry.register("orchestration_engine", self._orchestration_engine, OrchestrationEngine)
        self._container.bind_instance(OrchestrationEngine, self._orchestration_engine)

        # --- Shutdown Manager (Step 1.3) ---
        self._shutdown_manager = ShutdownManager(
            event_bus=self._event_bus,
            supervisor=self._lifecycle_supervisor,
            global_timeout=cfg.concurrency.shutdown_timeout_seconds,
        )
        self._registry.register("shutdown_manager", self._shutdown_manager, ShutdownManager)

        # Register shutdown hooks in LIFO-friendly priority order
        # Orchestration stops first (highest priority = first to drain)
        self._shutdown_manager.register_hook(
            "orchestration_engine", self._orchestration_engine.stop, priority=10, timeout_seconds=15.0
        )
        self._shutdown_manager.register_hook(
            "workflow_scheduler", self._workflow_scheduler.stop, priority=20, timeout_seconds=10.0
        )
        self._shutdown_manager.register_hook(
            "worker_queue", self._worker_queue.stop, priority=30, timeout_seconds=cfg.concurrency.shutdown_timeout_seconds
        )
        self._shutdown_manager.register_hook(
            "concurrency_manager", self._concurrency_manager.stop, priority=40, timeout_seconds=10.0
        )
        self._shutdown_manager.register_hook(
            "event_bus", self._event_bus.stop, priority=90, timeout_seconds=5.0
        )
        self._shutdown_manager.register_hook(
            "lifecycle_supervisor", self._lifecycle_supervisor.stop, priority=95, timeout_seconds=5.0
        )

        # Expose container via registry
        self._registry.register("dependency_container", self._container, DependencyContainer)
        self._registry.register("session_factory", self._session_factory, SessionFactory)

        logger.info("RuntimeManager: %d services constructed", len(self._registry))

    async def _start_services(self) -> None:
        """Start all services in registration order."""
        logger.info("RuntimeManager: starting services")

        assert self._event_bus is not None
        assert self._concurrency_manager is not None
        assert self._worker_queue is not None
        assert self._lifecycle_supervisor is not None
        assert self._workflow_scheduler is not None
        assert self._orchestration_engine is not None

        await self._event_bus.start()
        await self._concurrency_manager.start()
        await self._worker_queue.start()
        await self._lifecycle_supervisor.start()
        await self._workflow_scheduler.start()
        await self._orchestration_engine.start()

        self._shutdown_manager.install_signal_handlers()
        logger.info("RuntimeManager: all services started")

    def _require(self, name: str, t: type) -> Any:
        svc = self._registry.resolve_optional(name)
        if svc is None:
            raise RuntimeError(
                f"Service {name!r} not available — has RuntimeManager.start() been called?"
            )
        return svc
