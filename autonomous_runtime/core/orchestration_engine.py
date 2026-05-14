"""
orchestration_engine.py — Orchestration Core (Step 1.10)

The OrchestrationEngine is the highest-level execution coordinator.
It sits above the WorkflowScheduler and provides:

  - Orchestration policies (concurrency limits, priority overrides).
  - Execution plan submission (multi-step sequential/parallel plans).
  - Runtime coordination (pause propagation, drain coordination).
  - Observability hooks (execution metrics, active plan tracking).
  - Future integration point for Phase 9 (Planning) and Phase 10 (Repair Loop).

Design:
  - Plans are ordered sequences of WorkflowDefinitions.
  - Sequential plans run steps in order, stopping on first failure.
  - Parallel plans run all steps concurrently within the semaphore budget.
  - Every plan produces an OrchestrationResult with full audit data.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable

from .runtime_context import RuntimeContext
from .workflow_scheduler import WorkflowScheduler, WorkflowDefinition, WorkflowRecord
from .concurrency_manager import ConcurrencyManager
from .event_bus import EventBus, RuntimeEvent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Plan structures
# ---------------------------------------------------------------------------

class PlanStrategy(str, Enum):
    SEQUENTIAL = "sequential"   # steps run in order; fail-fast
    PARALLEL   = "parallel"     # steps run concurrently
    BEST_EFFORT = "best_effort" # parallel, failures logged but not fatal


@dataclass
class OrchestrationPlan:
    """
    A named, ordered collection of workflow steps.

    Submitted to the OrchestrationEngine for coordinated execution.
    """
    steps: list[WorkflowDefinition]
    name: str = "unnamed-plan"
    plan_id: str = field(default_factory=lambda: f"plan-{uuid.uuid4().hex[:8]}")
    strategy: PlanStrategy = PlanStrategy.SEQUENTIAL
    timeout_seconds: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None


@dataclass
class StepResult:
    """Result of a single plan step."""
    step_index: int
    workflow_id: str
    workflow_name: str
    success: bool
    result: Any = None
    error: str | None = None
    duration_seconds: float = 0.0


@dataclass
class OrchestrationResult:
    """
    Full result of executing an OrchestrationPlan.

    Provides per-step audit data and overall outcome.
    This is the foundation for replay validation in Phase 2.
    """
    plan_id: str
    plan_name: str
    strategy: PlanStrategy
    success: bool
    step_results: list[StepResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    ended_at: datetime | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        end = self.ended_at or datetime.now(tz=timezone.utc)
        return (end - self.started_at).total_seconds()

    @property
    def failed_steps(self) -> list[StepResult]:
        return [s for s in self.step_results if not s.success]

    @property
    def successful_steps(self) -> list[StepResult]:
        return [s for s in self.step_results if s.success]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "plan_name": self.plan_name,
            "strategy": self.strategy.value,
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "error": self.error,
            "step_count": len(self.step_results),
            "successful_steps": len(self.successful_steps),
            "failed_steps": len(self.failed_steps),
            "steps": [
                {
                    "index": s.step_index,
                    "workflow_id": s.workflow_id,
                    "name": s.workflow_name,
                    "success": s.success,
                    "error": s.error,
                    "duration_seconds": s.duration_seconds,
                }
                for s in self.step_results
            ],
            "metadata": self.metadata,
        }


# ---------------------------------------------------------------------------
# Orchestration events
# ---------------------------------------------------------------------------

@dataclass
class PlanStartedEvent(RuntimeEvent):
    plan_id: str = ""
    plan_name: str = ""
    strategy: str = ""
    step_count: int = 0
    source: str = "orchestration_engine"


@dataclass
class PlanCompletedEvent(RuntimeEvent):
    plan_id: str = ""
    plan_name: str = ""
    success: bool = True
    duration_seconds: float = 0.0
    failed_steps: int = 0
    source: str = "orchestration_engine"


# ---------------------------------------------------------------------------
# Orchestration policies
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OrchestrationPolicy:
    """
    Runtime coordination policy applied to plan execution.

    Extension point for Phase 5 (Validation & Safety) and
    Phase 9 (Planning & Cognitive Systems).
    """
    max_parallel_steps: int = 8         # Parallel cap per plan
    fail_fast: bool = True              # Stop sequential plan on first failure
    enable_timeout: bool = True         # Enforce per-plan timeouts
    require_running_phase: bool = True  # Reject submissions if not RUNNING/READY


DEFAULT_POLICY = OrchestrationPolicy()


# ---------------------------------------------------------------------------
# Orchestration engine
# ---------------------------------------------------------------------------

class OrchestrationEngine:
    """
    Top-level async execution coordinator.

    Accepts OrchestrationPlans and drives them through the WorkflowScheduler
    with policy enforcement and full result tracking.

    Future integration points:
      - Phase 9: accept PlanGraph instead of linear OrchestrationPlan
      - Phase 10: wrap execution in repair/retry loop
      - Phase 5: validate each step result against safety contracts
      - Phase 16: emit trace spans for distributed tracing

    Usage:
        engine = OrchestrationEngine(context, scheduler, concurrency, event_bus)
        await engine.start()

        plan = OrchestrationPlan(
            name="implement-feature",
            steps=[
                WorkflowDefinition(fn=analyze_repo, name="analyze"),
                WorkflowDefinition(fn=generate_patch, name="patch"),
                WorkflowDefinition(fn=run_tests, name="test"),
            ],
            strategy=PlanStrategy.SEQUENTIAL,
        )
        result = await engine.execute(plan)
    """

    def __init__(
        self,
        context: RuntimeContext,
        scheduler: WorkflowScheduler,
        concurrency: ConcurrencyManager,
        event_bus: EventBus,
        policy: OrchestrationPolicy = DEFAULT_POLICY,
    ) -> None:
        self._context = context
        self._scheduler = scheduler
        self._concurrency = concurrency
        self._bus = event_bus
        self._policy = policy

        self._active_plans: dict[str, OrchestrationResult] = {}
        self._completed_plans: dict[str, OrchestrationResult] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._stopping = False

        # Stats
        self._plans_submitted = 0
        self._plans_succeeded = 0
        self._plans_failed = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._started = True
        logger.info("OrchestrationEngine started")

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        # Cancel active plans
        async with self._lock:
            active = list(self._active_plans.keys())
        if active:
            logger.info("OrchestrationEngine: cancelling %d active plans", len(active))
            for plan_id in active:
                await self._cancel_plan(plan_id, reason="engine stopping")
        logger.info("OrchestrationEngine stopped")

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        plan: OrchestrationPlan,
        policy: OrchestrationPolicy | None = None,
    ) -> OrchestrationResult:
        """
        Execute an OrchestrationPlan.

        Returns OrchestrationResult with full step-level audit data.
        Never raises — failures are captured in the result.
        """
        effective_policy = policy or self._policy
        self._assert_ready(effective_policy)

        result = OrchestrationResult(
            plan_id=plan.plan_id,
            plan_name=plan.name,
            strategy=plan.strategy,
            success=False,
            metadata=plan.metadata,
        )

        async with self._lock:
            self._active_plans[plan.plan_id] = result
            self._plans_submitted += 1

        logger.info(
            "OrchestrationEngine: executing plan %s name=%r steps=%d strategy=%s",
            plan.plan_id, plan.name, len(plan.steps), plan.strategy.value,
        )

        await self._bus.publish_nowait(PlanStartedEvent(
            plan_id=plan.plan_id,
            plan_name=plan.name,
            strategy=plan.strategy.value,
            step_count=len(plan.steps),
        ))

        try:
            if plan.strategy == PlanStrategy.SEQUENTIAL:
                await self._execute_sequential(plan, result, effective_policy)
            elif plan.strategy in (PlanStrategy.PARALLEL, PlanStrategy.BEST_EFFORT):
                await self._execute_parallel(plan, result, effective_policy)
            else:
                raise ValueError(f"Unknown plan strategy: {plan.strategy!r}")

        except asyncio.CancelledError:
            result.error = "plan cancelled"
            result.success = False
        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            result.success = False
            logger.exception("OrchestrationEngine: plan %s raised unexpectedly", plan.plan_id)
        finally:
            result.ended_at = datetime.now(tz=timezone.utc)

        async with self._lock:
            self._active_plans.pop(plan.plan_id, None)
            self._completed_plans[plan.plan_id] = result
            if result.success:
                self._plans_succeeded += 1
            else:
                self._plans_failed += 1

        await self._bus.publish_nowait(PlanCompletedEvent(
            plan_id=plan.plan_id,
            plan_name=plan.name,
            success=result.success,
            duration_seconds=result.duration_seconds,
            failed_steps=len(result.failed_steps),
        ))

        log_fn = logger.info if result.success else logger.warning
        log_fn(
            "Plan %s %s name=%r steps=%d/%d duration=%.2fs",
            plan.plan_id,
            "succeeded" if result.success else "failed",
            plan.name,
            len(result.successful_steps),
            len(plan.steps),
            result.duration_seconds,
        )
        return result

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    async def _execute_sequential(
        self,
        plan: OrchestrationPlan,
        result: OrchestrationResult,
        policy: OrchestrationPolicy,
    ) -> None:
        """Execute steps one at a time. Stops on first failure if fail_fast=True."""
        for i, step_def in enumerate(plan.steps):
            step_result = await self._run_step(step_def, i)
            result.step_results.append(step_result)

            if not step_result.success and policy.fail_fast:
                result.error = f"Step {i} ({step_def.name!r}) failed: {step_result.error}"
                result.success = False
                logger.warning(
                    "Plan %s: fail-fast triggered at step %d/%d (%r)",
                    plan.plan_id, i + 1, len(plan.steps), step_def.name,
                )
                return

        result.success = all(s.success for s in result.step_results)

    async def _execute_parallel(
        self,
        plan: OrchestrationPlan,
        result: OrchestrationResult,
        policy: OrchestrationPolicy,
    ) -> None:
        """Execute all steps concurrently, bounded by max_parallel_steps."""
        semaphore = asyncio.Semaphore(policy.max_parallel_steps)

        async def _bounded(step_def: WorkflowDefinition, idx: int) -> StepResult:
            async with semaphore:
                return await self._run_step(step_def, idx)

        tasks = [
            asyncio.create_task(_bounded(step, i), name=f"plan-step-{i}")
            for i, step in enumerate(plan.steps)
        ]

        step_results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, sr in enumerate(step_results):
            if isinstance(sr, Exception):
                result.step_results.append(StepResult(
                    step_index=i,
                    workflow_id="",
                    workflow_name=plan.steps[i].name,
                    success=False,
                    error=f"{type(sr).__name__}: {sr}",
                ))
            else:
                result.step_results.append(sr)  # type: ignore[arg-type]

        if plan.strategy == PlanStrategy.BEST_EFFORT:
            result.success = True  # best-effort never fully fails
        else:
            result.success = all(s.success for s in result.step_results)

    async def _run_step(
        self, step_def: WorkflowDefinition, index: int
    ) -> StepResult:
        """Submit a single step to the scheduler and await its result."""
        t0 = datetime.now(tz=timezone.utc)
        try:
            record: WorkflowRecord = await self._scheduler.schedule(step_def)
            step_result_value = await self._scheduler.wait_for(
                record.workflow_id,
                timeout=step_def.timeout_seconds,
            )
            duration = (datetime.now(tz=timezone.utc) - t0).total_seconds()
            return StepResult(
                step_index=index,
                workflow_id=record.workflow_id,
                workflow_name=step_def.name,
                success=True,
                result=step_result_value,
                duration_seconds=duration,
            )
        except asyncio.CancelledError:
            duration = (datetime.now(tz=timezone.utc) - t0).total_seconds()
            return StepResult(
                step_index=index,
                workflow_id="",
                workflow_name=step_def.name,
                success=False,
                error="cancelled",
                duration_seconds=duration,
            )
        except Exception as exc:
            duration = (datetime.now(tz=timezone.utc) - t0).total_seconds()
            logger.warning("Step %d (%r) failed: %s", index, step_def.name, exc)
            return StepResult(
                step_index=index,
                workflow_id="",
                workflow_name=step_def.name,
                success=False,
                error=f"{type(exc).__name__}: {exc}",
                duration_seconds=duration,
            )

    # ------------------------------------------------------------------
    # Plan management
    # ------------------------------------------------------------------

    async def _cancel_plan(self, plan_id: str, reason: str = "") -> None:
        result = self._active_plans.get(plan_id)
        if result:
            result.error = reason
            result.success = False
            result.ended_at = datetime.now(tz=timezone.utc)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def get_result(self, plan_id: str) -> OrchestrationResult | None:
        return self._completed_plans.get(plan_id) or self._active_plans.get(plan_id)

    def active_plan_count(self) -> int:
        return len(self._active_plans)

    def summary(self) -> dict[str, Any]:
        return {
            "started": self._started,
            "stopping": self._stopping,
            "plans_submitted": self._plans_submitted,
            "plans_succeeded": self._plans_succeeded,
            "plans_failed": self._plans_failed,
            "active_plans": self.active_plan_count(),
            "policy": {
                "max_parallel_steps": self._policy.max_parallel_steps,
                "fail_fast": self._policy.fail_fast,
            },
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _assert_ready(self, policy: OrchestrationPolicy) -> None:
        if not self._started:
            raise RuntimeError("OrchestrationEngine not started")
        if self._stopping:
            raise RuntimeError("OrchestrationEngine is stopping")
        if policy.require_running_phase and not self._context.is_accepting_work:
            raise RuntimeError(
                f"OrchestrationEngine: runtime is not accepting work "
                f"(phase={self._context.current_phase.value!r})"
            )
