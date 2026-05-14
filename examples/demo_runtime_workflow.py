from __future__ import annotations

import asyncio

from autonomous_runtime.core.bootstrap import bootstrap_local
from autonomous_runtime.core.workflow_scheduler import WorkflowDefinition
from autonomous_runtime.core.orchestration_engine import (
    OrchestrationPlan,
    PlanStrategy,
)


async def analyze_repository():
    print("[STEP] analyzing repository...")
    await asyncio.sleep(1)
    return {
        "files_scanned": 42,
        "issues_found": 3,
    }


async def generate_patch():
    print("[STEP] generating patch...")
    await asyncio.sleep(1)
    return {
        "patches_generated": 2,
    }


async def validate_changes():
    print("[STEP] validating changes...")
    await asyncio.sleep(1)
    return {
        "validation": "passed",
    }


async def finalize():
    print("[STEP] finalizing workflow...")
    await asyncio.sleep(1)
    return {
        "status": "complete",
    }


async def main():
    print("\n=== Bootstrapping Runtime ===\n")

    manager = await bootstrap_local(
        runtime_name="demo-runtime",
        data_dir="./runtime_data",
    )

    print("[INFO] runtime started")
    print(manager.status())

    plan = OrchestrationPlan(
        name="demo-orchestration-plan",
        strategy=PlanStrategy.SEQUENTIAL,
        steps=[
            WorkflowDefinition(
                fn=analyze_repository,
                name="analyze",
            ),
            WorkflowDefinition(
                fn=generate_patch,
                name="generate",
            ),
            WorkflowDefinition(
                fn=validate_changes,
                name="validate",
            ),
            WorkflowDefinition(
                fn=finalize,
                name="finalize",
            ),
        ],
    )

    print("\n=== Executing Plan ===\n")

    result = await manager.orchestration_engine.execute(plan)

    print("\n=== Execution Result ===\n")

    print(f"Success: {result.success}")
    print(f"Completed Steps: {len(result.step_results)}")

    for step in result.step_results:
        print(
            f"- {step.workflow_name}: "
            f"success={step.success}"
        )

    print("\n=== Shutting Down Runtime ===\n")

    await manager.stop(reason="demo complete")

    print("[INFO] runtime stopped")


if __name__ == "__main__":
    asyncio.run(main())