# Usage Examples

This document demonstrates common usage patterns for the Autonomous Engineering Runtime.

---

# Starting the Runtime

## Start With Default Configuration

```bash
python -m autonomous_runtime
```

---

## Start With Custom Runtime Name

```bash
python -m autonomous_runtime --name dev-runtime
```

---

## Start With Custom Worker Count

```bash
python -m autonomous_runtime --workers 8
```

---

## Start With Debug Logging

```bash
python -m autonomous_runtime --log-level DEBUG
```

---

## Start Using JSON Config File

```bash
python -m autonomous_runtime --config config.json
```

---

# Bootstrapping the Runtime Programmatically

```python
from autonomous_runtime.core.bootstrap import bootstrap_local

manager = await bootstrap_local(
    runtime_name="example-runtime",
    data_dir="./runtime_data",
)

print(manager.is_running)
```

---

# Creating a Workflow

```python
from autonomous_runtime.core.workflow_scheduler import WorkflowDefinition

async def analyze_repository():
    return {
        "status": "complete",
        "files_scanned": 42,
    }

workflow = WorkflowDefinition(
    fn=analyze_repository,
    name="repository-analysis",
    priority=5,
)
```

---

# Scheduling a Workflow

```python
record = await manager.workflow_scheduler.schedule(workflow)

print(record.workflow_id)
print(record.status)
```

---

# Waiting for Workflow Completion

```python
result = await manager.workflow_scheduler.wait_for(
    record.workflow_id,
    timeout=30.0,
)

print(result)
```

---

# Executing Sequential Orchestration Plans

```python
from autonomous_runtime.core.workflow_scheduler import WorkflowDefinition
from autonomous_runtime.core.orchestration_engine import (
    OrchestrationPlan,
    PlanStrategy,
)

async def analyze():
    return "analysis complete"

async def generate():
    return "patch generated"

async def validate():
    return "validation passed"

plan = OrchestrationPlan(
    name="sequential-plan",
    strategy=PlanStrategy.SEQUENTIAL,
    steps=[
        WorkflowDefinition(fn=analyze, name="analyze"),
        WorkflowDefinition(fn=generate, name="generate"),
        WorkflowDefinition(fn=validate, name="validate"),
    ],
)

result = await manager.orchestration_engine.execute(plan)

print(result.success)
```

---

# Executing Parallel Orchestration Plans

```python
from autonomous_runtime.core.workflow_scheduler import WorkflowDefinition
from autonomous_runtime.core.orchestration_engine import (
    OrchestrationPlan,
    PlanStrategy,
)

async def task_one():
    return "task one complete"

async def task_two():
    return "task two complete"

async def task_three():
    return "task three complete"

plan = OrchestrationPlan(
    name="parallel-plan",
    strategy=PlanStrategy.PARALLEL,
    steps=[
        WorkflowDefinition(fn=task_one, name="task-one"),
        WorkflowDefinition(fn=task_two, name="task-two"),
        WorkflowDefinition(fn=task_three, name="task-three"),
    ],
)

result = await manager.orchestration_engine.execute(plan)

print(result.success)
print(len(result.step_results))
```

---

# Subscribing to Runtime Events

```python
from autonomous_runtime.core.event_bus import (
    RuntimePhaseChangedEvent,
)

async def on_phase_change(event):
    print(
        f"phase changed: "
        f"{event.from_phase} -> {event.to_phase}"
    )

manager.event_bus.subscribe(
    RuntimePhaseChangedEvent,
    on_phase_change,
    name="phase-listener",
)
```

---

# Creating Runtime Sessions

```python
from autonomous_runtime.core.runtime_session import RuntimeSession

session = RuntimeSession(
    name="agent-session",
    tags=["agent", "analysis"],
)

session.activate()

print(session.session_id)
print(session.status)
```

---

# Using the Dependency Container

```python
from autonomous_runtime.core.dependency_container import (
    DependencyContainer,
)

container = DependencyContainer()

container.bind_singleton(
    "config",
    lambda: {"env": "local"},
)

config = container.resolve("config")

print(config)
```

---

# Creating and Tracking Execution Cycles

```python
from autonomous_runtime.core.execution_cycle import (
    ExecutionCycle,
    ExecutionPhase,
)

cycle = ExecutionCycle(name="example-execution")

cycle.transition(ExecutionPhase.PLANNING)
cycle.transition(ExecutionPhase.RETRIEVING)
cycle.transition(ExecutionPhase.EXECUTING)

print(cycle.phase)
print(len(cycle.audit_trail))
```

---

# Graceful Runtime Shutdown

```python
await manager.stop(
    reason="shutdown requested",
)
```

---

# Runtime Status Inspection

```python
status = manager.status()

print(status)
```

---

# Running the Test Suite

```bash
pytest
```

---

# Running a Specific Test File

```bash
pytest tests/test_runtime.py
```

---

# Example Runtime Output

```text
[INFO] Runtime bootstrap started
[INFO] Environment validated successfully
[INFO] EventBus started
[INFO] WorkerQueue started
[INFO] WorkflowScheduler started
[INFO] OrchestrationEngine started
[INFO] Runtime phase changed: READY -> RUNNING
[INFO] Runtime operational
```

---

# Common Development Workflow

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest

# Start runtime
python -m autonomous_runtime
```