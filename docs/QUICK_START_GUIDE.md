# Quick Start

This guide gets the Autonomous Engineering Runtime running locally in a few minutes.

---

# 1. Clone Repository

```bash
git clone https://github.com/NahstyDev/autonomous-engineering-runtime.git

cd autonomous-engineering-runtime
```

---

# 2. Create Virtual Environment

## Windows

```bash
python -m venv .venv

.venv\Scripts\activate
```

---

## Linux / macOS

```bash
python3 -m venv .venv

source .venv/bin/activate
```

---

# 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

# 4. Run Tests

Verify the runtime installs correctly:

```bash
pytest
```

---

# 5. Start Runtime

Run the autonomous runtime:

```bash
python -m autonomous_runtime
```

The runtime will:

- Bootstrap environment configuration
- Initialize runtime services
- Start worker queues
- Initialize orchestration systems
- Enter RUNNING state

---

# 6. Start With Custom Configuration

Example:

```bash
python -m autonomous_runtime --name dev-runtime --workers 8 --log-level DEBUG
```

---

# 7. Example Workflow Execution

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

plan = OrchestrationPlan(
    name="example-plan",
    strategy=PlanStrategy.SEQUENTIAL,
    steps=[
        WorkflowDefinition(fn=analyze, name="analyze"),
        WorkflowDefinition(fn=generate, name="generate"),
    ],
)

result = await manager.orchestration_engine.execute(plan)

print(result.success)
```

---

# Expected Runtime Features

The current implementation includes:

- Runtime lifecycle management
- Async orchestration
- Workflow scheduling
- Worker queue infrastructure
- Event-driven coordination
- Concurrency management
- Deterministic execution tracking
- Integration-tested runtime systems

---

# Next Steps

See additional documentation:

- `docs/ARCHITECTURE.md`
- `docs/RUNTIME_EXECUTION_FLOW.md`
- `docs/INSTALLATION.md`