# Runtime Execution Flow Diagram

```text
┌────────────────────────────┐
│        User Request        │
│  Workflow / Agent Command  │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│    OrchestrationEngine     │
│  Plan Coordination Layer   │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     OrchestrationPlan      │
│ Sequential / Parallel Plan │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     WorkflowScheduler      │
│ Workflow Lifecycle Control │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│        WorkerQueue         │
│   Priority Task Dispatch   │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     ConcurrencyManager     │
│  Bounded Async Execution   │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│      RuntimeSession        │
│   Execution Session Scope  │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│      ExecutionCycle        │
│ Deterministic State Machine│
└─────────────┬──────────────┘
              │
              ▼

      ┌───────────────────┐
      │     CREATED       │
      └─────────┬─────────┘
                │
                ▼
      ┌───────────────────┐
      │     PLANNING      │
      └─────────┬─────────┘
                │
                ▼
      ┌───────────────────┐
      │    RETRIEVING     │
      └─────────┬─────────┘
                │
                ▼
      ┌───────────────────┐
      │    EXECUTING      │
      └─────────┬─────────┘
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼

┌───────────────────┐  ┌───────────────────┐
│     VERIFYING     │  │     REPAIRING     │
└─────────┬─────────┘  └─────────┬─────────┘
          │                      │
          ▼                      │
┌───────────────────┐            │
│    FINALIZING     │◄───────────┘
└─────────┬─────────┘
          │
          ▼

 ┌───────────────────────────────────────┐
 │           TERMINAL STATES             │
 ├───────────────────────────────────────┤
 │ COMPLETED                             │
 │ FAILED                                │
 │ CANCELLED                             │
 │ TIMED_OUT                             │
 └───────────────────────────────────────┘

              │
              ▼
┌────────────────────────────┐
│         EventBus           │
│ Runtime Event Emission     │
└─────────────┬──────────────┘
              │
              ▼
┌────────────────────────────┐
│     Audit / Persistence    │
│ Replay & Recovery Systems  │
└────────────────────────────┘
```