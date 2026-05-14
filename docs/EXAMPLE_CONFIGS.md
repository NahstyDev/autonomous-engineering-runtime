# Example Configuration Files

---

# .env.example

```env
# =========================================================
# Autonomous Engineering Runtime Environment Configuration
# =========================================================

# Runtime identity
RUNTIME_NAME=autonomous-runtime
RUNTIME_ENV=local

# Logging
RUNTIME_LOG_LEVEL=INFO

# Concurrency
RUNTIME_WORKER_COUNT=4
RUNTIME_MAX_CONCURRENT_TASKS=16
RUNTIME_QUEUE_MAX_SIZE=1000

# Timeouts
RUNTIME_TASK_TIMEOUT=300.0
RUNTIME_SHUTDOWN_TIMEOUT=30.0

# Persistence
RUNTIME_PERSISTENCE_BACKEND=sqlite
RUNTIME_DATA_DIR=./runtime_data
RUNTIME_DB_FILENAME=runtime.db

# Persistence features
RUNTIME_ENABLE_WAL=true
RUNTIME_ENABLE_JOURNALING=true
RUNTIME_ENABLE_CHECKPOINTING=true

# Feature flags
RUNTIME_ENABLE_EVENT_BUS=true
RUNTIME_ENABLE_WORKFLOW_SCHEDULER=true
RUNTIME_ENABLE_ORCHESTRATION=true
RUNTIME_ENABLE_EVENT_PERSISTENCE=true
```

---

# Example JSON Runtime Configuration

Save as:

```text
config.json
```

```json
{
  "runtime_name": "autonomous-runtime",
  "environment": "local",

  "concurrency": {
    "max_concurrent_tasks": 16,
    "worker_count": 4,
    "queue_max_size": 1000,
    "task_timeout_seconds": 300.0,
    "shutdown_timeout_seconds": 30.0
  },

  "persistence": {
    "backend": "sqlite",
    "data_dir": "./runtime_data",
    "db_path": "./runtime_data/runtime.db",
    "journal_dir": "./runtime_data/journal",
    "artifact_dir": "./runtime_data/artifacts",
    "checkpoint_dir": "./runtime_data/checkpoints",
    "enable_wal": true,
    "enable_journaling": true,
    "enable_checkpointing": true
  },

  "logging": {
    "level": "INFO",
    "log_dir": "./logs",
    "enable_structured": true,
    "enable_file_logging": true,
    "max_bytes": 10485760,
    "backup_count": 5
  },

  "features": {
    "enable_event_bus": true,
    "enable_workflow_scheduler": true,
    "enable_orchestration": true,
    "enable_event_persistence": true
  }
}
```

---

# Example Development Configuration

```json
{
  "runtime_name": "dev-runtime",
  "environment": "local",

  "concurrency": {
    "max_concurrent_tasks": 8,
    "worker_count": 2,
    "queue_max_size": 250,
    "task_timeout_seconds": 120.0,
    "shutdown_timeout_seconds": 15.0
  },

  "logging": {
    "level": "DEBUG",
    "log_dir": "./logs",
    "enable_structured": true,
    "enable_file_logging": true
  },

  "features": {
    "enable_event_bus": true,
    "enable_workflow_scheduler": true,
    "enable_orchestration": true,
    "enable_event_persistence": false
  }
}
```

---

# Example Production-Oriented Configuration

```json
{
  "runtime_name": "production-runtime",
  "environment": "production",

  "concurrency": {
    "max_concurrent_tasks": 64,
    "worker_count": 16,
    "queue_max_size": 5000,
    "task_timeout_seconds": 600.0,
    "shutdown_timeout_seconds": 60.0
  },

  "persistence": {
    "backend": "postgres",
    "data_dir": "./runtime_data",
    "enable_wal": true,
    "enable_journaling": true,
    "enable_checkpointing": true
  },

  "logging": {
    "level": "INFO",
    "log_dir": "./logs",
    "enable_structured": true,
    "enable_file_logging": true,
    "max_bytes": 52428800,
    "backup_count": 10
  },

  "features": {
    "enable_event_bus": true,
    "enable_workflow_scheduler": true,
    "enable_orchestration": true,
    "enable_event_persistence": true
  }
}
```

---

# Loading Configuration

## Environment Variables

```bash
python -m autonomous_runtime
```

---

## JSON Config File

```bash
python -m autonomous_runtime --config config.json
```

---

# Recommended Repository Structure

```text
project-root/
├── .env.example
├── config.example.json
├── config.dev.json
├── config.production.json
└── docs/
```