"""
config.py — Immutable Runtime Configuration

Loaded once during bootstrap. All subsystems receive a frozen RuntimeConfig
and may never modify it. Supports env-var, file, and programmatic loading.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


VALID_ENVIRONMENTS = (
    "local",
    "staging",
    "production",
    "container",
)


logger = logging.getLogger(__name__)


def _new_runtime_id() -> str:
    return f"rt-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Sub-configs (frozen dataclasses)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConcurrencyConfig:
    """Concurrency and worker pool settings."""
    max_concurrent_tasks: int = 16
    worker_count: int = 4
    queue_max_size: int = 1000
    task_timeout_seconds: float = 300.0
    shutdown_timeout_seconds: float = 30.0


@dataclass(frozen=True)
class PersistenceConfig:
    """
    Persistence backend settings.

    Phase 1 target: SQLite with WAL mode enabled.
    Phase 2 will introduce a Postgres backend behind the same interface.
    """
    backend: str = "sqlite"
    data_dir: Path = Path("./runtime_data")
    db_path: Path = Path("./runtime_data/runtime.db")
    journal_dir: Path = Path("./runtime_data/journal")
    artifact_dir: Path = Path("./runtime_data/artifacts")
    checkpoint_dir: Path = Path("./runtime_data/checkpoints")
    enable_wal: bool = True
    enable_journaling: bool = True
    enable_checkpointing: bool = True


@dataclass(frozen=True)
class LoggingConfig:
    """Structured logging settings."""
    level: str = "INFO"
    log_dir: Path = Path("./logs")
    enable_structured: bool = True
    enable_file_logging: bool = True
    max_bytes: int = 10 * 1024 * 1024   # 10 MB
    backup_count: int = 5


@dataclass(frozen=True)
class FeatureFlags:
    """Runtime feature gate — explicit opt-in per capability."""
    enable_event_bus: bool = True
    enable_workflow_scheduler: bool = True
    enable_orchestration: bool = True
    enable_event_persistence: bool = True


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RuntimeConfig:
    """
    Immutable top-level runtime configuration.

    Frozen post-bootstrap. No subsystem may mutate this object.
    All runtime components receive this via dependency injection.
    """
    runtime_id: str
    runtime_name: str
    environment: str  # local | staging | production

    concurrency: ConcurrencyConfig
    persistence: PersistenceConfig
    logging: LoggingConfig
    features: FeatureFlags

    # -----------------------------------------------------------------------
    # Loaders
    # -----------------------------------------------------------------------

    @classmethod
    def from_env(cls, overrides: dict[str, Any] | None = None) -> "RuntimeConfig":
        """
        Build config from environment variables (RUNTIME_* prefix).
        Applies optional programmatic overrides after env resolution.
        """
        data_dir = Path(os.getenv("RUNTIME_DATA_DIR", "./runtime_data"))

        concurrency = ConcurrencyConfig(
            max_concurrent_tasks=int(os.getenv("RUNTIME_MAX_CONCURRENT_TASKS", "16")),
            worker_count=int(os.getenv("RUNTIME_WORKER_COUNT", "4")),
            queue_max_size=int(os.getenv("RUNTIME_QUEUE_MAX_SIZE", "1000")),
            task_timeout_seconds=float(os.getenv("RUNTIME_TASK_TIMEOUT", "300.0")),
            shutdown_timeout_seconds=float(os.getenv("RUNTIME_SHUTDOWN_TIMEOUT", "30.0")),
        )

        persistence = PersistenceConfig(
            backend=os.getenv("RUNTIME_PERSISTENCE_BACKEND", "sqlite"),
            data_dir=data_dir,
            db_path=data_dir / os.getenv("RUNTIME_DB_FILENAME", "runtime.db"),
            journal_dir=data_dir / "journal",
            artifact_dir=data_dir / "artifacts",
            checkpoint_dir=data_dir / "checkpoints",
            enable_wal=os.getenv("RUNTIME_ENABLE_WAL", "true").lower() == "true",
            enable_journaling=os.getenv("RUNTIME_ENABLE_JOURNALING", "true").lower() == "true",
            enable_checkpointing=os.getenv("RUNTIME_ENABLE_CHECKPOINTING", "true").lower() == "true",
        )

        log_cfg = LoggingConfig(
            level=os.getenv("RUNTIME_LOG_LEVEL", "INFO"),
            log_dir=Path(os.getenv("RUNTIME_LOG_DIR", "./logs")),
            enable_structured=os.getenv("RUNTIME_STRUCTURED_LOGGING", "true").lower() == "true",
            enable_file_logging=os.getenv("RUNTIME_FILE_LOGGING", "true").lower() == "true",
        )

        features = FeatureFlags(
            enable_event_bus=os.getenv("RUNTIME_ENABLE_EVENT_BUS", "true").lower() == "true",
            enable_workflow_scheduler=os.getenv("RUNTIME_ENABLE_SCHEDULER", "true").lower() == "true",
            enable_orchestration=os.getenv("RUNTIME_ENABLE_ORCHESTRATION", "true").lower() == "true",
            enable_event_persistence=os.getenv("RUNTIME_ENABLE_EVENT_PERSISTENCE", "true").lower() == "true",
        )

        init_dict: dict[str, Any] = {
            "runtime_id": os.getenv("RUNTIME_ID", _new_runtime_id()),
            "runtime_name": os.getenv("RUNTIME_NAME", "autonomous-runtime"),
            "environment": os.getenv("RUNTIME_ENV", "local"),
            "concurrency": concurrency,
            "persistence": persistence,
            "logging": log_cfg,
            "features": features,
        }
        if overrides:
            init_dict.update(overrides)

        cfg = cls(**init_dict)
        cfg.validate()
        logger.debug("Config loaded from env: runtime_id=%s env=%s", cfg.runtime_id, cfg.environment)
        return cfg

    @classmethod
    def from_file(cls, path: Path) -> "RuntimeConfig":
        """Load configuration from a JSON file."""
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path) as f:
            raw: dict[str, Any] = json.load(f)

        def _path(d: dict, key: str) -> None:
            if key in d:
                d[key] = Path(d[key])

        conc_raw = raw.pop("concurrency", {})
        raw["concurrency"] = ConcurrencyConfig(**conc_raw)

        pers_raw = raw.pop("persistence", {})
        for k in ("data_dir", "db_path", "journal_dir", "artifact_dir", "checkpoint_dir"):
            _path(pers_raw, k)
        raw["persistence"] = PersistenceConfig(**pers_raw)

        log_raw = raw.pop("logging", {})
        _path(log_raw, "log_dir")
        raw["logging"] = LoggingConfig(**log_raw)

        raw["features"] = FeatureFlags(**raw.pop("features", {}))

        cfg = cls(**raw)
        cfg.validate()
        return cfg

    # -----------------------------------------------------------------------
    # Validation
    # -----------------------------------------------------------------------

    def validate(self) -> None:
        """Enforce configuration invariants. Raises ValueError on any violation."""
        errors: list[str] = []

        if self.environment not in VALID_ENVIRONMENTS:
            errors.append(f"Invalid environment: {self.environment!r}")
        if self.concurrency.max_concurrent_tasks < 1:
            errors.append("max_concurrent_tasks must be >= 1")
        if self.concurrency.worker_count < 1:
            errors.append("worker_count must be >= 1")
        if self.concurrency.task_timeout_seconds <= 0:
            errors.append("task_timeout_seconds must be > 0")
        if self.concurrency.shutdown_timeout_seconds <= 0:
            errors.append("shutdown_timeout_seconds must be > 0")
        if self.persistence.backend not in ("sqlite", "postgres"):
            errors.append(f"Unknown persistence backend: {self.persistence.backend!r}")
        if self.logging.level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            errors.append(f"Invalid log level: {self.logging.level!r}")

        if errors:
            raise ValueError("RuntimeConfig validation failed:\n  " + "\n  ".join(errors))
