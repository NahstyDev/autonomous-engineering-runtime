"""
environment_manager.py — Environment Manager (Step 1.3)

Responsible for:
  - Loading and validating environment variables
  - Provisioning required filesystem directories
  - Providing a clean, validated view of the runtime environment
  - Detecting environment anomalies before bootstrap proceeds

This runs before any other subsystem so that config.from_env() can rely
on a known-good environment state.
"""
from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Environment validation results
# ---------------------------------------------------------------------------

class EnvironmentError(RuntimeError):
    """Raised when the environment fails validation."""
    pass


# ---------------------------------------------------------------------------
# Environment manager
# ---------------------------------------------------------------------------

class EnvironmentManager:
    """
    Validates and prepares the execution environment.

    Responsibilities:
      1. Assert required env vars are present.
      2. Validate env var formats/ranges.
      3. Provision filesystem directories with correct permissions.
      4. Provide a snapshot of resolved env state for diagnostics.
    """

    # Env vars that must exist (with description for error messages)
    REQUIRED_VARS: dict[str, str] = {}

    # Env vars that are validated if present (key → validator callable)
    VALIDATED_VARS: dict[str, Any] = {
        "RUNTIME_WORKER_COUNT":         lambda v: int(v) >= 1,
        "RUNTIME_MAX_CONCURRENT_TASKS": lambda v: int(v) >= 1,
        "RUNTIME_TASK_TIMEOUT":         lambda v: float(v) > 0,
        "RUNTIME_SHUTDOWN_TIMEOUT":     lambda v: float(v) > 0,
        "RUNTIME_ENV":                  lambda v: v in ("local", "staging", "production", "container"),
        "RUNTIME_LOG_LEVEL":            lambda v: v in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        "RUNTIME_PERSISTENCE_BACKEND":  lambda v: v in ("sqlite", "postgres"),
    }

    def __init__(self, config_overrides: dict[str, str] | None = None) -> None:
        """
        Args:
            config_overrides: Optional dict of env key→value overrides
                              applied before validation (useful for testing).
        """
        self._overrides = config_overrides or {}
        self._resolved: dict[str, str] = {}
        self._provisioned_dirs: list[Path] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def prepare(self) -> None:
        """
        Run full environment preparation sequence.
        Raises EnvironmentError on any failure.
        """
        logger.info("EnvironmentManager: preparing runtime environment")
        self._apply_overrides()
        self._validate_required()
        self._validate_present()
        self._resolve_snapshot()
        logger.info("EnvironmentManager: environment validated successfully")

    def provision_directories(self, *paths: Path) -> None:
        """
        Create directories required by the runtime.
        Sets restrictive permissions (700) for data directories.
        """
        for path in paths:
            if path.exists():
                if not path.is_dir():
                    raise EnvironmentError(f"Path exists but is not a directory: {path}")
                logger.debug("Directory already exists: %s", path)
            else:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    # Restrictive permissions for data directories
                    if any(name in str(path) for name in ("runtime_data", "logs", "journal", "checkpoint")):
                        path.chmod(stat.S_IRWXU)
                    logger.info("Directory provisioned: %s", path)
                    self._provisioned_dirs.append(path)
                except OSError as e:
                    raise EnvironmentError(f"Failed to create directory {path}: {e}") from e

    def get(self, key: str, default: str | None = None) -> str | None:
        """Retrieve a resolved env value."""
        return self._resolved.get(key, os.getenv(key, default))

    def require(self, key: str) -> str:
        """Retrieve a required env value. Raises if absent."""
        value = self.get(key)
        if value is None:
            raise EnvironmentError(f"Required environment variable not set: {key!r}")
        return value

    @property
    def provisioned_dirs(self) -> list[Path]:
        return list(self._provisioned_dirs)

    def snapshot(self) -> dict[str, str]:
        """Return a copy of the resolved environment snapshot."""
        return dict(self._resolved)

    def diagnostics(self) -> dict[str, Any]:
        return {
            "resolved_vars": len(self._resolved),
            "provisioned_dirs": [str(p) for p in self._provisioned_dirs],
            "overrides_applied": list(self._overrides.keys()),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_overrides(self) -> None:
        for key, value in self._overrides.items():
            os.environ[key] = value
            logger.debug("Env override applied: %s", key)

    def _validate_required(self) -> None:
        missing = [
            f"{key} ({description})"
            for key, description in self.REQUIRED_VARS.items()
            if not os.getenv(key)
        ]
        if missing:
            raise EnvironmentError(
                "Missing required environment variables:\n  " + "\n  ".join(missing)
            )

    def _validate_present(self) -> None:
        errors: list[str] = []
        for key, validator in self.VALIDATED_VARS.items():
            value = os.getenv(key)
            if value is None:
                continue  # Optional — skip absent vars
            try:
                if not validator(value):
                    errors.append(f"{key}={value!r} failed validation")
            except (ValueError, TypeError) as e:
                errors.append(f"{key}={value!r} raised {e}")

        if errors:
            raise EnvironmentError(
                "Environment validation errors:\n  " + "\n  ".join(errors)
            )

    def _resolve_snapshot(self) -> None:
        """Capture a snapshot of all RUNTIME_* vars for diagnostics."""
        self._resolved = {
            key: value
            for key, value in os.environ.items()
            if key.startswith("RUNTIME_")
        }
