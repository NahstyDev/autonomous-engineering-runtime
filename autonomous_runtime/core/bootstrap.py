"""
bootstrap.py — Runtime Bootstrap (Step 1.1)

Single entry point for runtime initialization.

Responsibilities:
  1. Configure structured logging.
  2. Load and validate RuntimeConfig.
  3. Validate and prepare the environment.
  4. Provision required filesystem directories.
  5. Construct and start the RuntimeManager.
  6. Return a fully operational RuntimeManager.

Design:
  - Bootstrap is a pure function — no global state modified.
  - All side effects are explicit (logging setup, dir creation).
  - Failures at any stage are fatal and logged with full context.
  - Returns a RuntimeManager that owns the full runtime lifecycle.
"""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .environment_manager import EnvironmentManager
from .runtime_manager import RuntimeManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def configure_logging(config: RuntimeConfig) -> None:
    """
    Configure structured logging for the runtime.

    Sets up:
      - Console handler (always enabled)
      - Rotating file handler (if enable_file_logging)
      - Log format with runtime_id, level, module, message
    """
    log_cfg = config.logging
    root = logging.getLogger()
    root.setLevel(log_cfg.level)

    # Remove any handlers already attached (e.g. from pytest)
    root.handlers.clear()

    fmt = logging.Formatter(
        fmt=(
            "%(asctime)s | %(levelname)-8s | "
            f"runtime={config.runtime_id} | "
            "%(name)s:%(lineno)d | %(message)s"
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(log_cfg.level)
    root.addHandler(console)

    # File handler
    if log_cfg.enable_file_logging:
        try:
            log_cfg.log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_cfg.log_dir / f"{config.runtime_name}.log"
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=log_cfg.max_bytes,
                backupCount=log_cfg.backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(fmt)
            file_handler.setLevel(log_cfg.level)
            root.addHandler(file_handler)
            logger.debug("File logging configured: %s", log_file)
        except OSError as e:
            logger.warning("Could not set up file logging: %s", e)

    logger.info(
        "Logging configured: level=%s file_logging=%s",
        log_cfg.level, log_cfg.enable_file_logging,
    )


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

async def bootstrap(
    config: RuntimeConfig | None = None,
    env_overrides: dict[str, str] | None = None,
    config_file: Path | None = None,
) -> RuntimeManager:
    """
    Full runtime bootstrap sequence.

    Args:
        config:        Pre-constructed RuntimeConfig (takes precedence).
        env_overrides: Optional env var overrides applied before config load.
        config_file:   Path to a JSON config file (used if config is None).

    Returns:
        A fully started RuntimeManager with all Phase 1 services operational.

    Raises:
        RuntimeError:  If any bootstrap stage fails.
        ValueError:    If config validation fails.
        OSError:       If filesystem provisioning fails.
    """
    # ------------------------------------------------------------------
    # Stage 1 — Environment preparation
    # ------------------------------------------------------------------
    print("[bootstrap] Stage 1: preparing environment")
    env_mgr = EnvironmentManager(config_overrides=env_overrides)
    try:
        env_mgr.prepare()
    except Exception as e:
        print(f"[bootstrap] FATAL: environment validation failed: {e}", file=sys.stderr)
        raise RuntimeError(f"Bootstrap failed at environment stage: {e}") from e

    # ------------------------------------------------------------------
    # Stage 2 — Configuration loading
    # ------------------------------------------------------------------
    print("[bootstrap] Stage 2: loading configuration")
    try:
        if config is not None:
            cfg = config
        elif config_file is not None:
            cfg = RuntimeConfig.from_file(config_file)
        else:
            cfg = RuntimeConfig.from_env()
    except Exception as e:
        print(f"[bootstrap] FATAL: config load failed: {e}", file=sys.stderr)
        raise RuntimeError(f"Bootstrap failed at config stage: {e}") from e

    # ------------------------------------------------------------------
    # Stage 3 — Logging
    # ------------------------------------------------------------------
    print("[bootstrap] Stage 3: configuring logging")
    configure_logging(cfg)
    logger.info("Bootstrap started: runtime_id=%s name=%r env=%s",
                cfg.runtime_id, cfg.runtime_name, cfg.environment)

    # ------------------------------------------------------------------
    # Stage 4 — Directory provisioning
    # ------------------------------------------------------------------
    logger.info("Bootstrap Stage 4: provisioning directories")
    try:
        env_mgr.provision_directories(
            cfg.persistence.data_dir,
            cfg.persistence.journal_dir,
            cfg.persistence.artifact_dir,
            cfg.persistence.checkpoint_dir,
            cfg.logging.log_dir,
        )
    except Exception as e:
        logger.critical("Bootstrap FATAL: directory provisioning failed: %s", e)
        raise RuntimeError(f"Bootstrap failed at directory stage: {e}") from e

    # ------------------------------------------------------------------
    # Stage 5 — Runtime manager construction and startup
    # ------------------------------------------------------------------
    logger.info("Bootstrap Stage 5: constructing runtime")
    manager = RuntimeManager(cfg)
    try:
        await manager.start()
    except Exception as e:
        logger.critical("Bootstrap FATAL: runtime start failed: %s", e, exc_info=True)
        raise RuntimeError(f"Bootstrap failed at runtime start: {e}") from e

    logger.info(
        "Bootstrap complete. runtime_id=%s phase=%s services=%d",
        cfg.runtime_id,
        manager.context.current_phase.value,
        len(manager.context.registry),
    )
    return manager


# ---------------------------------------------------------------------------
# Convenience: bootstrap from env (common case)
# ---------------------------------------------------------------------------

async def bootstrap_from_env(env_overrides: dict[str, str] | None = None) -> RuntimeManager:
    """Bootstrap from environment variables. Most common entry point."""
    return await bootstrap(env_overrides=env_overrides)


async def bootstrap_from_file(config_path: Path) -> RuntimeManager:
    """Bootstrap from a JSON config file."""
    return await bootstrap(config_file=config_path)


async def bootstrap_local(
    runtime_name: str = "autonomous-runtime",
    data_dir: str = "./runtime_data",
    log_level: str = "INFO",
    worker_count: int = 4,
    **extra_overrides: str,
) -> RuntimeManager:
    """
    Bootstrap with sensible local/development defaults.

    Convenience wrapper that sets common RUNTIME_* env vars
    before bootstrapping. Safe to call repeatedly in tests.
    """
    overrides: dict[str, str] = {
        "RUNTIME_NAME": runtime_name,
        "RUNTIME_ENV": "local",
        "RUNTIME_DATA_DIR": data_dir,
        "RUNTIME_LOG_LEVEL": log_level,
        "RUNTIME_WORKER_COUNT": str(worker_count),
        "RUNTIME_FILE_LOGGING": "false",  # default off for local dev
        **extra_overrides,
    }
    return await bootstrap(env_overrides=overrides)
