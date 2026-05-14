"""
main.py — Runtime Entry Point

Boots the autonomous engineering runtime and runs until shutdown.

Usage:
    python -m autonomous_runtime                  # from env vars
    python -m autonomous_runtime --config cfg.json
    RUNTIME_LOG_LEVEL=DEBUG python -m autonomous_runtime
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="autonomous-runtime",
        description="Autonomous Engineering Runtime — Phase 1",
    )
    parser.add_argument(
        "--config", "-c",
        type=Path,
        default=None,
        help="Path to JSON config file (default: load from environment)",
    )
    parser.add_argument(
        "--name",
        type=str,
        default=None,
        help="Override runtime name",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override log level",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Override worker count",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Override data directory path",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> int:
    from autonomous_runtime.core.bootstrap import bootstrap, bootstrap_from_file

    # Build env overrides from CLI args
    overrides: dict[str, str] = {}
    if args.name:
        overrides["RUNTIME_NAME"] = args.name
    if args.log_level:
        overrides["RUNTIME_LOG_LEVEL"] = args.log_level
    if args.workers:
        overrides["RUNTIME_WORKER_COUNT"] = str(args.workers)
    if args.data_dir:
        overrides["RUNTIME_DATA_DIR"] = args.data_dir

    try:
        if args.config:
            manager = await bootstrap_from_file(args.config)
        else:
            manager = await bootstrap(env_overrides=overrides or None)
    except Exception as e:
        print(f"FATAL: Bootstrap failed: {e}", file=sys.stderr)
        return 1

    logger.info("Runtime operational. Waiting for shutdown signal (SIGINT/SIGTERM)...")

    try:
        await manager.wait_for_shutdown()
    except asyncio.CancelledError:
        logger.info("Shutdown cancellation received")
    finally:
        await manager.stop(reason="shutdown")

    logger.info("Runtime exited cleanly")
    return 0


def main() -> None:
    args = parse_args()
    try:
        exit_code = asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\n[INFO] Shutdown signal received.")
        exit_code = 0

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
