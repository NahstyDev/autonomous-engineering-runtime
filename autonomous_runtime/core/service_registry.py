"""
service_registry.py — Service Registry (Step 1.4)

Central registry for all named runtime services.
Services are registered by name and interface, then resolved by name.

Design:
- Services are typed via Protocol / base class (Phase 1: duck-typed via name).
- Registry enforces unique names — duplicate registration is an error.
- Supports optional lifecycle hooks: start() / stop() per service.
- Ordered shutdown: services are stopped in LIFO registration order.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, TypeVar, runtime_checkable

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Service protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class LifecycleService(Protocol):
    """
    Optional protocol for services that have explicit start/stop lifecycle.
    Services implementing this protocol will be started and stopped by
    the registry during runtime initialization and shutdown.
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...


# ---------------------------------------------------------------------------
# Registration record
# ---------------------------------------------------------------------------

@dataclass
class ServiceRegistration:
    """Metadata for a registered service."""
    name: str
    service: Any
    interface: type | None     # Expected type/Protocol (for validation)
    registered_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    started: bool = False
    stopped: bool = False
    tags: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Registry errors
# ---------------------------------------------------------------------------

class ServiceNotFoundError(KeyError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Service not registered: {name!r}")
        self.name = name


class DuplicateServiceError(ValueError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Service already registered: {name!r}")
        self.name = name


class ServiceTypeError(TypeError):
    pass


# ---------------------------------------------------------------------------
# Service registry
# ---------------------------------------------------------------------------

class ServiceRegistry:
    """
    Typed, named service registry with lifecycle coordination.

    Usage:
        registry.register("event_bus", event_bus_instance, EventBus)
        bus = registry.resolve("event_bus", EventBus)

    Lifecycle:
        await registry.start_all()   # calls .start() on LifecycleServices
        await registry.stop_all()    # calls .stop() in LIFO order
    """

    def __init__(self) -> None:
        self._services: dict[str, ServiceRegistration] = {}
        self._registration_order: list[str] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        service: Any,
        interface: type | None = None,
        tags: list[str] | None = None,
        allow_override: bool = False,
    ) -> None:
        """
        Register a service by name.

        Args:
            name:           Unique service identifier.
            service:        The service instance.
            interface:      Expected type/Protocol for type validation.
            tags:           Optional categorization tags.
            allow_override: If True, replaces an existing registration (use sparingly).

        Raises:
            DuplicateServiceError: If name is already registered and allow_override=False.
            ServiceTypeError:      If service does not satisfy the interface.
        """
        if name in self._services and not allow_override:
            raise DuplicateServiceError(name)

        if interface is not None and not isinstance(service, interface):
            raise ServiceTypeError(
                f"Service {name!r} does not satisfy interface {interface.__name__!r}. "
                f"Got: {type(service).__name__!r}"
            )

        reg = ServiceRegistration(
            name=name,
            service=service,
            interface=interface,
            tags=tags or [],
        )
        self._services[name] = reg
        if name not in self._registration_order:
            self._registration_order.append(name)

        logger.debug("Service registered: %s type=%s", name, type(service).__name__)

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, name: str, expected_type: type[T] | None = None) -> T:
        """
        Resolve a service by name, with optional type assertion.

        Raises:
            ServiceNotFoundError: If name is not registered.
            ServiceTypeError:     If resolved service doesn't match expected_type.
        """
        reg = self._services.get(name)
        if reg is None:
            raise ServiceNotFoundError(name)

        if expected_type is not None and not isinstance(reg.service, expected_type):
            raise ServiceTypeError(
                f"Service {name!r} resolved to {type(reg.service).__name__!r}, "
                f"expected {expected_type.__name__!r}"
            )

        return reg.service  # type: ignore[return-value]

    def resolve_optional(self, name: str, expected_type: type[T] | None = None) -> T | None:
        """Like resolve(), but returns None if not found instead of raising."""
        try:
            return self.resolve(name, expected_type)
        except ServiceNotFoundError:
            return None

    def has(self, name: str) -> bool:
        return name in self._services

    # ------------------------------------------------------------------
    # Lifecycle coordination
    # ------------------------------------------------------------------

    async def start_all(self) -> None:
        """
        Start all registered LifecycleService instances in registration order.
        Errors during start are logged and re-raised immediately.
        """
        logger.info("ServiceRegistry: starting %d services", len(self._registration_order))
        for name in self._registration_order:
            reg = self._services[name]
            if isinstance(reg.service, LifecycleService) and not reg.started:
                logger.debug("Starting service: %s", name)
                try:
                    await reg.service.start()
                    reg.started = True
                    logger.info("Service started: %s", name)
                except Exception as e:
                    logger.exception("Service failed to start: %s", name)
                    raise RuntimeError(f"Service {name!r} failed to start: {e}") from e

    async def stop_all(self) -> None:
        """
        Stop all LifecycleService instances in LIFO (reverse registration) order.
        Errors during stop are logged but do not prevent other services from stopping.
        """
        logger.info("ServiceRegistry: stopping services (LIFO)")
        for name in reversed(self._registration_order):
            reg = self._services.get(name)
            if reg is None:
                continue
            if isinstance(reg.service, LifecycleService) and reg.started and not reg.stopped:
                logger.debug("Stopping service: %s", name)
                try:
                    await reg.service.stop()
                    reg.stopped = True
                    logger.info("Service stopped: %s", name)
                except Exception:
                    logger.exception("Service failed to stop cleanly: %s", name)
                    # Continue stopping other services

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def registered_names(self) -> list[str]:
        return list(self._registration_order)

    def by_tag(self, tag: str) -> list[Any]:
        return [
            reg.service
            for reg in self._services.values()
            if tag in reg.tags
        ]

    def summary(self) -> list[dict]:
        return [
            {
                "name": reg.name,
                "type": type(reg.service).__name__,
                "tags": reg.tags,
                "has_lifecycle": isinstance(reg.service, LifecycleService),
                "started": reg.started,
                "stopped": reg.stopped,
                "registered_at": reg.registered_at.isoformat(),
            }
            for reg in self._services.values()
        ]

    def __len__(self) -> int:
        return len(self._services)
