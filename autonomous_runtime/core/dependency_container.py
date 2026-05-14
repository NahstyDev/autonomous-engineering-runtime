"""
dependency_container.py — Dependency Injection Container (Step 1.4)

A lightweight IoC container supporting three binding modes:
  - SINGLETON:  One instance for the lifetime of the container.
  - TRANSIENT:  New instance on every resolve() call.
  - FACTORY:    Caller-supplied callable invoked on each resolve().

Design principles:
- No magic. All bindings are explicit.
- Circular dependency detection via resolution stack.
- Thread-safe singleton creation (double-checked locking pattern).
- Supports both class-based and factory-based registration.
- Integrates with ServiceRegistry for named service lookup.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Binding modes
# ---------------------------------------------------------------------------

class BindingMode(Enum):
    SINGLETON  = auto()
    TRANSIENT  = auto()
    FACTORY    = auto()


# ---------------------------------------------------------------------------
# Binding record
# ---------------------------------------------------------------------------

@dataclass
class Binding(Generic[T]):
    key: type[T] | str
    mode: BindingMode
    factory: Callable[[], T]
    _instance: T | None = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def resolve(self) -> T:
        if self.mode == BindingMode.SINGLETON:
            if self._instance is None:
                with self._lock:
                    if self._instance is None:  # double-checked
                        self._instance = self.factory()
            return self._instance  # type: ignore[return-value]

        elif self.mode in (BindingMode.TRANSIENT, BindingMode.FACTORY):
            return self.factory()

        raise ValueError(f"Unknown binding mode: {self.mode}")


# ---------------------------------------------------------------------------
# Container errors
# ---------------------------------------------------------------------------

class DependencyNotFoundError(KeyError):
    def __init__(self, key: Any) -> None:
        super().__init__(f"No binding for: {key!r}")


class CircularDependencyError(RuntimeError):
    pass


class DuplicateBindingError(ValueError):
    def __init__(self, key: Any) -> None:
        super().__init__(f"Binding already registered for: {key!r}")


# ---------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------

class DependencyContainer:
    """
    Lightweight IoC container.

    Example:
        container = DependencyContainer()
        container.bind_singleton(EventBus, lambda: EventBus())
        bus = container.resolve(EventBus)

    Supports string keys for loose binding:
        container.bind_singleton("config", lambda: load_config())
        cfg = container.resolve("config")
    """

    def __init__(self, parent: "DependencyContainer | None" = None) -> None:
        """
        Args:
            parent: Optional parent container for hierarchical resolution.
                    Child containers check local bindings first, then parent.
        """
        self._bindings: dict[Any, Binding] = {}
        self._parent = parent
        self._resolution_stack: threading.local = threading.local()
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def bind_singleton(
        self,
        key: type[T] | str,
        factory: Callable[[], T],
        allow_override: bool = False,
    ) -> "DependencyContainer":
        """Bind key to a singleton (factory called once, result cached)."""
        self._bind(key, BindingMode.SINGLETON, factory, allow_override)
        return self  # fluent

    def bind_transient(
        self,
        key: type[T] | str,
        factory: Callable[[], T],
        allow_override: bool = False,
    ) -> "DependencyContainer":
        """Bind key to a transient (factory called on every resolve)."""
        self._bind(key, BindingMode.TRANSIENT, factory, allow_override)
        return self

    def bind_factory(
        self,
        key: type[T] | str,
        factory: Callable[[], T],
        allow_override: bool = False,
    ) -> "DependencyContainer":
        """Explicit factory binding (semantically identical to transient, clearer intent)."""
        self._bind(key, BindingMode.FACTORY, factory, allow_override)
        return self

    def bind_instance(
        self,
        key: type[T] | str,
        instance: T,
        allow_override: bool = False,
    ) -> "DependencyContainer":
        """Bind key to an already-constructed instance (effectively a pre-seeded singleton)."""
        self._bind(key, BindingMode.SINGLETON, lambda: instance, allow_override)
        # Pre-seed the instance so the lambda is never called
        binding = self._bindings[key]
        binding._instance = instance
        return self

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, key: type[T] | str) -> T:
        """
        Resolve a dependency.

        Checks local bindings first, then parent container (if any).
        Raises DependencyNotFoundError if no binding exists anywhere.
        Raises CircularDependencyError if a cycle is detected.
        """
        # Circular dependency detection per-thread
        stack = self._get_resolution_stack()
        if key in stack:
            cycle = " → ".join(str(k) for k in stack) + f" → {key}"
            raise CircularDependencyError(f"Circular dependency detected: {cycle}")

        binding = self._bindings.get(key)
        if binding is None:
            if self._parent is not None:
                return self._parent.resolve(key)
            raise DependencyNotFoundError(key)

        stack.add(key)
        try:
            result = binding.resolve()
            logger.debug("Resolved: %s → %s", key, type(result).__name__)
            return result
        finally:
            stack.discard(key)

    def resolve_optional(self, key: type[T] | str) -> T | None:
        """Like resolve(), but returns None if not bound."""
        try:
            return self.resolve(key)
        except DependencyNotFoundError:
            return None

    def has(self, key: type | str) -> bool:
        """True if key is bound locally or in any parent container."""
        if key in self._bindings:
            return True
        if self._parent is not None:
            return self._parent.has(key)
        return False

    # ------------------------------------------------------------------
    # Child container
    # ------------------------------------------------------------------

    def create_child(self) -> "DependencyContainer":
        """
        Create a child container that inherits parent bindings.
        Child overrides shadow parent. Useful for request-scoped bindings.
        """
        return DependencyContainer(parent=self)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def registered_keys(self) -> list[Any]:
        with self._lock:
            return list(self._bindings.keys())

    def summary(self) -> list[dict]:
        with self._lock:
            rows = []
            for key, binding in self._bindings.items():
                rows.append({
                    "key": str(key),
                    "mode": binding.mode.name,
                    "has_instance": binding._instance is not None,
                })
            return rows

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _bind(
        self,
        key: Any,
        mode: BindingMode,
        factory: Callable,
        allow_override: bool,
    ) -> None:
        with self._lock:
            if key in self._bindings and not allow_override:
                raise DuplicateBindingError(key)
            self._bindings[key] = Binding(key=key, mode=mode, factory=factory)
            logger.debug("Bound: %s mode=%s", key, mode.name)

    def _get_resolution_stack(self) -> set:
        if not hasattr(self._resolution_stack, "stack"):
            self._resolution_stack.stack = set()
        return self._resolution_stack.stack
