"""
event_bus.py — Runtime Event Bus (Step 1.7)

Typed async publish/subscribe infrastructure.

Design:
- Events are typed dataclasses (RuntimeEvent subclasses).
- Subscribers register per event type (class-based routing).
- Delivery is async — handlers are awaited in registration order.
- Dead-letter queue captures undeliverable events.
- No global state — EventBus is injected via ServiceRegistry.
- Replay-safe: events carry sequence IDs and timestamps.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Type, TypeVar

logger = logging.getLogger(__name__)

E = TypeVar("E", bound="RuntimeEvent")
EventHandler = Callable[["RuntimeEvent"], Awaitable[None]]


# ---------------------------------------------------------------------------
# Base event
# ---------------------------------------------------------------------------

@dataclass
class RuntimeEvent:
    """
    Base class for all runtime events.

    Every event carries a unique ID, a correlation ID (for tracing chains
    of related events), and an immutable timestamp.
    """
    event_id: str = field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:10]}")
    correlation_id: str | None = None
    emitted_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    sequence: int = 0        # Set by the bus on publish
    source: str = "runtime"  # Emitting subsystem name

    @property
    def event_type(self) -> str:
        return self.__class__.__name__

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "event_id": self.event_id,
            "correlation_id": self.correlation_id,
            "emitted_at": self.emitted_at.isoformat(),
            "sequence": self.sequence,
            "source": self.source,
        }


# ---------------------------------------------------------------------------
# Built-in runtime events
# ---------------------------------------------------------------------------

@dataclass
class RuntimePhaseChangedEvent(RuntimeEvent):
    from_phase: str = ""
    to_phase: str = ""
    reason: str = ""
    source: str = "lifecycle_supervisor"


@dataclass
class TaskSubmittedEvent(RuntimeEvent):
    task_id: str = ""
    task_name: str = ""
    source: str = "worker_queue"


@dataclass
class TaskCompletedEvent(RuntimeEvent):
    task_id: str = ""
    task_name: str = ""
    duration_seconds: float = 0.0
    source: str = "worker_queue"


@dataclass
class TaskFailedEvent(RuntimeEvent):
    task_id: str = ""
    task_name: str = ""
    error: str = ""
    source: str = "worker_queue"


@dataclass
class ShutdownRequestedEvent(RuntimeEvent):
    reason: str = ""
    source: str = "shutdown_manager"


@dataclass
class WorkflowScheduledEvent(RuntimeEvent):
    workflow_id: str = ""
    workflow_name: str = ""
    source: str = "workflow_scheduler"


@dataclass
class ExecutionPhaseChangedEvent(RuntimeEvent):
    execution_id: str = ""
    from_phase: str = ""
    to_phase: str = ""
    source: str = "execution_cycle"


# ---------------------------------------------------------------------------
# Dead letter record
# ---------------------------------------------------------------------------

@dataclass
class DeadLetterEntry:
    event: RuntimeEvent
    handler_name: str
    error: str
    recorded_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))


# ---------------------------------------------------------------------------
# Subscription
# ---------------------------------------------------------------------------

@dataclass
class Subscription:
    subscription_id: str
    event_type: type[RuntimeEvent]
    handler: EventHandler
    name: str  # For diagnostics
    is_active: bool = True


# ---------------------------------------------------------------------------
# Event bus
# ---------------------------------------------------------------------------

class EventBus:
    """
    Typed async event bus.

    Usage:
        bus = EventBus()
        await bus.start()

        # Subscribe
        async def on_phase_change(event: RuntimeEvent) -> None:
            print(event)

        bus.subscribe(RuntimePhaseChangedEvent, on_phase_change, name="logger")

        # Publish
        await bus.publish(RuntimePhaseChangedEvent(from_phase="ready", to_phase="running"))

        await bus.stop()
    """

    MAX_DEAD_LETTER_SIZE = 1000

    def __init__(self) -> None:
        self._subscriptions: dict[type[RuntimeEvent], list[Subscription]] = {}
        self._dead_letters: list[DeadLetterEntry] = []
        self._sequence: int = 0
        self._lock = asyncio.Lock()
        self._started = False
        self._stopped = False
        self._publish_count = 0
        self._error_count = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._started = True
        logger.info("EventBus started")

    async def stop(self) -> None:
        self._stopped = True
        logger.info(
            "EventBus stopped. published=%d errors=%d dead_letters=%d",
            self._publish_count, self._error_count, len(self._dead_letters),
        )

    # ------------------------------------------------------------------
    # Subscription
    # ------------------------------------------------------------------

    def subscribe(
        self,
        event_type: type[E],
        handler: Callable[[E], Awaitable[None]],
        name: str = "",
    ) -> str:
        """
        Register an async handler for an event type.

        Returns a subscription_id that can be used to unsubscribe.
        """
        sub_id = f"sub-{uuid.uuid4().hex[:8]}"
        sub = Subscription(
            subscription_id=sub_id,
            event_type=event_type,
            handler=handler,  # type: ignore[arg-type]
            name=name or handler.__name__,
        )
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        self._subscriptions[event_type].append(sub)
        logger.debug(
            "EventBus: subscribed %r to %s id=%s",
            sub.name, event_type.__name__, sub_id,
        )
        return sub_id

    def subscribe_all(
        self,
        handler: Callable[[RuntimeEvent], Awaitable[None]],
        name: str = "",
    ) -> str:
        """Subscribe to ALL event types via the RuntimeEvent base class."""
        return self.subscribe(RuntimeEvent, handler, name=name)  # type: ignore

    def unsubscribe(self, subscription_id: str) -> bool:
        """Deactivate a subscription by ID. Returns True if found."""
        for subs in self._subscriptions.values():
            for sub in subs:
                if sub.subscription_id == subscription_id:
                    sub.is_active = False
                    logger.debug("EventBus: unsubscribed %s", subscription_id)
                    return True
        return False

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    async def publish(self, event: RuntimeEvent) -> int:
        """
        Publish an event to all matching subscribers.

        Delivery is sequential — handlers are awaited in registration order.
        Handler exceptions are caught, logged, and sent to the dead letter queue.

        Returns the number of handlers that received the event.
        """
        if not self._started:
            raise RuntimeError("EventBus.publish() called before start()")

        async with self._lock:
            self._sequence += 1
            event.sequence = self._sequence
        self._publish_count += 1

        handlers = self._collect_handlers(type(event))
        delivered = 0

        for sub in handlers:
            if not sub.is_active:
                continue
            try:
                await sub.handler(event)
                delivered += 1
            except Exception as exc:
                self._error_count += 1
                error_str = f"{type(exc).__name__}: {exc}"
                logger.exception(
                    "EventBus handler %r failed for %s: %s",
                    sub.name, event.event_type, error_str,
                )
                self._record_dead_letter(event, sub.name, error_str)

        logger.debug(
            "EventBus: published %s seq=%d delivered=%d/%d",
            event.event_type, event.sequence, delivered, len(handlers),
        )
        return delivered

    async def publish_nowait(self, event: RuntimeEvent) -> None:
        """Fire-and-forget publish. Exceptions are only logged."""
        try:
            await self.publish(event)
        except Exception:
            logger.exception("EventBus.publish_nowait failed for %s", event.event_type)

    # ------------------------------------------------------------------
    # Dead letter
    # ------------------------------------------------------------------

    @property
    def dead_letters(self) -> list[DeadLetterEntry]:
        return list(self._dead_letters)

    def clear_dead_letters(self) -> int:
        count = len(self._dead_letters)
        self._dead_letters.clear()
        return count

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def subscription_count(self) -> int:
        return sum(len(subs) for subs in self._subscriptions.values())

    def summary(self) -> dict[str, Any]:
        return {
            "started": self._started,
            "stopped": self._stopped,
            "publish_count": self._publish_count,
            "error_count": self._error_count,
            "dead_letters": len(self._dead_letters),
            "subscribed_event_types": [t.__name__ for t in self._subscriptions],
            "total_subscriptions": self.subscription_count(),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _collect_handlers(self, event_type: type[RuntimeEvent]) -> list[Subscription]:
        """
        Collect handlers for the given event type using MRO-based matching.
        RuntimeEvent base class catches all.
        """
        handlers: list[Subscription] = []
        for registered_type, subs in self._subscriptions.items():
            if issubclass(event_type, registered_type):
                handlers.extend(subs)
        return handlers

    def _record_dead_letter(self, event: RuntimeEvent, handler_name: str, error: str) -> None:
        if len(self._dead_letters) >= self.MAX_DEAD_LETTER_SIZE:
            self._dead_letters.pop(0)  # Evict oldest
        self._dead_letters.append(
            DeadLetterEntry(event=event, handler_name=handler_name, error=error)
        )
