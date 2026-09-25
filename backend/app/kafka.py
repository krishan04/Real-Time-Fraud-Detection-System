import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from . import config

logger = logging.getLogger("fraud.backend.kafka")


class AlertHub:
    """Fan-out broadcast of live fraud alerts to SSE subscribers.

    Bounded: each subscriber owns a small queue; if a slow/disconnected client
    lets the queue fill, the alert is intentionally discarded (live-only, no
    replay semantics). The hub drops messages when nobody is subscribed.
    """

    def __init__(self):
        self._subscribers: set[asyncio.Queue] = set()
        self.delivered = 0
        self.dropped = 0

    def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=config.SSE_QUEUE_SIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        self._subscribers.discard(queue)

    def publish(self, payload: dict):
        if not self._subscribers:
            self.dropped += 1
            return
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(payload)
                self.delivered += 1
            except asyncio.QueueFull:
                self.dropped += 1

    @property
    def stats(self) -> dict:
        return {
            "subscribers": len(self._subscribers),
            "delivered": self.delivered,
            "dropped": self.dropped,
        }


class AlertConsumer:
    """Application-lifetime aiokafka consumer for the fraud_alerts topic.

    Runs independently of any SSE connection; each alert is published to the
    hub, which fans it out to whichever clients are currently connected.
    """

    def __init__(self, hub: AlertHub):
        self._hub = hub
        self._task: asyncio.Task | None = None
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            config.TOPIC_ALERTS,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id=config.CONSUMER_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            request_timeout_ms=20000,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._run())
        logger.info(
            "fraud-alert consumer started (group=%s topic=%s)",
            config.CONSUMER_GROUP,
            config.TOPIC_ALERTS,
        )

    async def _run(self):
        try:
            async for message in self._consumer:
                try:
                    payload = json.loads(message.value.decode("utf-8"))
                except Exception:
                    logger.warning("skipping malformed alert message")
                    continue
                self._hub.publish(payload)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("alert consumer stopped unexpectedly")

    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None