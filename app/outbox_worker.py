import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_factory, engine
from app.models import OutboxEvent

logger = logging.getLogger(__name__)

BATCH_SIZE = 10
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 1.0


def _message_bytes(payload: dict) -> bytes:
    return json.dumps(payload, default=str).encode("utf-8")


async def process_pending_events(
    session: AsyncSession,
    producer,
    *,
    topic: str = settings.kafka_topic,
    batch_size: int = BATCH_SIZE,
    max_attempts: int = MAX_ATTEMPTS,
    backoff_base: float = BACKOFF_BASE_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    published = 0
    attempted_ids: list = []
    for _ in range(batch_size):
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == "pending")
            .order_by(OutboxEvent.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if attempted_ids:
            stmt = stmt.where(OutboxEvent.id.notin_(attempted_ids))
        result = await session.execute(stmt)
        event = result.scalar_one_or_none()
        if event is None:
            break
        attempted_ids.append(event.id)

        if await _publish_event(
            producer,
            event,
            topic=topic,
            max_attempts=max_attempts,
            backoff_base=backoff_base,
            sleep=sleep,
        ):
            event.status = "published"
            event.published_at = datetime.now(timezone.utc)
            published += 1
        else:
            event.retry_count += 1

        await session.commit()

    return published


async def _publish_event(
    producer,
    event: OutboxEvent,
    *,
    topic: str,
    max_attempts: int,
    backoff_base: float,
    sleep: Callable[[float], Awaitable[None]],
) -> bool:
    payload = _message_bytes(event.payload)
    key = str(event.aggregate_id).encode("utf-8")

    for attempt in range(1, max_attempts + 1):
        try:
            await producer.send_and_wait(topic, key=key, value=payload)
            return True
        except Exception:
            logger.exception(
                "Failed to publish outbox event %s (attempt %s/%s)",
                event.id,
                attempt,
                max_attempts,
            )
            if attempt < max_attempts:
                await sleep(backoff_base * (2 ** (attempt - 1)))
    return False


async def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    logger.info("Outbox worker started, topic=%s", settings.kafka_topic)
    try:
        while True:
            try:
                async with async_session_factory() as session:
                    published = await process_pending_events(session, producer)
                if published == 0:
                    await asyncio.sleep(settings.outbox_poll_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox worker iteration failed; will retry")
                await asyncio.sleep(settings.outbox_poll_interval_seconds)
    finally:
        await producer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_worker())
