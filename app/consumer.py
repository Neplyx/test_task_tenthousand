import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import async_session_factory, engine
from app.models import Notification

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 1.0


def _decode_message(value: bytes) -> dict:
    payload = json.loads(value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Kafka event payload must be a JSON object")
    return payload


async def process_order_created_event(session: AsyncSession, payload: dict) -> bool:
    """Store one notification. Return True if it was inserted, False if duplicate."""
    if payload.get("event_type") != "order.created":
        return False

    event_id = UUID(payload["event_id"])
    order_id = UUID(payload["order_id"])

    stmt = (
        insert(Notification)
        .values(
            event_id=event_id,
            order_id=order_id,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=[Notification.event_id])
        .returning(Notification.id)
    )
    result = await session.execute(stmt)
    created = result.scalar_one_or_none() is not None
    await session.commit()
    return created


async def _process_with_retry(
    payload: dict,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    backoff_base: float = BACKOFF_BASE_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            async with async_session_factory() as session:
                await process_order_created_event(session, payload)
            return
        except Exception:
            logger.exception(
                "Failed to process Kafka event %s (attempt %s/%s)",
                payload.get("event_id"),
                attempt,
                max_attempts,
            )
            if attempt < max_attempts:
                await sleep(backoff_base * (2 ** (attempt - 1)))
    raise RuntimeError(f"Failed to process event after {max_attempts} attempts")


async def run_consumer() -> None:
    logging.basicConfig(level=logging.INFO)
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info(
        "Kafka consumer started, topic=%s group=%s",
        settings.kafka_topic,
        settings.kafka_consumer_group,
    )

    try:
        while True:
            message = await consumer.getone()
            tp = TopicPartition(message.topic, message.partition)

            try:
                payload = _decode_message(message.value)
                await _process_with_retry(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Kafka message processing failed; offset will not be committed "
                    "and the message will be retried"
                )
                consumer.seek(tp, message.offset)
                await asyncio.sleep(settings.kafka_consumer_retry_delay_seconds)
                continue

            # Commit only this successfully handled message, after the DB commit.
            await consumer.commit(
                {tp: OffsetAndMetadata(message.offset + 1, "")}
            )
    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_consumer())
