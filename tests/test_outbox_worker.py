import json
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models import OutboxEvent
from app.outbox_worker import process_pending_events


def _payload(event_id, order_id) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "order.created",
        "order_id": str(order_id),
        "customer_id": str(uuid4()),
        "total": "10.00",
        "items": [{"product_id": str(uuid4()), "quantity": 1, "unit_price": "10.00"}],
    }


async def _insert_pending(session_factory, event_id, order_id) -> None:
    async with session_factory() as session:
        session.add(
            OutboxEvent(
                id=event_id,
                event_type="order.created",
                aggregate_id=order_id,
                payload=_payload(event_id, order_id),
                status="pending",
            )
        )
        await session.commit()


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, dict]] = []

    async def send_and_wait(self, topic, key=None, value=None):
        self.messages.append((topic, key, json.loads(value)))


class FailingProducer:
    async def send_and_wait(self, topic, key=None, value=None):
        raise RuntimeError("kafka unavailable")


class FailOnceProducer:
    def __init__(self) -> None:
        self.event_ids: list[str] = []
        self._failed = False

    async def send_and_wait(self, topic, key=None, value=None):
        payload = json.loads(value)
        self.event_ids.append(payload["event_id"])
        if not self._failed:
            self._failed = True
            raise RuntimeError("kafka unavailable")


async def test_successful_publish(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    event_id = uuid4()
    order_id = uuid4()
    await _insert_pending(factory, event_id, order_id)

    producer = FakeProducer()
    async with factory() as session:
        published = await process_pending_events(session, producer, max_attempts=1)

    assert published == 1
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
    assert event.status == "published"
    assert event.published_at is not None
    assert producer.messages[0][0] == "orders.events"
    assert producer.messages[0][1] == str(order_id).encode("utf-8")
    assert producer.messages[0][2]["event_id"] == str(event_id)
    assert producer.messages[0][2]["event_type"] == "order.created"
    assert producer.messages[0][2]["order_id"] == str(order_id)
    assert "customer_id" in producer.messages[0][2]
    assert producer.messages[0][2]["total"] == "10.00"
    assert isinstance(producer.messages[0][2]["items"], list)


async def test_publish_failure_keeps_event_pending(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    event_id = uuid4()
    await _insert_pending(factory, event_id, uuid4())

    async with factory() as session:
        published = await process_pending_events(
            session, FailingProducer(), max_attempts=1
        )

    assert published == 0
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
    assert event.status == "pending"
    assert event.published_at is None
    assert event.retry_count == 1


async def test_retry_publishes_stable_event_id(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    event_id = uuid4()
    order_id = uuid4()
    await _insert_pending(factory, event_id, order_id)

    producer = FailOnceProducer()
    async with factory() as session:
        await process_pending_events(session, producer, max_attempts=1)
    async with factory() as session:
        await process_pending_events(session, producer, max_attempts=1)

    assert producer.event_ids == [str(event_id), str(event_id)]
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
    assert event.status == "published"


class FailForKeyProducer:
    def __init__(self, fail_key: bytes) -> None:
        self.fail_key = fail_key
        self.messages: list[tuple[str, bytes, dict]] = []

    async def send_and_wait(self, topic, key=None, value=None):
        if key == self.fail_key:
            raise RuntimeError("kafka unavailable")
        self.messages.append((topic, key, json.loads(value)))


async def test_failed_event_does_not_block_later_events(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    first_id, first_order = uuid4(), uuid4()
    second_id, second_order = uuid4(), uuid4()
    await _insert_pending(factory, first_id, first_order)
    await _insert_pending(factory, second_id, second_order)

    producer = FailForKeyProducer(str(first_order).encode("utf-8"))
    async with factory() as session:
        published = await process_pending_events(session, producer, max_attempts=1)

    assert published == 1
    async with factory() as session:
        first = await session.get(OutboxEvent, first_id)
        second = await session.get(OutboxEvent, second_id)
    assert first.status == "pending"
    assert first.retry_count == 1
    assert second.status == "published"
    assert second.published_at is not None
    assert producer.messages[0][2]["event_id"] == str(second_id)


async def test_order_creation_writes_pending_outbox(client, test_engine):
    product = await client.post(
        "/products",
        json={"name": "Widget", "price": "10.00", "stock": 5},
    )
    assert product.status_code == 201

    order = await client.post(
        "/orders",
        headers={"Idempotency-Key": str(uuid4())},
        json={
            "customer_id": str(uuid4()),
            "items": [{"product_id": product.json()["id"], "quantity": 1}],
        },
    )
    assert order.status_code == 201

    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with factory() as session:
        result = await session.execute(select(OutboxEvent))
        events = list(result.scalars().all())

    assert len(events) == 1
    event = events[0]
    assert event.status == "pending"
    assert event.payload["event_id"] == str(event.id)
    assert event.payload["order_id"] == order.json()["id"]
    assert event.aggregate_id is not None
