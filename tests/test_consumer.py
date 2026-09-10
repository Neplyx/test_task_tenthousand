from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.consumer import process_order_created_event
from app.models import Notification


def _event(event_id, order_id) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "order.created",
        "order_id": str(order_id),
        "customer_id": str(uuid4()),
        "total": "10.00",
        "items": [
            {
                "product_id": str(uuid4()),
                "quantity": 1,
                "unit_price": "10.00",
            }
        ],
    }


async def test_order_created_writes_notification(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    event_id = uuid4()
    order_id = uuid4()
    payload = _event(event_id, order_id)

    async with factory() as session:
        created = await process_order_created_event(session, payload)

    assert created is True

    async with factory() as session:
        result = await session.execute(
            select(Notification).where(Notification.event_id == event_id)
        )
        notification = result.scalar_one()

    assert notification.order_id == order_id
    assert notification.payload["event_id"] == str(event_id)
    assert notification.payload["event_type"] == "order.created"


async def test_duplicate_kafka_event_creates_one_notification(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    event_id = uuid4()
    payload = _event(event_id, uuid4())

    async with factory() as session:
        first = await process_order_created_event(session, payload)
    async with factory() as session:
        second = await process_order_created_event(session, payload)

    assert first is True
    assert second is False

    async with factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(Notification.event_id == event_id)
        )

    assert count == 1
