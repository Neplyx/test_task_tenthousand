import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models import Order, OutboxEvent, Product


async def _create_product(client, name: str, price: str, stock: int) -> dict:
    response = await client.post(
        "/products",
        json={"name": name, "price": price, "stock": stock},
    )
    assert response.status_code == 201
    return response.json()


async def _create_order(client, customer_id, items, idempotency_key: str):
    return await client.post(
        "/orders",
        headers={"Idempotency-Key": idempotency_key},
        json={"customer_id": str(customer_id), "items": items},
    )


async def test_create_order_success(client):
    product = await _create_product(client, "Widget", "10.00", 5)
    key = str(uuid4())
    customer_id = str(uuid4())

    response = await _create_order(
        client,
        customer_id,
        [{"product_id": product["id"], "quantity": 2}],
        key,
    )

    assert response.status_code == 201
    data = response.json()
    assert data["customer_id"] == customer_id
    assert data["status"] == "created"
    assert data["idempotency_key"] == key
    assert len(data["items"]) == 1
    assert data["items"][0]["product_id"] == product["id"]
    assert data["items"][0]["quantity"] == 2


async def test_nonexistent_product(client):
    response = await _create_order(
        client,
        uuid4(),
        [{"product_id": str(uuid4()), "quantity": 1}],
        str(uuid4()),
    )

    assert response.status_code == 404


async def test_insufficient_stock(client):
    product = await _create_product(client, "Widget", "10.00", 1)

    response = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 2}],
        str(uuid4()),
    )

    assert response.status_code == 409


async def test_correct_decimal_total(client):
    product_a = await _create_product(client, "A", "10.10", 10)
    product_b = await _create_product(client, "B", "5.05", 10)

    response = await _create_order(
        client,
        uuid4(),
        [
            {"product_id": product_a["id"], "quantity": 2},
            {"product_id": product_b["id"], "quantity": 1},
        ],
        str(uuid4()),
    )

    assert response.status_code == 201
    assert Decimal(response.json()["total"]) == Decimal("25.25")


async def test_price_snapshot_stored(client, test_engine):
    product = await _create_product(client, "Widget", "9.99", 10)

    response = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 1}],
        str(uuid4()),
    )
    assert response.status_code == 201
    assert Decimal(response.json()["items"][0]["unit_price"]) == Decimal("9.99")

    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with session_factory() as session:
        db_product = await session.get(Product, UUID(product["id"]))
        db_product.price = Decimal("50.00")
        await session.commit()

    stored = await client.get(f"/orders/{response.json()['id']}")
    assert Decimal(stored.json()["items"][0]["unit_price"]) == Decimal("9.99")
    assert Decimal(stored.json()["total"]) == Decimal("9.99")


async def test_stock_decreases_after_order(client):
    product = await _create_product(client, "Widget", "10.00", 5)

    response = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 3}],
        str(uuid4()),
    )
    assert response.status_code == 201

    listed = await client.get("/products")
    remaining = next(p for p in listed.json() if p["id"] == product["id"])
    assert remaining["stock"] == 2


async def test_failed_order_does_not_decrease_stock(client):
    available = await _create_product(client, "In stock", "10.00", 5)
    scarce = await _create_product(client, "Scarce", "10.00", 1)

    response = await _create_order(
        client,
        uuid4(),
        [
            {"product_id": available["id"], "quantity": 1},
            {"product_id": scarce["id"], "quantity": 2},
        ],
        str(uuid4()),
    )
    assert response.status_code == 409

    listed = await client.get("/products")
    stocks = {p["id"]: p["stock"] for p in listed.json()}
    assert stocks[available["id"]] == 5
    assert stocks[scarce["id"]] == 1


async def test_outbox_event_created_with_order(client, test_engine):
    product = await _create_product(client, "Widget", "10.00", 5)
    response = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 1}],
        str(uuid4()),
    )
    assert response.status_code == 201
    order = response.json()

    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with session_factory() as session:
        result = await session.execute(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == UUID(order["id"]))
        )
        events = list(result.scalars().all())

    assert len(events) == 1
    event = events[0]
    assert event.event_type == "order.created"
    assert event.status == "pending"
    assert event.payload["event_id"] == str(event.id)
    assert event.payload["order_id"] == order["id"]
    assert event.payload["total"] == order["total"]
    assert isinstance(event.payload["total"], str)


async def test_duplicate_idempotency_key(client):
    product = await _create_product(client, "Widget", "10.00", 5)
    key = str(uuid4())
    payload = [{"product_id": product["id"], "quantity": 1}]
    customer_id = uuid4()

    first = await _create_order(client, customer_id, payload, key)
    second = await _create_order(client, customer_id, payload, key)

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]

    listed = await client.get("/orders")
    assert len(listed.json()) == 1

    products = await client.get("/products")
    remaining = next(p for p in products.json() if p["id"] == product["id"])
    assert remaining["stock"] == 4


async def test_get_order_by_id(client):
    product = await _create_product(client, "Widget", "10.00", 5)
    created = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 1}],
        str(uuid4()),
    )
    order_id = created.json()["id"]

    response = await client.get(f"/orders/{order_id}")
    assert response.status_code == 200
    assert response.json()["id"] == order_id
    assert len(response.json()["items"]) == 1

    missing = await client.get(f"/orders/{uuid4()}")
    assert missing.status_code == 404


async def test_list_orders(client):
    product = await _create_product(client, "Widget", "10.00", 10)

    first = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 1}],
        str(uuid4()),
    )
    second = await _create_order(
        client,
        uuid4(),
        [{"product_id": product["id"], "quantity": 1}],
        str(uuid4()),
    )

    response = await client.get("/orders")
    assert response.status_code == 200
    orders = response.json()
    assert {order["id"] for order in orders} == {first.json()["id"], second.json()["id"]}
    assert all(order["items"] for order in orders)


async def test_concurrent_orders_for_last_item(client, test_engine):
    product = await _create_product(client, "Last item", "10.00", 1)
    payload = [{"product_id": product["id"], "quantity": 1}]

    first, second = await asyncio.gather(
        _create_order(client, uuid4(), payload, str(uuid4())),
        _create_order(client, uuid4(), payload, str(uuid4())),
    )

    statuses = sorted([first.status_code, second.status_code])
    assert statuses == [201, 409]

    products = await client.get("/products")
    remaining = next(p for p in products.json() if p["id"] == product["id"])
    assert remaining["stock"] == 0

    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(Order))
    assert count == 1


async def test_concurrent_duplicate_idempotency_key_returns_same_order(client, test_engine):
    product = await _create_product(client, "Only one", "10.00", 1)
    key = str(uuid4())
    customer_id = uuid4()
    payload = [{"product_id": product["id"], "quantity": 1}]

    first, second = await asyncio.gather(
        _create_order(client, customer_id, payload, key),
        _create_order(client, customer_id, payload, key),
    )

    assert sorted([first.status_code, second.status_code]) == [200, 201]
    assert first.json()["id"] == second.json()["id"]

    products = await client.get("/products")
    remaining = next(p for p in products.json() if p["id"] == product["id"])
    assert remaining["stock"] == 0

    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(Order))
    assert count == 1
