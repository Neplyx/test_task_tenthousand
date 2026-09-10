# Order Processing Service

A small asynchronous order-processing service built with FastAPI, SQLAlchemy 2.x, PostgreSQL and Kafka.

## Stack

- Python 3.12+
- FastAPI
- SQLAlchemy 2.x async + asyncpg
- PostgreSQL
- Kafka via aiokafka
- Alembic
- pytest + pytest-asyncio + Testcontainers
- Docker Compose

## Run

Requirements: Docker Desktop / Docker Engine with Docker Compose.

```bash
docker compose up --build
```

The API is available at `http://localhost:8000`.

Useful endpoints:

- `GET /health`
- `GET /health/db`
- `POST /products`
- `GET /products`
- `POST /orders`
- `GET /orders/{id}`
- `GET /orders`

`POST /orders` requires an `Idempotency-Key` header.

Example product request:

```json
{
  "name": "Keyboard",
  "price": "99.90",
  "stock": 5
}
```

Example order request:

```json
{
  "customer_id": "11111111-1111-1111-1111-111111111111",
  "items": [
    {
      "product_id": "22222222-2222-2222-2222-222222222222",
      "quantity": 2
    }
  ]
}
```

## Project structure

```text
app/
  main.py            FastAPI application and health endpoints
  config.py          environment-based settings
  database.py        async SQLAlchemy engine/session setup
  models.py          Product, Order, OrderItem, OutboxEvent, Notification
  schemas.py         Pydantic request/response models
  products.py        products API
  orders.py          orders API, transactions, locking, idempotency, outbox insert
  outbox_worker.py   publishes pending outbox events to Kafka
  consumer.py        consumes order.created and writes notifications
alembic/
  versions/001_initial.py
tests/
docker-compose.yml
Dockerfile
```

## Order transaction and stock concurrency

Order creation uses PostgreSQL row-level locking with `SELECT ... FOR UPDATE`.
All requested product rows are locked in deterministic product-ID order before stock is validated or changed. This prevents two concurrent orders from overselling the same stock and also reduces deadlock risk when one order contains multiple products.

The order transaction contains:

1. product row locks
2. product existence and stock validation
3. stock decrement
4. Order insert
5. OrderItem inserts with price snapshots
6. OutboxEvent insert
7. commit

If any operation fails, the PostgreSQL transaction is rolled back, so stock, the order and the outbox event cannot be partially persisted.

Money is represented as Python `Decimal` and PostgreSQL `NUMERIC(12, 2)`. No float arithmetic is used. `OrderItem.unit_price` stores the product price at order time so later product price changes do not change historical orders.

## Idempotency-Key

`orders.idempotency_key` has a database `UNIQUE` constraint.

A repeated request with the same key returns the already-created order instead of creating another one. The service checks for an existing order before work starts and re-checks after product locks are acquired, because another request with the same key may have committed while this request was waiting for the lock.

The database uniqueness constraint is the final protection for concurrent duplicate requests. If two requests still race to insert the same key, the losing transaction is rolled back and the already-created order is returned.

## Transactional Outbox and Kafka

The API never publishes to Kafka directly from `POST /orders`.

Instead, the `order.created` OutboxEvent is stored in the same PostgreSQL transaction as the order and stock update. A separate outbox worker reads pending events and publishes them to the `orders.events` Kafka topic.

The worker marks an event as `published` only after Kafka acknowledges the send. If Kafka is unavailable, the event stays pending and is retried with bounded exponential backoff. The worker uses `FOR UPDATE SKIP LOCKED`, allowing multiple workers to skip rows already being processed by another worker.

Delivery is intentionally **at-least-once**, not exactly-once. A worker can publish successfully and crash before marking the outbox row as published. After restart, the same event can therefore be published again.

The outbox row ID is the stable `event_id`, so retries publish the same event identity.

## Idempotent consumer

The Kafka consumer reads `order.created` events and writes a row to `notifications`.

`notifications.event_id` has a `UNIQUE` constraint and inserts use PostgreSQL `ON CONFLICT DO NOTHING`. Therefore duplicate Kafka deliveries of the same `event_id` create at most one notification.

Kafka auto-commit is disabled. The consumer commits the Kafka offset only after the database transaction succeeds. If database processing fails, the offset is not committed and Kafka can redeliver the message.

## Tests

Install development dependencies:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
```

The PostgreSQL-specific tests use Testcontainers and therefore require Docker to be running. They cover, among other cases:

- successful orders
- nonexistent products
- insufficient stock
- Decimal totals and price snapshots
- rollback behavior
- concurrent orders for the last item
- sequential and concurrent duplicate Idempotency-Key requests
- outbox creation with the order
- outbox retries and stable event IDs
- duplicate Kafka events creating only one notification

## Docker services

`docker compose up` starts:

- `postgres`
- `kafka`
- `api`
- `outbox-worker`
- `consumer`

Inside Docker, application services connect to `postgres:5432` and `kafka:9092` by Compose service name rather than `localhost`.

## Bonus features included

- `SELECT ... FOR UPDATE SKIP LOCKED` for outbox worker coordination
- bounded exponential retry/backoff for producer and consumer processing

A dead-letter topic is not implemented; it is optional bonus functionality and not required for the core delivery guarantees of this task.
