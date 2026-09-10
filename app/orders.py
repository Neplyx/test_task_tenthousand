import uuid
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models import Order, OrderItem, OutboxEvent, Product
from app.schemas import OrderCreate, OrderResponse

router = APIRouter(prefix="/orders", tags=["orders"])


def _merge_quantities(items: list) -> dict[UUID, int]:
    merged: dict[UUID, int] = {}
    for item in items:
        merged[item.product_id] = merged.get(item.product_id, 0) + item.quantity
    return merged


async def _load_order(db: AsyncSession, order_id: UUID) -> Order | None:
    result = await db.execute(
        select(Order).options(selectinload(Order.items)).where(Order.id == order_id)
    )
    return result.scalar_one_or_none()


async def _load_order_by_key(db: AsyncSession, idempotency_key: str) -> Order | None:
    result = await db.execute(
        select(Order)
        .options(selectinload(Order.items))
        .where(Order.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


def _outbox_payload(order: Order, event_id: uuid.UUID) -> dict:
    return {
        "event_id": str(event_id),
        "event_type": "order.created",
        "order_id": str(order.id),
        "customer_id": str(order.customer_id),
        "total": str(order.total),
        "items": [
            {
                "product_id": str(item.product_id),
                "quantity": item.quantity,
                "unit_price": str(item.unit_price),
            }
            for item in order.items
        ],
    }


async def _create_order(
    db: AsyncSession,
    body: OrderCreate,
    idempotency_key: str,
) -> tuple[Order, bool]:
    existing = await _load_order_by_key(db, idempotency_key)
    if existing is not None:
        return existing, False

    requested = _merge_quantities(body.items)
    product_ids = sorted(requested.keys())

    result = await db.execute(
        select(Product)
        .where(Product.id.in_(product_ids))
        .order_by(Product.id)
        .with_for_update()
    )
    products = list(result.scalars().all())
    found = {product.id: product for product in products}

    # A concurrent request with the same idempotency key may have committed
    # while we were waiting for product row locks. Re-check after locking so
    # duplicate retries return the already-created order instead of failing
    # stock validation after the first request consumed the stock.
    existing = await _load_order_by_key(db, idempotency_key)
    if existing is not None:
        return existing, False

    missing = [product_id for product_id in product_ids if product_id not in found]
    if missing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product not found: {missing[0]}",
        )

    for product_id in product_ids:
        product = found[product_id]
        quantity = requested[product_id]
        if product.stock < quantity:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Insufficient stock",
            )

    total = Decimal("0.00")
    order_items: list[OrderItem] = []
    for product_id in product_ids:
        product = found[product_id]
        quantity = requested[product_id]
        product.stock -= quantity
        unit_price = product.price
        total += unit_price * quantity
        order_items.append(
            OrderItem(
                product_id=product.id,
                quantity=quantity,
                unit_price=unit_price,
            )
        )

    order_id = uuid.uuid4()
    order = Order(
        id=order_id,
        customer_id=body.customer_id,
        total=total,
        status="created",
        idempotency_key=idempotency_key,
        items=order_items,
    )
    event_id = uuid.uuid4()
    outbox = OutboxEvent(
        id=event_id,
        event_type="order.created",
        aggregate_id=order_id,
        payload=_outbox_payload(order, event_id),
        status="pending",
    )
    db.add(order)
    db.add(outbox)

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        existing = await _load_order_by_key(db, idempotency_key)
        if existing is None:
            raise
        return existing, False

    created = await _load_order(db, order.id)
    if created is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Order was not found after commit",
        )
    return created, True


@router.post("", response_model=OrderResponse)
async def create_order(
    body: OrderCreate,
    response: Response,
    db: AsyncSession = Depends(get_db),
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1),
) -> Order:
    order, created = await _create_order(db, body, idempotency_key)
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return order


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> Order:
    order = await _load_order(db, order_id)
    if order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found")
    return order


@router.get("", response_model=list[OrderResponse])
async def list_orders(db: AsyncSession = Depends(get_db)) -> list[Order]:
    result = await db.execute(
        select(Order).options(selectinload(Order.items)).order_by(Order.created_at)
    )
    return list(result.scalars().all())
