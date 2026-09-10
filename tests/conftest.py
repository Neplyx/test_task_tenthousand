import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer

from app.database import Base, get_db
import app.models  # noqa: F401


@pytest.fixture(scope="session")
def postgres_url() -> str:
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres.get_connection_url().replace("psycopg2", "asyncpg")


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def test_engine(postgres_url: str):
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session")
async def client(test_engine):
    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    from app.main import app

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def clean_db(test_engine):
    async with test_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE notifications, outbox_events, order_items, "
                "orders, products RESTART IDENTITY CASCADE"
            )
        )
    yield
