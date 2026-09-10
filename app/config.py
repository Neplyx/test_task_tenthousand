from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://orders:orders@localhost:5432/orders"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "orders.events"
    kafka_consumer_group: str = "notification-writers"
    kafka_consumer_retry_delay_seconds: float = 2.0
    outbox_poll_interval_seconds: float = 1.0


settings = Settings()
