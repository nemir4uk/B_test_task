from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel
from sqlalchemy import BigInteger, Identity, DateTime, func, text, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from src.config import settings


async_engine_pg = create_async_engine(f'postgresql+asyncpg://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}',
                                      echo=False,
                                      pool_size=10,
                                      max_overflow=20)

Async_Session_pg = async_sessionmaker(async_engine_pg, class_=AsyncSession, expire_on_commit=False)


class Metadata(BaseModel):
    timestamp: datetime


class ConsumedData(BaseModel):
    data: dict
    metadata: Metadata


class PgBase(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)


class Messages(PgBase):
    __tablename__ = 'messages'

    payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None]


async def create_if_not_exist(async_session_pg) -> None:
    async with async_session_pg() as session:
        stmt = text("""CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        payload JSON NOT NULL,
        received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        processed_at TIMESTAMP WITH TIME ZONE
        )""")
        session.execute(stmt)
        session.commit()


async def insert_values(async_session_pg, payload) -> None:
    async with async_session_pg() as session:
        stmt = insert(Messages).values(payload=payload)
        session.execute(stmt)
        session.commit()





