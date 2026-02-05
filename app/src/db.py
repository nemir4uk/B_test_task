from datetime import datetime
from typing import Any, Dict
from pydantic import BaseModel
from sqlalchemy import BigInteger, Identity, create_engine, DateTime, func, text, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.dialects.postgresql import JSON
from src.config import settings


engine_pg = create_engine(f'postgresql://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}', echo=False)

Session_pg = sessionmaker(bind=engine_pg)


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
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime]


def create_if_not_exist(session_pg) -> None:
    with session_pg() as session:
        stmt = text("""CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        payload JSON NOT NULL,
        received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        processed_at TIMESTAMP WITH TIME ZONE
        )""")
        session.execute(stmt)
        session.commit()


def insert_values(session_pg, payload) -> None:
    with session_pg() as session:
        stmt = insert(Messages).values(payload=payload)
        session.execute(stmt)
        session.commit()





