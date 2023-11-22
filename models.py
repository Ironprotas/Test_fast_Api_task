import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import Column, Integer, String, DateTime, func, Text, Float
from sqlalchemy.ext.declarative import declarative_base

RABBITMQ_URL = "amqp://guest:guest@rabbitmq_fast_api:5672/"

DATABASE_URL = "postgresql+asyncpg://postgres:123@my-postgres-fast/result"

engine = create_async_engine(DATABASE_URL, echo=True)
Base = declarative_base()

class Result(Base):
    __tablename__ = "result"
    id = Column(Integer, primary_key=True, index=True)
    datetime = Column(DateTime, default=func.now())
    title = Column(String)
    text = Column(Text, nullable=False)
    x_avg_count_in_line = Column(Float)

async def create_db():
    conn = await asyncpg.connect(user='postgres', password="123", host="my-postgres-fast")
    result = await conn.fetch("SELECT 1 FROM pg_database WHERE datname = 'result'")
    if not result:
        await conn.execute('CREATE DATABASE result TEMPLATE template0')
        await  init_db()

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

