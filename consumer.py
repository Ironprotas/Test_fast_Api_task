import asyncio
import json
import time

from aio_pika import connect, IncomingMessage
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import Column, Integer, String, DateTime, func, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime as dt
from pydantic import BaseModel
import asyncpg


DATABASE_URL = "postgresql+asyncpg://postgres:123@localhost/result"
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
engine = create_async_engine(DATABASE_URL, echo=True)
Base = declarative_base()

class Message(BaseModel):
    datetime: str
    title: str
    text: str

class Result(Base):
    __tablename__ = "result"
    id = Column(Integer, primary_key=True, index=True)
    datetime = Column(DateTime, default=func.now())
    title = Column(String)
    text = Column(Text, nullable=False)
    x_avg_count_in_line = Column(Integer)

async def create_db():
    conn = await asyncpg.connect(user='postgres', password="123")
    result = await conn.fetch("SELECT 1 FROM pg_database WHERE datname = 'result'")
    if not result:
        await conn.execute('CREATE DATABASE result TEMPLATE template0')

async def on_message(message: IncomingMessage):
    try:
        data = json.loads(message.body.decode("utf-8"))
        message_model = Message(**data)
        datetime_str = message_model.datetime
        title = message_model.title
        text = message_model.text
        count_x = text.count("Х")
        datetime = dt.strptime(datetime_str, "%d.%m.%Y %H:%M:%S.%f")

        async with AsyncSession(engine) as db:
             result = Result(datetime=datetime, title=title, text=text, x_avg_count_in_line=count_x)
             db.add(result)
             await db.commit()

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

async def consume():
    connection = await connect(RABBITMQ_URL)
    channel = await connection.channel()

    queue = await channel.declare_queue("data_upload", durable=True)

    await queue.consume(on_message)

async def init():
    await create_db()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def main():
    await init()
    loop = asyncio.get_event_loop()
    loop.create_task(consume())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

