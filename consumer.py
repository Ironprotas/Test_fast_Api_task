import asyncio
import json
from aio_pika import connect, IncomingMessage
from models import Result, engine, create_db, Base
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime as dt
from pydantic import BaseModel

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

class Message(BaseModel):
    datetime: str
    title: str
    text: str

async def on_message(message: IncomingMessage):
    try:
        data = json.loads(message.body.decode("utf-8"))
        message_model = Message(**data)
        datetime_str = message_model.datetime
        title = message_model.title
        text = message_model.text
        count_x = text.count("Х")
        datetime = dt.strptime(datetime_str, "%d.%m.%Y %H:%M:%S.%f")
        average_x = (count_x / len(text))*100

        async with AsyncSession(engine) as db:
             result = Result(datetime=datetime, title=title, text=text, x_avg_count_in_line=average_x)
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
        print("Programm stoped")
    finally:
        loop.close()

if __name__ == "__main__":
    print("test")
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("Programm stoped")
    finally:
        loop.close()