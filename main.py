import asyncio
import re
import json
import uvicorn
from fastapi import FastAPI, HTTPException
from aio_pika import connect, Channel, Message
from datetime import datetime as dt
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import select
from models import create_db, engine, Result, RABBITMQ_URL, DATABASE_URL
from sqlalchemy.ext.asyncio import AsyncSession


Base = declarative_base()
session_async = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

RABBITMQ_QUEUE = "data_upload"


async def send_to_rabbitmq(channel: Channel, message: dict):
    message_body = json.dumps(message).encode("utf-8")
    rabbit_message = Message(message_body)
    await channel.default_exchange.publish(rabbit_message, routing_key=RABBITMQ_QUEUE)

async def declare_queue(channel: Channel):
    await channel.declare_queue(RABBITMQ_QUEUE, durable=True)


def create_app():
    app = FastAPI(docs_url='/')

    @app.on_event("startup")
    async def startup_event():
        connection = await connect(RABBITMQ_URL)
        channel = await connection.channel()
        await declare_queue(channel)

        await connection.close()

    @app.post("/upload_data")
    async def send_data_to_broker():
        connection = await connect(RABBITMQ_URL)
        channel = await connection.channel()

        with open("O_Genri_Testovaya_20_vmeste (1).txt", "r") as file:
            file_name = file.name
            lines = file.readlines()

        for line in lines:
            text_without_newlines = re.sub(r'\n', '', line)
            if not text_without_newlines:
                continue
            date_now = dt.now()
            date_string = date_now.strftime("%d.%m.%Y %H:%M:%S.%f")
            await send_to_rabbitmq(channel, {
                "datetime": date_string,
                "title": file_name,
                "text": text_without_newlines,
            })
            await asyncio.sleep(3)

    @app.get("/get_data/{id}")
    async def get_data(id: int):
        async with AsyncSession(engine) as db:
            result = await db.execute(select(Result).filter(Result.id == id))
            data = result.scalar_one()

            if not data:
                raise HTTPException(status_code=404, detail="Data not found")

            return {
                "datetime": data.datetime,
                "title": data.title,
                "x_avg_count_in_line": data.x_avg_count_in_line,
            }
    @app.get("/get_avg_x/")
    async def get_avg_x():
        async with AsyncSession(engine) as db:
            info =select(Result.datetime,Result.title, Result.x_avg_count_in_line).filter(Result.x_avg_count_in_line > 0)
            get_base = await db.execute(info)
            result =  get_base.fetchall()

            if not result:
                raise HTTPException(status_code=404, detail="Data not found")

            return result

    @app.get("/get_all/")
    async def get_all():
        async with AsyncSession(engine) as db:
            info = select(Result.datetime,Result.title, Result.x_avg_count_in_line)
            get_base = await db.execute(info)
            result = get_base.fetchall()

            return result

    return app


def main():
    uvicorn.run(
        f"{__name__}:create_app",
        host='0.0.0.0', port=8888,
        debug=True,
    )

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_db())
    main()
