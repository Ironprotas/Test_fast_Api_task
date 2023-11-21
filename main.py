import asyncio
import re
import json
import uvicorn
from fastapi import FastAPI
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, func, Text
from aio_pika import connect, Channel, Message
from datetime import datetime as dt
import consumer



RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

Base = declarative_base()
RABBITMQ_QUEUE = "data_upload"

class Result(Base):
    __tablename__ = "result"
    id = Column(Integer, primary_key=True, index=True)
    datetime = Column(DateTime, default=func.now())
    title = Column(String)
    text = Column(Text, nullable=False)
    x_avg_count_in_line = Column(Integer)

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
            file_name =file.name
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

    return app

def main():
    uvicorn.run(
        f"{__name__}:create_app",
        host='0.0.0.0', port=8888,
        debug=True,
    )

if __name__ == '__main__':
    main()
    consumer.main()