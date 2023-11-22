#!/bin/sh

while ! nc -z rabbitmq_fast_api 5672; do
  sleep 0.1
done

exec uvicorn main:create_app --host 0.0.0.0 --port 8000 --reload && python consumer.py
