#!/bin/sh

while ! nc -z rabbitmq_fast_api 5672; do
  sleep 0.2
done

uvicorn main:create_app --host 0.0.0.0 --port 8000 --reload &
sleep 10
python consumer.py
