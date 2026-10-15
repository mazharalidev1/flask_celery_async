FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -e ".[otel]" \
    && pip install --no-cache-dir -r examples/otel-demo/requirements.txt

ENV PYTHONUNBUFFERED=1