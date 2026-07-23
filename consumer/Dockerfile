FROM python:3.12-slim

RUN groupadd -r consumer && useradd -r -g consumer -d /app -s /sbin/nologin consumer

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_consumer.py app.py ./

RUN chown -R consumer:consumer /app

USER consumer

ENTRYPOINT ["python", "-m", "app"]
