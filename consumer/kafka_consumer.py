"""Kafka consumer with AWS MSK IAM authentication."""

from __future__ import annotations

import os
import signal
from typing import Callable

import structlog
from confluent_kafka import Consumer, KafkaError

logger = structlog.get_logger(__name__)


class ShutdownRequested(Exception):
    """Raised to break out of the poll loop on SIGTERM/SIGINT."""


def _install_signal_handlers() -> None:
    def _handler(signum: int, _frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        raise ShutdownRequested

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def create_consumer() -> Consumer:
    bootstrap = os.environ["MSK_BOOTSTRAP_SERVERS"]
    group = os.environ.get("KAFKA_CONSUMER_GROUP", "data-processing-consumer")
    security_protocol = os.environ.get("SECURITY_PROTOCOL", "SASL_SSL")

    config: dict = {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
        "max.poll.interval.ms": 300000,
        "partition.assignment.strategy": "cooperative-sticky",
        "security.protocol": security_protocol,
    }

    if security_protocol == "SASL_SSL":
        config["sasl.mechanisms"] = "AWS_MSK_IAM"
        config["sasl.aws.region"] = os.environ.get("AWS_REGION", "eu-north-1")

    return Consumer(config)


def poll_loop(
    consumer: Consumer,
    handlers: dict[str, Callable[[dict], None]],
) -> None:
    _install_signal_handlers()

    topics = list(handlers.keys())
    consumer.subscribe(topics)
    logger.info("subscribed_to_topics", topics=topics)
    logger.info("consumer_loop_started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("kafka_error", error=str(msg.error()))
                continue

            topic = msg.topic()
            try:
                record = {
                    "topic": topic,
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": msg.value().decode("utf-8") if msg.value() else None,
                    "timestamp": msg.timestamp(),
                    "headers": {
                        h[0]: h[1].decode("utf-8") if isinstance(h[1], bytes) else h[1]
                        for h in (msg.headers() or [])
                    },
                }

                handler = handlers.get(topic)
                if handler:
                    handler(record)
                else:
                    logger.warning("no_handler_for_topic", topic=topic)

                consumer.commit(asynchronous=False)

            except Exception:
                logger.exception("message_processing_error", topic=topic, offset=msg.offset())

    except ShutdownRequested:
        pass
    finally:
        logger.info("consumer_loop_stopped")
        consumer.close()
