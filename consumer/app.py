"""Kafka consumer entry point — generic forwarder to DynamoDB.

Architecture:
    MSK Topics → Consumer (ECS Fargate) → DynamoDB EventsTable
                                               ↓
                                    Athena federated queries
                                               ↓
                                          Analytics

The consumer does NO analytics. It is a thin, stateless forwarder.
All analytics are performed via Athena SQL against DynamoDB.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from typing import Protocol

import boto3
import structlog
from botocore.exceptions import ClientError
from kafka_consumer import create_consumer, ensure_topics, poll_loop


def _log_level() -> int:
    raw_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    if raw_level.isdigit():
        return int(raw_level)
    return getattr(logging, raw_level, logging.INFO)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer() if sys.stderr.isatty() else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(_log_level()),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

THROTTLE_ERRORS = {
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
}
MAX_DYNAMO_RETRIES = 5
MAX_DYNAMO_BACKOFF_SECONDS = 8


class TableWriter(Protocol):
    def put_item(self, *, Item: dict) -> dict: ...


def _build_writer() -> TableWriter:
    ddb = boto3.resource("dynamodb")
    return ddb.Table(os.environ["EVENTS_TABLE_NAME"])


def _handle_record(record: dict, writer: TableWriter) -> None:
    try:
        payload = json.loads(record["value"]) if record.get("value") else {}
    except (json.JSONDecodeError, TypeError):
        payload = {}

    item = {
        "EventId": str(uuid.uuid4()),
        "Topic": record["topic"],
        "Partition": record["partition"],
        "Offset": record["offset"],
        "ReceivedAt": int(time.time()),
        "MessageKey": record.get("key"),
        "Payload": payload,
    }

    if record.get("headers"):
        item["Headers"] = record["headers"]

    for attempt in range(MAX_DYNAMO_RETRIES):
        try:
            writer.put_item(Item=item)
            break
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code not in THROTTLE_ERRORS:
                raise
            if attempt == MAX_DYNAMO_RETRIES - 1:
                logger.error(
                    "dynamo_throttled_max_retries",
                    event_id=item["EventId"],
                    error_code=error_code,
                    attempts=MAX_DYNAMO_RETRIES,
                )
                raise

            backoff = min(2 ** attempt, MAX_DYNAMO_BACKOFF_SECONDS)
            logger.warning(
                "dynamo_throttled",
                event_id=item["EventId"],
                error_code=error_code,
                attempt=attempt + 1,
                retry_in=backoff,
            )
            time.sleep(backoff)

    logger.info(
        "event_written",
        event_id=item["EventId"],
        topic=record["topic"],
        partition=record["partition"],
        offset=record["offset"],
    )


def main() -> None:
    writer = _build_writer()

    success_topic = os.environ.get("KAFKA_SUCCESS_TOPIC", "processing.succeeded")
    failure_topic = os.environ.get("KAFKA_FAILURE_TOPIC", "processing.failed")

    logger.info(
        "starting_kafka_consumer",
        bootstrap=os.environ.get("MSK_BOOTSTRAP_SERVERS", "(not set)"),
        success_topic=success_topic,
        failure_topic=failure_topic,
        consumer_group=os.environ.get("KAFKA_CONSUMER_GROUP", "data-processing-consumer"),
        events_table=os.environ.get("EVENTS_TABLE_NAME", "(not set)"),
    )

    def on_success(record: dict) -> None:
        _handle_record(record, writer)

    def on_failure(record: dict) -> None:
        _handle_record(record, writer)

    topic_names = [success_topic, failure_topic]
    topic_partitions = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "3"))
    topic_replication_factor = int(os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "3"))

    consumer = create_consumer()

    ensure_topics(
        topic_names,
        partitions=topic_partitions,
        replication_factor=topic_replication_factor,
    )
    poll_loop(consumer, {success_topic: on_success, failure_topic: on_failure})


if __name__ == "__main__":
    main()
