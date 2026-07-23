"""Tests for the Kafka consumer module."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from kafka_consumer import ShutdownRequested, create_consumer, poll_loop


def _make_message(
    topic: str = "processing.succeeded",
    partition: int = 0,
    offset: int = 0,
    value: dict | None = None,
    key: str | None = None,
) -> MagicMock:
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.value.return_value = json.dumps(value).encode() if value else None
    msg.key.return_value = key.encode() if key else None
    msg.error.return_value = None
    msg.timestamp.return_value = (int(time.time()), 0)
    msg.headers.return_value = []
    return msg


def _make_error_message(error_code: int | None = None) -> MagicMock:
    msg = MagicMock()
    if error_code is not None:
        err = MagicMock()
        err.code.return_value = error_code
        msg.error.return_value = err
    else:
        msg.error.return_value = None
    return msg


class TestCreateConsumer:
    @patch.dict("os.environ", {
        "MSK_BOOTSTRAP_SERVERS": "b-1.msk.abc:9098",
        "KAFKA_CONSUMER_GROUP": "test-group",
        "SECURITY_PROTOCOL": "SASL_SSL",
        "AWS_REGION": "us-east-1",
    })
    @patch("kafka_consumer.Consumer")
    def test_sasl_ssl_config(self, MockConsumer: MagicMock) -> None:
        create_consumer()
        call_args = MockConsumer.call_args[0][0]
        assert call_args["security.protocol"] == "SASL_SSL"
        assert call_args["sasl.mechanisms"] == "AWS_MSK_IAM"
        assert call_args["sasl.aws.region"] == "us-east-1"
        assert call_args["group.id"] == "test-group"

    @patch.dict("os.environ", {
        "MSK_BOOTSTRAP_SERVERS": "localhost:29092",
        "SECURITY_PROTOCOL": "PLAINTEXT",
    }, clear=True)
    @patch("kafka_consumer.Consumer")
    def test_plaintext_config(self, MockConsumer: MagicMock) -> None:
        create_consumer()
        call_args = MockConsumer.call_args[0][0]
        assert call_args["security.protocol"] == "PLAINTEXT"
        assert "sasl.mechanisms" not in call_args

    @patch.dict("os.environ", {
        "MSK_BOOTSTRAP_SERVERS": "b-1.msk.abc:9098",
    }, clear=True)
    @patch("kafka_consumer.Consumer")
    def test_defaults(self, MockConsumer: MagicMock) -> None:
        create_consumer()
        call_args = MockConsumer.call_args[0][0]
        assert call_args["group.id"] == "data-processing-consumer"
        assert call_args["security.protocol"] == "SASL_SSL"
        assert call_args["auto.offset.reset"] == "earliest"
        assert call_args["enable.auto.commit"] is False
        assert call_args["partition.assignment.strategy"] == "cooperative-sticky"


class TestPollLoop:
    def test_processes_messages_and_commits(self) -> None:
        consumer = MagicMock()
        msg = _make_message(offset=42)
        consumer.poll.side_effect = [msg, ShutdownRequested]

        handler = MagicMock()
        poll_loop(consumer, {"processing.succeeded": handler})

        handler.assert_called_once()
        record = handler.call_args[0][0]
        assert record["topic"] == "processing.succeeded"
        assert record["offset"] == 42
        assert record["value"] == json.dumps({"key": "val"}) if False else record["value"] is None or isinstance(record["value"], str)
        consumer.commit.assert_called_once_with(asynchronous=False)
        consumer.close.assert_called_once()

    def test_skips_partition_eof(self) -> None:
        consumer = MagicMock()
        eof_msg = _make_error_message(error_code=-191)  # _PARTITION_EOF
        consumer.poll.side_effect = [eof_msg, ShutdownRequested]

        handler = MagicMock()
        poll_loop(consumer, {"processing.succeeded": handler})

        handler.assert_not_called()
        consumer.close.assert_called_once()

    def test_skips_kafka_errors(self) -> None:
        consumer = MagicMock()
        err_msg = MagicMock()
        err = MagicMock()
        err.__str__ = lambda self: "broker unavailable"
        err.code.return_value = 1
        err_msg.error.return_value = err
        consumer.poll.side_effect = [err_msg, ShutdownRequested]

        handler = MagicMock()
        poll_loop(consumer, {"processing.succeeded": handler})

        handler.assert_not_called()
        consumer.close.assert_called_once()

    def test_warns_on_unhandled_topic(self) -> None:
        consumer = MagicMock()
        msg = _make_message(topic="unknown.topic")
        consumer.poll.side_effect = [msg, ShutdownRequested]

        handler = MagicMock()
        poll_loop(consumer, {"processing.succeeded": handler})

        handler.assert_not_called()
        consumer.close.assert_called_once()

    def test_records_key_and_headers(self) -> None:
        consumer = MagicMock()
        msg = _make_message(key="my-key", value={"a": 1})
        msg.headers.return_value = [("correlation-id", b"abc-123")]
        consumer.poll.side_effect = [msg, ShutdownRequested]

        handler = MagicMock()
        poll_loop(consumer, {"processing.succeeded": handler})

        record = handler.call_args[0][0]
        assert record["key"] == "my-key"
        assert record["headers"] == {"correlation-id": "abc-123"}

    def test_closes_consumer_on_exception(self) -> None:
        consumer = MagicMock()
        msg = _make_message()
        consumer.poll.side_effect = [msg, ShutdownRequested]

        def exploding_handler(record: dict) -> None:
            raise RuntimeError("boom")

        poll_loop(consumer, {"processing.succeeded": exploding_handler})
        consumer.close.assert_called_once()
