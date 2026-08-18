"""Tests for the consumer app (DynamoDB forwarder)."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from app import _handle_record, _build_writer


def _make_record(
    topic: str = "processing.succeeded",
    partition: int = 0,
    offset: int = 0,
    value: dict | str | None = None,
    key: str | None = None,
    headers: dict | None = None,
) -> dict:
    return {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "key": key,
        "value": json.dumps(value) if isinstance(value, dict) else value,
        "timestamp": (int(time.time()), 0),
        "headers": headers or {},
    }


class TestHandleRecord:
    def test_writes_json_payload(self) -> None:
        writer = MagicMock()
        record = _make_record(value={"job_id": "j-1", "status": "ok"})

        _handle_record(record, writer)

        writer.put_item.assert_called_once()
        item = writer.put_item.call_args[1]["Item"]
        assert item["Topic"] == "processing.succeeded"
        assert item["Partition"] == 0
        assert item["Offset"] == 0
        assert item["Payload"] == {"job_id": "j-1", "status": "ok"}
        assert "EventId" in item
        assert "ReceivedAt" in item

    def test_persists_upload_correlation_fields(self) -> None:
        writer = MagicMock()
        record = _make_record(value={
            "job_id": "raw-uploads#customers.csv#001",
            "raw_bucket": "raw-uploads",
            "object_key": "customers.csv",
            "execution_name": "csv-processing-execution",
        })

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert item["JobId"] == "raw-uploads#customers.csv#001"
        assert item["RawBucket"] == "raw-uploads"
        assert item["ObjectKey"] == "customers.csv"
        assert item["ExecutionName"] == "csv-processing-execution"

    def test_omits_missing_upload_correlation_fields(self) -> None:
        writer = MagicMock()

        _handle_record(writer=writer, record=_make_record(value={"status": "ok"}))

        item = writer.put_item.call_args[1]["Item"]
        assert "JobId" not in item
        assert "RawBucket" not in item
        assert "ObjectKey" not in item
        assert "ExecutionName" not in item

    def test_writes_none_payload(self) -> None:
        writer = MagicMock()
        record = _make_record(value=None)

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert item["Payload"] == {}

    def test_writes_invalid_json_as_empty(self) -> None:
        writer = MagicMock()
        record = _make_record(value="not-json")

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert item["Payload"] == {}

    def test_includes_message_key(self) -> None:
        writer = MagicMock()
        record = _make_record(key="csv-batch-42")

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert item["MessageKey"] == "csv-batch-42"

    def test_includes_headers_when_present(self) -> None:
        writer = MagicMock()
        record = _make_record(headers={"trace-id": "abc"})

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert item["Headers"] == {"trace-id": "abc"}

    def test_omits_headers_when_empty(self) -> None:
        writer = MagicMock()
        record = _make_record()

        _handle_record(record, writer)

        item = writer.put_item.call_args[1]["Item"]
        assert "Headers" not in item

    @patch("app.time.sleep")
    def test_retries_on_throttling(self, sleep: MagicMock) -> None:
        writer = MagicMock()
        throttle_error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "throttled"}},
            "PutItem",
        )
        writer.put_item.side_effect = [throttle_error, throttle_error, {}]

        record = _make_record(value={"a": 1})
        _handle_record(record, writer)

        assert writer.put_item.call_count == 3
        sleep.assert_any_call(1)
        sleep.assert_any_call(2)

    @patch("app.time.sleep")
    def test_raises_after_max_throttle_retries(self, sleep: MagicMock) -> None:
        writer = MagicMock()
        throttle_error = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "throttled"}},
            "PutItem",
        )
        writer.put_item.side_effect = throttle_error

        record = _make_record(value={"a": 1})
        with pytest.raises(ClientError):
            _handle_record(record, writer)

        assert writer.put_item.call_count == 5
        assert sleep.call_count == 4

    def test_raises_on_non_throttle_error(self) -> None:
        writer = MagicMock()
        service_error = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "oops"}},
            "PutItem",
        )
        writer.put_item.side_effect = service_error

        record = _make_record()
        with pytest.raises(ClientError):
            _handle_record(record, writer)

    def test_generates_unique_event_ids(self) -> None:
        writer = MagicMock()
        _handle_record(_make_record(offset=1), writer)
        _handle_record(_make_record(offset=2), writer)

        id1 = writer.put_item.call_args_list[0][1]["Item"]["EventId"]
        id2 = writer.put_item.call_args_list[1][1]["Item"]["EventId"]
        assert id1 != id2


class TestBuildWriter:
    @patch.dict("os.environ", {"EVENTS_TABLE_NAME": "TestTable"}, clear=True)
    @patch("app.boto3")
    def test_creates_table_resource(self, mock_boto3: MagicMock) -> None:
        mock_table = MagicMock()
        mock_boto3.resource.return_value.Table.return_value = mock_table

        writer = _build_writer()

        mock_boto3.resource.assert_called_once_with("dynamodb")
        assert writer is mock_table
