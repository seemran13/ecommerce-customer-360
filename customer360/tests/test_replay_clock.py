"""Small deterministic tests for event-time coordination between replay and heartbeat."""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.abspath("ingestion"))
import replay_producer
import heartbeat_producer


def test_parse_event_time_treats_naive_timestamp_as_utc():
    parsed = replay_producer.parse_event_time('{"event_ts":"2026-07-01T04:17:57"}')
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0)
    assert parsed.isoformat() == "2026-07-01T04:17:57+00:00"


def test_replay_clock_is_atomically_written_and_read(tmp_path, monkeypatch):
    clock_file = tmp_path / ".c360_replay_clock.json"
    monkeypatch.setattr(replay_producer, "CLOCK_FILE", str(clock_file))
    when = datetime(2026, 7, 1, 4, 17, 57, tzinfo=timezone.utc)
    replay_producer.write_replay_clock(True, when)
    payload = json.loads(clock_file.read_text(encoding="utf-8"))
    assert payload["active"] is True
    assert payload["event_ts"] == "2026-07-01T04:17:57"
    assert not (tmp_path / ".c360_replay_clock.json.tmp").exists()


def test_heartbeat_parses_naive_replay_clock_as_utc():
    parsed = heartbeat_producer.parse_clock("2026-07-01T04:17:57")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0)
