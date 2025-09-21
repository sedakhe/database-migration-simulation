import json
import argparse

def test_parse_args_defaults():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="data/sample_events.json")
    args = parser.parse_args([])
    assert args.file == "data/sample_events.json"

def test_loads_events_from_file(tmp_path):
    file = tmp_path / "events.json"
    file.write_text(json.dumps([{"op": "c", "after": {"user_id": 1}, "timestamp_ms": 1234}]))
    events = json.loads(file.read_text())
    assert events[0]["op"] == "c"
    assert "timestamp_ms" in events[0]
