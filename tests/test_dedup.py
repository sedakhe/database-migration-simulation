import pandas as pd

def test_latest_event_wins():
    df = pd.DataFrame([
        {"user_id": 1, "op": "c", "ts": 100, "name": "Old"},
        {"user_id": 1, "op": "u", "ts": 200, "name": "New"},
    ])
    latest = df.sort_values("ts").groupby("user_id").tail(1)
    assert latest.iloc[0]["name"] == "New"
