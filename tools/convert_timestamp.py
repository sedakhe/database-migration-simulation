from datetime import datetime

# Example values from your sample JSON
timestamps_ms = [
    1726531200000,
    1726531240000,
    1726531250000,
    1726531260000,
    1726531270000
]

for ts in timestamps_ms:
    # Convert ms to seconds
    seconds = ts / 1000.0
    
    # Convert to UTC datetime
    dt = datetime.utcfromtimestamp(seconds)
    
    print(f"{ts} ms -> {dt} UTC")
