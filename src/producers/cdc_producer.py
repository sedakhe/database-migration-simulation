import argparse
import json
import logging
import pathlib
import time

from kafka import KafkaProducer


# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"   # generating timestamped logs
)
logger = logging.getLogger(__name__)


# Producer function
def produce_events(file_path: str, topic: str, bootstrap: str, delay: float):
    """
    Read CDC events from JSON file and publish them to Kafka topic.
    """

    # Initializing Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",               # ensure broker confirms write
        linger_ms=10,             # small batching
        retries=3                 # retry on transient errors
    )
    logger.info(f"Connected to Kafka at {bootstrap}, topic='{topic}'")

    # Loading events from file
    events = json.loads(pathlib.Path(file_path).read_text())
    logger.info(f"Loaded {len(events)} events from {file_path}")

    # Publishing events one by one
    for i, event in enumerate(events, 1):
        producer.send(topic, event)
        producer.flush()  # force immediate send
        logger.info(f"[{i}/{len(events)}] Sent event: {event}")
        time.sleep(delay)  # spacing between events

    logger.info("All events published. Closing producer.")
    producer.close()


# CLI entrypoint
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka CDC Producer: send events from JSON to Kafka."       # Making producer configurable
    )
    parser.add_argument("--file", type=str, default="data/sample_events.json",
                        help="Path to the JSON file with CDC events")
    parser.add_argument("--topic", type=str, default="users_cdc",
                        help="Kafka topic to publish events to")
    parser.add_argument("--bootstrap", type=str, default="localhost:9092",
                        help="Kafka bootstrap server(s)")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Delay in seconds between sending events")

    args = parser.parse_args()

    # Alternatively, instead of using args via CLI, we can use a config file
    # with the following structure:
    # {
    #     "file": "data/sample_events.json",
    #     "topic": "users_cdc",
    #     "bootstrap": "localhost:9092",
    #     "delay": 0.5
    # }

    produce_events(
        file_path=args.file,
        topic=args.topic,
        bootstrap=args.bootstrap,
        delay=args.delay
    )
