#!/usr/bin/env python3
"""
CDC Producer for Database Migration Simulation

This script simulates a source system that emits Change Data Capture (CDC) events
to Kafka. It reads sample data and publishes it to a Kafka topic for processing
by the Flink job.
"""

import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCProducer:
    """Producer that simulates CDC events from a source database system."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "cdc-events"):
        """
        Initialize the CDC Producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to publish events to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                retry_backoff_ms=100,
                request_timeout_ms=30000,
                max_block_ms=10000
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def _load_sample_data(self, data_file: str) -> List[Dict[str, Any]]:
        """Load sample data from JSON file."""
        try:
            with open(data_file, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} records from {data_file}")
            return data
        except FileNotFoundError:
            logger.error(f"Sample data file not found: {data_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in sample data file: {e}")
            raise
    
    def _enrich_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich the CDC event with additional metadata.
        
        Args:
            event: Original event data
            
        Returns:
            Enriched event with CDC metadata
        """
        enriched_event = event.copy()
        
        # Add CDC metadata
        enriched_event['cdc_timestamp'] = datetime.utcnow().isoformat() + 'Z'
        enriched_event['cdc_sequence'] = random.randint(1000, 9999)
        enriched_event['cdc_source'] = 'legacy_database'
        enriched_event['cdc_schema'] = 'public'
        enriched_event['cdc_table'] = 'users'
        
        # Add some random variations for realistic simulation
        if random.random() < 0.1:  # 10% chance of adding noise
            enriched_event['processing_notes'] = f"Event processed with noise level {random.randint(1, 5)}"
        
        return enriched_event
    
    def _simulate_realistic_timing(self, event: Dict[str, Any]) -> float:
        """
        Simulate realistic timing between events based on event type.
        
        Args:
            event: Event data
            
        Returns:
            Sleep duration in seconds
        """
        operation = event.get('cdc_operation', 'INSERT')
        
        # Different timing patterns for different operations
        if operation == 'INSERT':
            # New records come in bursts
            return random.uniform(0.5, 2.0)
        elif operation == 'UPDATE':
            # Updates are more frequent
            return random.uniform(0.1, 1.0)
        elif operation == 'DELETE':
            # Deletes are rare
            return random.uniform(5.0, 10.0)
        else:
            return random.uniform(1.0, 3.0)
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """
        Publish a single CDC event to Kafka.
        
        Args:
            event: Event data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            enriched_event = self._enrich_event(event)
            
            # Use the record ID as the key for partitioning
            key = str(event.get('id', 'unknown'))
            
            # Send the event
            future = self.producer.send(
                self.topic,
                key=key,
                value=enriched_event
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Published event {key} to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish event {event.get('id', 'unknown')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing event: {e}")
            return False
    
    def run_simulation(self, data_file: str, continuous: bool = False, interval: int = 1):
        """
        Run the CDC simulation.
        
        Args:
            data_file: Path to sample data file
            continuous: Whether to run continuously
            interval: Interval between batches in seconds
        """
        logger.info("Starting CDC simulation...")
        
        # Load sample data
        sample_data = self._load_sample_data(data_file)
        
        batch_count = 0
        total_events = 0
        successful_events = 0
        
        try:
            while True:
                batch_count += 1
                logger.info(f"Processing batch {batch_count}")
                
                # Shuffle data for realistic simulation
                batch_data = sample_data.copy()
                random.shuffle(batch_data)
                
                for event in batch_data:
                    total_events += 1
                    
                    if self.publish_event(event):
                        successful_events += 1
                    
                    # Simulate realistic timing
                    sleep_duration = self._simulate_realistic_timing(event)
                    time.sleep(sleep_duration)
                
                logger.info(
                    f"Batch {batch_count} completed. "
                    f"Total events: {total_events}, Successful: {successful_events}"
                )
                
                if not continuous:
                    break
                
                # Wait before next batch
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        except Exception as e:
            logger.error(f"Simulation failed: {e}")
        finally:
            self.close()
            logger.info(
                f"Simulation completed. "
                f"Total events: {total_events}, Successful: {successful_events}"
            )
    
    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer connection closed")


def main():
    """Main function to run the CDC producer."""
    parser = argparse.ArgumentParser(description='CDC Producer for Database Migration Simulation')
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='cdc-events',
        help='Kafka topic to publish to (default: cdc-events)'
    )
    parser.add_argument(
        '--data-file',
        default='data/sample_events.json',
        help='Path to sample data file (default: data/sample_events.json)'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run simulation continuously'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='Interval between batches in seconds (default: 30)'
    )
    
    args = parser.parse_args()
    
    # Resolve data file path
    data_file = args.data_file
    if not os.path.isabs(data_file):
        # Make path relative to project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_file = os.path.join(project_root, data_file)
    
    try:
        producer = CDCProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic
        )
        
        producer.run_simulation(
            data_file=data_file,
            continuous=args.continuous,
            interval=args.interval
        )
        
    except Exception as e:
        logger.error(f"Failed to start CDC producer: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()