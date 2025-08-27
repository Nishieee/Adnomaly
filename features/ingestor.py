#!/usr/bin/env python3
"""
Feast ingestor for Adnomaly Phase 3.
Reads aggregated features from Kafka and writes them to Feast online store (Redis).
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

from confluent_kafka import Consumer
from feast import FeatureStore

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'features')
GROUP_ID = os.getenv('GROUP_ID', 'feast-ingestor')
AUTO_OFFSET_RESET = os.getenv('AUTO_OFFSET_RESET', 'latest')

# Feast configuration
FEATURE_REPO_PATH = os.getenv('FEATURE_REPO_PATH', 'features/feature_repo')
FEATURE_VIEW_NAME = os.getenv('FEATURE_VIEW_NAME', 'traffic_5m_by_geo_platform')


def create_kafka_consumer() -> Consumer:
    """Create and return a Kafka consumer."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET,
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 2000
    }
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])
    return consumer


def create_feature_store() -> FeatureStore:
    """Create and return a Feast feature store."""
    try:
        store = FeatureStore(repo_path=FEATURE_REPO_PATH)
        return store
    except Exception as e:
        print(f"Error creating feature store: {e}", file=sys.stderr)
        return None


def validate_feature_message(data: Dict[str, Any]) -> bool:
    """Validate a feature message from Kafka."""
    required_fields = [
        'window_start', 'window_end', 'geo', 'platform', 
        'ctr_avg', 'bounce_rate_avg', 'event_count'
    ]
    
    for field in required_fields:
        if field not in data:
            print(f"Warning: Missing required field '{field}'", file=sys.stderr)
            return False
    
    # Validate data types
    if not isinstance(data['geo'], str) or len(data['geo']) != 2:
        print("Warning: Invalid geo format", file=sys.stderr)
        return False
    
    if data['platform'] not in ['web', 'ios', 'android']:
        print("Warning: Invalid platform value", file=sys.stderr)
        return False
    
    if not isinstance(data['ctr_avg'], (int, float)) or data['ctr_avg'] < 0:
        print("Warning: Invalid ctr_avg value", file=sys.stderr)
        return False
    
    if not isinstance(data['bounce_rate_avg'], (int, float)) or data['bounce_rate_avg'] < 0:
        print("Warning: Invalid bounce_rate_avg value", file=sys.stderr)
        return False
    
    if not isinstance(data['event_count'], int) or data['event_count'] < 0:
        print("Warning: Invalid event_count value", file=sys.stderr)
        return False
    
    return True


def process_feature_message(store: FeatureStore, data: Dict[str, Any]) -> bool:
    """Process a feature message and write to Feast online store."""
    try:
        # Create Feast row
        row = {
            "geo": data["geo"],
            "platform": data["platform"],
            "event_timestamp": data["window_end"],
            "ctr_avg": float(data["ctr_avg"]),
            "bounce_rate_avg": float(data["bounce_rate_avg"]),
            "event_count": int(data["event_count"]),
        }
        
        # Write to online store
        store.online_write_batch(
            feature_view_name=FEATURE_VIEW_NAME,
            rows=[row]
        )
        
        print(f"Written to Feast: {data['geo']}/{data['platform']} at {data['window_end']}")
        return True
        
    except Exception as e:
        print(f"Error writing to Feast: {e}", file=sys.stderr)
        return False


def main():
    """Main function to run the Feast ingestor."""
    print(f"Starting Feast ingestor...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Group ID: {GROUP_ID}")
    print(f"Feature repo: {FEATURE_REPO_PATH}")
    print(f"Feature view: {FEATURE_VIEW_NAME}")
    print("Press Ctrl+C to stop")
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    # Create feature store
    store = create_feature_store()
    if not store:
        print("Failed to create feature store. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\nShutting down...")
        consumer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    events_processed = 0
    events_written = 0
    start_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue
            if msg.value():
                events_processed += 1
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    if validate_feature_message(data):
                        if process_feature_message(store, data):
                            events_written += 1
                    
                    # Print statistics every 10 events
                    if events_processed % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = events_processed / elapsed if elapsed > 0 else 0
                        print(f"Processed: {events_processed}, Written: {events_written}, Rate: {rate:.1f} events/sec")
                        
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}", file=sys.stderr)
                except Exception as e:
                    print(f"Error processing message: {e}", file=sys.stderr)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in main loop: {e}", file=sys.stderr)
    finally:
        print(f"Final stats - Processed: {events_processed}, Written: {events_written}")
        consumer.close()


if __name__ == '__main__':
    main()
