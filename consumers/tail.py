#!/usr/bin/env python3
"""
Clickstream consumer for Adnomaly project.
Reads events from Kafka and prints them to stdout.
"""

import json
import os
import signal
import sys
from typing import Dict, Any

from confluent_kafka import Consumer

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.schema import validate_event

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'clickstream')
GROUP_ID = os.getenv('GROUP_ID', 'tail-cli')
AUTO_OFFSET_RESET = os.getenv('AUTO_OFFSET_RESET', 'latest')


def create_consumer() -> Consumer:
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


def validate_and_print_event(event_data: Dict[str, Any]) -> None:
    """Validate an event and print it to stdout if valid."""
    try:
        # Validate the event
        validate_event(event_data)
        
        # Print as single line JSON
        print(json.dumps(event_data))
        
    except Exception as e:
        # Print warning to stderr and continue
        print(f"Warning: Invalid event skipped - {e}", file=sys.stderr)


def main():
    """Main function to run the consumer."""
    print(f"Starting clickstream consumer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Group ID: {GROUP_ID}")
    print(f"Auto offset reset: {AUTO_OFFSET_RESET}")
    print("Press Ctrl+C to stop")
    
    # Create consumer
    consumer = create_consumer()
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\nShutting down...")
        consumer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue
            if msg.value():
                event_data = json.loads(msg.value().decode('utf-8'))
                validate_and_print_event(event_data)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in consumer: {e}", file=sys.stderr)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
