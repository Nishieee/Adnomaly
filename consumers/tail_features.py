#!/usr/bin/env python3
"""
Features consumer for Adnomaly Phase 2.
Reads aggregated features from Kafka and prints them to stdout.
"""

import json
import os
import signal
import sys
from typing import Dict, Any

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Consumer

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'features')
GROUP_ID = os.getenv('GROUP_ID', 'tail-features')
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


def validate_and_print_feature(feature_data: Dict[str, Any]) -> None:
    """Validate a feature record and print it to stdout if valid."""
    try:
        # Basic validation of required fields
        required_fields = ['window_start', 'window_end', 'geo', 'platform', 'ctr_avg', 'bounce_rate_avg', 'event_count']
        
        for field in required_fields:
            if field not in feature_data:
                print(f"Warning: Missing required field '{field}' in feature record", file=sys.stderr)
                return
        
        # Validate data types
        if not isinstance(feature_data['window_start'], str) or not feature_data['window_start'].endswith('Z'):
            print("Warning: Invalid window_start format", file=sys.stderr)
            return
        
        if not isinstance(feature_data['window_end'], str) or not feature_data['window_end'].endswith('Z'):
            print("Warning: Invalid window_end format", file=sys.stderr)
            return
        
        if not isinstance(feature_data['geo'], str) or len(feature_data['geo']) != 2:
            print("Warning: Invalid geo format", file=sys.stderr)
            return
        
        if feature_data['platform'] not in ['web', 'ios', 'android']:
            print("Warning: Invalid platform value", file=sys.stderr)
            return
        
        if not isinstance(feature_data['ctr_avg'], (int, float)) or feature_data['ctr_avg'] < 0:
            print("Warning: Invalid ctr_avg value", file=sys.stderr)
            return
        
        if not isinstance(feature_data['bounce_rate_avg'], (int, float)) or feature_data['bounce_rate_avg'] < 0:
            print("Warning: Invalid bounce_rate_avg value", file=sys.stderr)
            return
        
        if not isinstance(feature_data['event_count'], int) or feature_data['event_count'] < 0:
            print("Warning: Invalid event_count value", file=sys.stderr)
            return
        
        # Print as single line JSON
        print(json.dumps(feature_data))
        
    except Exception as e:
        # Print warning to stderr and continue
        print(f"Warning: Invalid feature record skipped - {e}", file=sys.stderr)


def main():
    """Main function to run the features consumer."""
    print(f"Starting features consumer...")
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
                feature_data = json.loads(msg.value().decode('utf-8'))
                validate_and_print_feature(feature_data)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in consumer: {e}", file=sys.stderr)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
