#!/usr/bin/env python3
"""
Clickstream event generator for Adnomaly project.
Generates realistic clickstream events and sends them to Kafka.
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import hashlib
from faker import Faker

from confluent_kafka import Producer

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.schema import validate_event

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'clickstream')
EVENTS_PER_SEC = float(os.getenv('EVENTS_PER_SEC', '120'))
HASH_SALT = os.getenv('HASH_SALT')  # No default required by Phase 1 make target provides one

# Constants - Balanced distribution
GEO_COUNTRIES = ['US', 'IN', 'BR', 'DE', 'JP', 'CA', 'AU', 'UK', 'FR', 'IT', 'ES', 'NL', 'SE', 'NO', 'DK', 'FI', 'CH', 'AT', 'BE', 'IE', 'PT', 'GR', 'PL', 'CZ', 'HU', 'RO', 'BG', 'HR', 'SI', 'SK', 'LT', 'LV', 'EE', 'LU', 'MT', 'CY']

# Balanced platform distribution (web: 40%, mobile: 60%)
PLATFORMS = ['web', 'ios', 'android']
PLATFORM_WEIGHTS = [0.4, 0.3, 0.3]  # 40% web, 30% iOS, 30% Android

# Realistic metric ranges
CTR_MIN, CTR_MAX = 0.005, 0.05
CPC_MIN, CPC_MAX = 0.10, 1.20
BOUNCE_RATE_MIN, BOUNCE_RATE_MAX = 0.20, 0.90

# Time range for diverse timestamps (last 12 months for better distribution)
TIME_RANGE_DAYS = 365  # 12 months of data

# Initialize Faker
fake = Faker()


def generate_balanced_timestamp() -> str:
    """Generate a timestamp with balanced distribution across the last 12 months."""
    # Use weighted random to ensure better distribution across months
    # More recent months get slightly higher weight
    days_ago = int(random.weibullvariate(TIME_RANGE_DAYS * 0.3, 2))
    days_ago = min(days_ago, TIME_RANGE_DAYS)  # Ensure we don't go beyond range
    
    random_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
    
    # Add random hours, minutes, seconds for more diversity
    random_date += timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999)
    )
    
    return random_date.isoformat().replace('+00:00', 'Z')


def generate_user_id_hash(salt: str) -> str:
    """Generate a stable user ID hash from a UUID and salt."""
    user_uuid = str(uuid.uuid4())
    hash_input = f"{user_uuid}:{salt}".encode('utf-8')
    hash_result = hashlib.sha256(hash_input).hexdigest()
    return hash_result[:16].lower()  # Take first 16 hex chars, ensure lowercase


def sample_event() -> Dict[str, Any]:
    """Generate a single clickstream event with balanced distribution."""
    # Generate balanced timestamp
    timestamp = generate_balanced_timestamp()
    
    # Generate user ID hash
    user_id_hash = generate_user_id_hash(HASH_SALT)
    
    # Generate ad and campaign IDs
    ad_id = f"ad_{random.randint(1000, 9999)}"
    campaign_id = f"camp_{random.randint(100, 999)}"
    
    # Balanced selections
    geo = random.choice(GEO_COUNTRIES)  # Equal probability for all countries
    platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0]  # Weighted platform selection
    user_agent = fake.user_agent()
    
    # Generate realistic metrics with some correlation
    ctr = random.uniform(CTR_MIN, CTR_MAX)
    cpc = random.uniform(CPC_MIN, CPC_MAX)
    bounce_rate = random.uniform(BOUNCE_RATE_MIN, BOUNCE_RATE_MAX)
    
    # Conversion based on CTR with realistic correlation
    conversion_prob = ctr * random.uniform(0.1, 0.8)
    conversion = 1 if random.random() < conversion_prob else 0
    
    return {
        'timestamp': timestamp,
        'user_id_hash': user_id_hash,
        'ad_id': ad_id,
        'campaign_id': campaign_id,
        'geo': geo,
        'platform': platform,
        'user_agent': user_agent,
        'CPC': round(cpc, 3),
        'CTR': round(ctr, 4),
        'conversion': conversion,
        'bounce_rate': round(bounce_rate, 3)
    }


def create_producer() -> Producer:
    """Create and return a Kafka producer."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'acks': 'all',
        'retries': 3
    }
    return Producer(config)


def send_event(producer: Producer, event: Dict[str, Any]) -> None:
    """Send a single event to Kafka."""
    try:
        # Validate event before sending
        validate_event(event)
        
        # Send to Kafka
        producer.produce(TOPIC, value=json.dumps(event).encode('utf-8'))
        producer.flush()
        
    except Exception as e:
        print(f"Error sending event: {e}", file=sys.stderr)


def main():
    """Main function to run the event generator."""
    print(f"Starting clickstream generator...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Events per second: {EVENTS_PER_SEC}")
    print(f"Hash salt: {HASH_SALT}")
    print("Press Ctrl+C to stop")
    
    # Create producer
    producer = create_producer()
    
    # Calculate sleep interval
    sleep_interval = 1.0 / EVENTS_PER_SEC
    
    # Statistics
    events_sent = 0
    last_report_time = time.time()
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print(f"\nShutting down... Sent {events_sent} events total")
        producer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        while True:
            start_time = time.time()
            
            # Generate and send event
            event = sample_event()
            send_event(producer, event)
            events_sent += 1
            
            # Report progress every ~5 seconds
            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - last_report_time
                actual_eps = events_sent / elapsed if elapsed > 0 else 0
                print(f"Sent {events_sent} events in {elapsed:.1f}s ({actual_eps:.1f} events/sec)")
                events_sent = 0
                last_report_time = current_time
            
            # Sleep to maintain rate
            elapsed = time.time() - start_time
            if elapsed < sleep_interval:
                time.sleep(sleep_interval - elapsed)
                
    except KeyboardInterrupt:
        print(f"\nShutting down... Sent {events_sent} events total")
    except Exception as e:
        print(f"Error in main loop: {e}", file=sys.stderr)
    finally:
        producer.close()


if __name__ == '__main__':
    main()
