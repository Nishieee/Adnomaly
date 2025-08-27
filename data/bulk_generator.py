#!/usr/bin/env python3
"""
Bulk clickstream event generator for Adnomaly project.
Generates months of historical data for comprehensive feature store testing.
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
HASH_SALT = os.getenv('HASH_SALT', 'bulk_generation_salt')

# Constants - Balanced distribution
GEO_COUNTRIES = ['US', 'IN', 'BR', 'DE', 'JP', 'CA', 'AU', 'UK', 'FR', 'IT', 'ES', 'NL', 'SE', 'NO', 'DK', 'FI', 'CH', 'AT', 'BE', 'IE', 'PT', 'GR', 'PL', 'CZ', 'HU', 'RO', 'BG', 'HR', 'SI', 'SK', 'LT', 'LV', 'EE', 'LU', 'MT', 'CY']

# Balanced platform distribution (web: 40%, mobile: 60%)
PLATFORMS = ['web', 'ios', 'android']
PLATFORM_WEIGHTS = [0.4, 0.3, 0.3]  # 40% web, 30% iOS, 30% Android

# Realistic metric ranges
CTR_MIN, CTR_MAX = 0.005, 0.05
CPC_MIN, CPC_MAX = 0.10, 1.20
BOUNCE_RATE_MIN, BOUNCE_RATE_MAX = 0.20, 0.90

# Initialize Faker
fake = Faker()

def generate_historical_timestamp(start_date: datetime, end_date: datetime) -> str:
    """Generate a timestamp within the specified date range."""
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    random_date = start_date + timedelta(days=random_days)
    
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
    return hash_result[:16].lower()

def sample_event(start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Generate a single clickstream event with historical timestamp."""
    # Generate historical timestamp
    timestamp = generate_historical_timestamp(start_date, end_date)
    
    # Generate user ID hash
    user_id_hash = generate_user_id_hash(HASH_SALT)
    
    # Generate ad and campaign IDs
    ad_id = f"ad_{random.randint(1000, 9999)}"
    campaign_id = f"camp_{random.randint(100, 999)}"
    
    # Balanced selections
    geo = random.choice(GEO_COUNTRIES)
    platform = random.choices(PLATFORMS, weights=PLATFORM_WEIGHTS)[0]
    user_agent = fake.user_agent()
    
    # Generate realistic metrics with some correlation
    ctr = random.uniform(CTR_MIN, CTR_MAX)
    cpc = random.uniform(CPC_MIN, CPC_MAX)
    bounce_rate = random.uniform(BOUNCE_RATE_MIN, BOUNCE_RATE_MAX)
    
    # Create event
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp,
        "user_id_hash": user_id_hash,
        "geo": geo,
        "platform": platform,
        "ad_id": ad_id,
        "campaign_id": campaign_id,
        "user_agent": user_agent,
        "ctr": round(ctr, 4),
        "cpc": round(cpc, 2),
        "bounce_rate": round(bounce_rate, 2)
    }
    
    return event

def delivery_report(err, msg):
    """Delivery report handler for Kafka producer."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_bulk_data(months: int = 6, events_per_day: int = 10000):
    """Generate bulk historical data for the specified number of months."""
    
    # Calculate date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=months * 30)
    
    total_events = months * 30 * events_per_day
    print(f"Generating {total_events:,} events from {start_date.date()} to {end_date.date()}")
    print(f"Events per day: {events_per_day:,}")
    print(f"Total months: {months}")
    
    # Initialize Kafka producer
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'client.id': 'bulk-generator'
    })
    
    # Generate events
    events_sent = 0
    start_time = time.time()
    
    try:
        for day in range((end_date - start_date).days):
            current_date = start_date + timedelta(days=day)
            print(f"Generating data for {current_date.date()} ({day + 1}/{(end_date - start_date).days})")
            
            for _ in range(events_per_day):
                event = sample_event(start_date, end_date)
                
                # Validate event
                try:
                    validate_event(event)
                except Exception as e:
                    print(f"Event validation failed: {e}")
                    continue
                
                # Send to Kafka
                producer.produce(
                    TOPIC,
                    json.dumps(event).encode('utf-8'),
                    callback=delivery_report
                )
                
                events_sent += 1
                
                # Flush every 1000 events
                if events_sent % 1000 == 0:
                    producer.flush()
                    elapsed = time.time() - start_time
                    rate = events_sent / elapsed
                    print(f"Sent {events_sent:,} events ({rate:.0f} events/sec)")
        
        # Final flush
        producer.flush()
        
        elapsed = time.time() - start_time
        print(f"\n✅ Bulk generation complete!")
        print(f"Total events sent: {events_sent:,}")
        print(f"Total time: {elapsed:.1f} seconds")
        print(f"Average rate: {events_sent / elapsed:.0f} events/sec")
        
    except KeyboardInterrupt:
        print("\n⚠️ Generation interrupted by user")
        producer.flush()
    except Exception as e:
        print(f"❌ Error during generation: {e}")
        producer.flush()

if __name__ == "__main__":
    # Parse command line arguments
    months = 6  # Default to 6 months
    events_per_day = 10000  # Default to 10k events per day
    
    if len(sys.argv) > 1:
        months = int(sys.argv[1])
    if len(sys.argv) > 2:
        events_per_day = int(sys.argv[2])
    
    print("🚀 Adnomaly Bulk Data Generator")
    print("=" * 50)
    
    generate_bulk_data(months, events_per_day)
