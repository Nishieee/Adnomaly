#!/usr/bin/env python3
"""
Clickstream event generator for Adnomaly project.
Generates realistic clickstream events with temporal variation and sends them to Kafka.
"""

import json
import math
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
HASH_SALT = os.getenv('HASH_SALT')  # No default required by Phase 1 make target provides one

# Deterministic seed
SEED = int(os.getenv("SEED", "42"))
random.seed(SEED)

# Traffic shaping
BASE_EPS = float(os.getenv("EVENTS_PER_SEC", "120"))  # base; will be modulated
DIURNAL_AMPL = float(os.getenv("DIURNAL_AMPL", "0.6"))  # 0..1; 0.6 = ±60% swing
PEAK_HOUR = int(os.getenv("PEAK_HOUR_UTC", "20"))  # hour of daily max
WEEKEND_MULT = float(os.getenv("WEEKEND_MULT", "0.85"))  # weekend volume factor

# Geo/platform skew (weights must sum ~1 after normalization)
GEO_WEIGHTS = os.getenv("GEO_WEIGHTS", "US:0.30,IN:0.20,BR:0.12,DE:0.08,FR:0.08,UK:0.08,CA:0.06,JP:0.04,IT:0.02,AU:0.02")
PLATFORM_WEIGHTS = os.getenv("PLATFORM_WEIGHTS", "web:0.45,android:0.30,ios:0.25")

# CTR/bounce shaping
CTR_BASE = float(os.getenv("CTR_BASE", "0.022"))       # baseline CTR
CTR_DIURNAL_AMPL = float(os.getenv("CTR_DIURNAL_AMPL", "0.35"))
BOUNCE_BASE = float(os.getenv("BOUNCE_BASE", "0.36"))
BOUNCE_DIURNAL_AMPL = float(os.getenv("BOUNCE_DIURNAL_AMPL", "0.20"))

# Noise
NOISE_STD_CTR = float(os.getenv("NOISE_STD_CTR", "0.002"))
NOISE_STD_BOUNCE = float(os.getenv("NOISE_STD_BOUNCE", "0.02"))
NOISE_STD_CPC = float(os.getenv("NOISE_STD_CPC", "0.05"))

# Initialize Faker
fake = Faker()


def parse_weights(spec: str):
    """Parse weight strings into keys and probabilities."""
    parts = [p.strip() for p in spec.split(",") if p.strip()]
    keys, vals = [], []
    for p in parts:
        k, v = p.split(":")
        keys.append(k.strip())
        vals.append(float(v))
    s = sum(vals)
    probs = [v/s for v in vals]
    return keys, probs


GEO_KEYS, GEO_PROBS = parse_weights(GEO_WEIGHTS)
PLATFORM_KEYS, PLATFORM_PROBS = parse_weights(PLATFORM_WEIGHTS)


def diurnal_mult(dt, ampl: float):
    """Compute diurnal multiplier (cosine curve, peak at PEAK_HOUR)."""
    # dt is aware UTC datetime
    # cosine: 1 + ampl * cos(2π * (hour - peak)/24)
    hour = dt.hour + dt.minute/60.0
    phase = 2*math.pi*((hour - PEAK_HOUR) / 24.0)
    return max(0.0, 1.0 + ampl*math.cos(phase))


def weekend_mult(dt):
    """Compute weekend volume multiplier."""
    return WEEKEND_MULT if dt.weekday() >= 5 else 1.0  # 5=Sat,6=Sun


def jitter(mu, std, lo=None, hi=None):
    """Add thin noise to a value."""
    x = mu + random.gauss(0, std)
    if lo is not None: x = max(lo, x)
    if hi is not None: x = min(hi, x)
    return x


def current_eps(now_utc):
    """Compute current events per second based on time."""
    return BASE_EPS * diurnal_mult(now_utc, DIURNAL_AMPL) * weekend_mult(now_utc)


def generate_user_id_hash(salt: str) -> str:
    """Generate a stable user ID hash from a UUID and salt."""
    user_uuid = str(uuid.uuid4())
    hash_input = f"{user_uuid}:{salt}".encode('utf-8')
    hash_result = hashlib.sha256(hash_input).hexdigest()
    return hash_result[:16].lower()  # Take first 16 hex chars, ensure lowercase


def sample_event(now_utc: datetime) -> Dict[str, Any]:
    """Generate a single clickstream event with temporal variation."""
    # Generate user ID hash
    user_id_hash = generate_user_id_hash(HASH_SALT)
    
    # Generate ad and campaign IDs
    ad_id = f"ad_{random.randint(1000, 9999)}"
    campaign_id = f"camp_{random.randint(100, 999)}"
    
    # Weighted selections
    geo = random.choices(GEO_KEYS, GEO_PROBS, k=1)[0]
    platform = random.choices(PLATFORM_KEYS, PLATFORM_PROBS, k=1)[0]
    user_agent = fake.user_agent()
    
    # Shaping CTR/bounce with time-of-day
    ctr_mu = CTR_BASE * diurnal_mult(now_utc, CTR_DIURNAL_AMPL)
    bounce_mu = BOUNCE_BASE / max(0.6, diurnal_mult(now_utc, BOUNCE_DIURNAL_AMPL))  # lower bounce at peak time
    ctr = jitter(ctr_mu, NOISE_STD_CTR, lo=0.001, hi=1.0)
    bounce = jitter(bounce_mu, NOISE_STD_BOUNCE, lo=0.05, hi=0.98)
    
    # CPC with light noise (keep ≥0)
    base_cpc = 0.25 if platform == "web" else 0.35  # tiny platform effect
    cpc = max(0.01, jitter(base_cpc, NOISE_STD_CPC))
    
    # Conversion probability tied to CTR (bounded)
    p_conv = max(0.0005, min(0.25, ctr * random.uniform(0.1, 0.8)))
    conversion = 1 if random.random() < p_conv else 0
    
    return {
        'timestamp': now_utc.isoformat().replace('+00:00', 'Z'),
        'user_id_hash': user_id_hash,
        'ad_id': ad_id,
        'campaign_id': campaign_id,
        'geo': geo,
        'platform': platform,
        'user_agent': user_agent,
        'CPC': round(cpc, 3),
        'CTR': round(ctr, 4),
        'conversion': conversion,
        'bounce_rate': round(bounce, 3)
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
    print(f"Starting clickstream generator with temporal variation...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Base EPS: {BASE_EPS}")
    print(f"Diurnal amplitude: {DIURNAL_AMPL}")
    print(f"Peak hour UTC: {PEAK_HOUR}")
    print(f"Weekend multiplier: {WEEKEND_MULT}")
    print(f"Hash salt: {HASH_SALT}")
    print("Press Ctrl+C to stop")
    
    # Create producer
    producer = create_producer()
    
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
            now = datetime.now(timezone.utc)
            eps = max(1.0, current_eps(now))
            period = 1.0 / eps
            
            # Generate and send event
            event = sample_event(now)
            send_event(producer, event)
            events_sent += 1
            
            # Report progress every ~5 seconds
            current_time = time.time()
            if current_time - last_report_time >= 5.0:
                elapsed = current_time - last_report_time
                actual_eps = events_sent / elapsed if elapsed > 0 else 0
                print(f"Sent {events_sent} events in {elapsed:.1f}s ({actual_eps:.1f} events/sec, target: {eps:.1f})")
                events_sent = 0
                last_report_time = current_time
            
            # Sleep to maintain variable rate
            time.sleep(period)
                
    except KeyboardInterrupt:
        print(f"\nShutting down... Sent {events_sent} events total")
    except Exception as e:
        print(f"Error in main loop: {e}", file=sys.stderr)
    finally:
        producer.close()


if __name__ == '__main__':
    main()
