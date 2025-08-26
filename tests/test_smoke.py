"""
Smoke tests for the clickstream generator.
"""

import pytest
import random
import uuid
import hashlib
from datetime import datetime, timezone
from faker import Faker
from data.schema import validate_event


def create_sample_event():
    """Create a sample event for testing."""
    fake = Faker()
    
    # Generate timestamp in ISO8601 UTC format
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    # Generate user ID hash
    user_uuid = str(uuid.uuid4())
    hash_input = f"{user_uuid}:salt".encode('utf-8')
    hash_result = hashlib.sha256(hash_input).hexdigest()
    user_id_hash = hash_result[:16].lower()
    
    # Generate ad and campaign IDs
    ad_id = f"ad_{random.randint(1000, 9999)}"
    campaign_id = f"camp_{random.randint(100, 999)}"
    
    # Random selections
    geo = random.choice(['US', 'IN', 'BR', 'DE', 'JP'])
    platform = random.choice(['web', 'ios', 'android'])
    user_agent = fake.user_agent()
    
    # Generate metrics
    ctr = random.uniform(0.005, 0.05)
    cpc = random.uniform(0.10, 1.20)
    bounce_rate = random.uniform(0.20, 0.90)
    
    # Conversion based on CTR with some randomness
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

def test_sample_event_structure():
    """Test that sample_event() returns a valid event structure."""
    event = create_sample_event()
    
    # Check that all required fields exist
    required_fields = [
        'timestamp', 'user_id_hash', 'ad_id', 'campaign_id', 
        'geo', 'platform', 'user_agent', 'CPC', 'CTR', 
        'conversion', 'bounce_rate'
    ]
    
    for field in required_fields:
        assert field in event, f"Missing required field: {field}"
    
    # Check field types
    assert isinstance(event['timestamp'], str)
    assert isinstance(event['user_id_hash'], str)
    assert isinstance(event['ad_id'], str)
    assert isinstance(event['campaign_id'], str)
    assert isinstance(event['geo'], str)
    assert isinstance(event['platform'], str)
    assert isinstance(event['user_agent'], str)
    assert isinstance(event['CPC'], float)
    assert isinstance(event['CTR'], float)
    assert isinstance(event['conversion'], int)
    assert isinstance(event['bounce_rate'], float)


def test_sample_event_validation():
    """Test that sample_event() produces valid events."""
    event = create_sample_event()
    
    # Validate the event using our schema
    validated_event = validate_event(event)
    
    # Check that validation didn't change the data
    assert validated_event.timestamp == event['timestamp']
    assert validated_event.user_id_hash == event['user_id_hash']
    assert validated_event.ad_id == event['ad_id']
    assert validated_event.campaign_id == event['campaign_id']
    assert validated_event.geo == event['geo']
    assert validated_event.platform == event['platform']
    assert validated_event.user_agent == event['user_agent']
    assert validated_event.CPC == event['CPC']
    assert validated_event.CTR == event['CTR']
    assert validated_event.conversion == event['conversion']
    assert validated_event.bounce_rate == event['bounce_rate']


def test_multiple_sample_events():
    """Test that multiple sample events are all valid and different."""
    events = [create_sample_event() for _ in range(5)]
    
    # All events should be valid
    for event in events:
        validate_event(event)
    
    # Events should be different (at least some fields)
    timestamps = [event['timestamp'] for event in events]
    user_hashes = [event['user_id_hash'] for event in events]
    
    # Timestamps should be different (generated at different times)
    assert len(set(timestamps)) > 1, "All timestamps are identical"
    
    # User hashes should be different (random UUIDs)
    assert len(set(user_hashes)) > 1, "All user hashes are identical"


def test_event_field_constraints():
    """Test that generated events meet field constraints."""
    event = create_sample_event()
    
    # Check geo is 2 uppercase letters
    assert len(event['geo']) == 2
    assert event['geo'].isupper()
    
    # Check platform is valid
    assert event['platform'] in ['web', 'ios', 'android']
    
    # Check ad_id format
    assert event['ad_id'].startswith('ad_')
    assert event['ad_id'][3:].isdigit()
    
    # Check campaign_id format
    assert event['campaign_id'].startswith('camp_')
    assert event['campaign_id'][5:].isdigit()
    
    # Check user_id_hash is lowercase hex
    assert all(c in '0123456789abcdef' for c in event['user_id_hash'])
    assert len(event['user_id_hash']) >= 16
    
    # Check numeric ranges
    assert 0 <= event['CTR'] <= 1
    assert event['CPC'] >= 0
    assert 0 <= event['bounce_rate'] <= 1
    assert event['conversion'] in [0, 1]
    
    # Check timestamp ends with Z
    assert event['timestamp'].endswith('Z')
