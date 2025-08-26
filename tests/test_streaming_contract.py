"""
Contract tests for streaming functionality.
Tests the parsing and formatting logic without requiring Flink runtime.
"""

import json
import pytest
from datetime import datetime, timezone
from streaming.job import parse_clickstream_event, ClickstreamEvent, format_window_result


def test_parse_valid_clickstream_event():
    """Test parsing a valid clickstream event."""
    valid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "ios",
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)...",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    event = parse_clickstream_event(json.dumps(valid_event_json))
    
    assert event is not None
    assert isinstance(event, ClickstreamEvent)
    assert event.geo == "US"
    assert event.platform == "ios"
    assert event.ctr == 0.019
    assert event.bounce_rate == 0.61
    assert event.timestamp == datetime(2025, 8, 26, 14, 3, 12, tzinfo=timezone.utc)


def test_parse_invalid_timestamp():
    """Test parsing event with invalid timestamp."""
    invalid_event_json = {
        "timestamp": "2025-08-26T14:03:12",  # Missing Z
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "ios",
        "user_agent": "Mozilla/5.0",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    event = parse_clickstream_event(json.dumps(invalid_event_json))
    assert event is None


def test_parse_invalid_geo():
    """Test parsing event with invalid geo."""
    invalid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "usa",  # Lowercase, wrong length
        "platform": "ios",
        "user_agent": "Mozilla/5.0",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    event = parse_clickstream_event(json.dumps(invalid_event_json))
    assert event is None


def test_parse_invalid_platform():
    """Test parsing event with invalid platform."""
    invalid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "desktop",  # Invalid platform
        "user_agent": "Mozilla/5.0",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    event = parse_clickstream_event(json.dumps(invalid_event_json))
    assert event is None


def test_parse_invalid_ctr():
    """Test parsing event with invalid CTR."""
    invalid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "ios",
        "user_agent": "Mozilla/5.0",
        "CPC": 0.42,
        "CTR": 1.5,  # > 1
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    event = parse_clickstream_event(json.dumps(invalid_event_json))
    assert event is None


def test_parse_invalid_bounce_rate():
    """Test parsing event with invalid bounce_rate."""
    invalid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "ios",
        "user_agent": "Mozilla/5.0",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": -0.1  # < 0
    }
    
    event = parse_clickstream_event(json.dumps(invalid_event_json))
    assert event is None


def test_parse_invalid_json():
    """Test parsing invalid JSON."""
    event = parse_clickstream_event("invalid json")
    assert event is None


def test_format_window_result():
    """Test formatting window result as JSON."""
    # Mock window data: (key, (ctr_avg, bounce_rate_avg, event_count), window_start, window_end)
    window_data = (
        ("US", "ios"),  # key (geo, platform)
        (0.025, 0.65, 150),  # aggregated values (ctr_avg, bounce_rate_avg, event_count)
        1693065600000,  # window_start (milliseconds)
        1693065900000   # window_end (milliseconds)
    )
    
    result_json = format_window_result(window_data)
    result = json.loads(result_json)
    
    # Check required fields exist
    required_fields = ['window_start', 'window_end', 'geo', 'platform', 'ctr_avg', 'bounce_rate_avg', 'event_count']
    for field in required_fields:
        assert field in result, f"Missing required field: {field}"
    
    # Check field types
    assert isinstance(result['window_start'], str)
    assert isinstance(result['window_end'], str)
    assert isinstance(result['geo'], str)
    assert isinstance(result['platform'], str)
    assert isinstance(result['ctr_avg'], float)
    assert isinstance(result['bounce_rate_avg'], float)
    assert isinstance(result['event_count'], int)
    
    # Check field values
    assert result['geo'] == "US"
    assert result['platform'] == "ios"
    assert result['ctr_avg'] == 0.025
    assert result['bounce_rate_avg'] == 0.65
    assert result['event_count'] == 150
    
    # Check timestamp format (should end with Z)
    assert result['window_start'].endswith('Z')
    assert result['window_end'].endswith('Z')


def test_format_window_result_zero_events():
    """Test formatting window result with zero events."""
    window_data = (
        ("DE", "web"),
        (0.0, 0.0, 0),  # Zero events
        1693065600000,
        1693065900000
    )
    
    result_json = format_window_result(window_data)
    result = json.loads(result_json)
    
    assert result['ctr_avg'] == 0.0
    assert result['bounce_rate_avg'] == 0.0
    assert result['event_count'] == 0
    assert result['geo'] == "DE"
    assert result['platform'] == "web"


def test_round_trip_json_encoding():
    """Test round-trip JSON encoding/decoding of a valid event."""
    valid_event_json = {
        "timestamp": "2025-08-26T14:03:12Z",
        "user_id_hash": "9c0b6c1e07f3a4e1",
        "ad_id": "ad_281",
        "campaign_id": "camp_12",
        "geo": "US",
        "platform": "ios",
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)...",
        "CPC": 0.42,
        "CTR": 0.019,
        "conversion": 0,
        "bounce_rate": 0.61
    }
    
    # Encode to JSON
    json_str = json.dumps(valid_event_json)
    
    # Parse the event
    event = parse_clickstream_event(json_str)
    
    assert event is not None
    assert event.geo == "US"
    assert event.platform == "ios"
    assert event.ctr == 0.019
    assert event.bounce_rate == 0.61
