"""
Tests for schema validation.
"""

import pytest
from datetime import datetime, timezone

from data.schema import validate_event, ClickEvent


def test_valid_event():
    """Test that a valid event passes validation."""
    valid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    result = validate_event(valid_event)
    assert isinstance(result, ClickEvent)
    assert result.timestamp == valid_event['timestamp']
    assert result.user_id_hash == valid_event['user_id_hash']
    assert result.ad_id == valid_event['ad_id']
    assert result.campaign_id == valid_event['campaign_id']
    assert result.geo == valid_event['geo']
    assert result.platform == valid_event['platform']
    assert result.user_agent == valid_event['user_agent']
    assert result.CPC == valid_event['CPC']
    assert result.CTR == valid_event['CTR']
    assert result.conversion == valid_event['conversion']
    assert result.bounce_rate == valid_event['bounce_rate']


def test_bad_timestamp_no_z():
    """Test that timestamp without Z suffix fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000',  # Missing Z
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='Timestamp must end with Z'):
        validate_event(invalid_event)


def test_bad_geo_not_uppercase():
    """Test that geo with lowercase letters fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'us',  # Lowercase
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='geo must be exactly 2 uppercase letters'):
        validate_event(invalid_event)


def test_bad_geo_wrong_length():
    """Test that geo with wrong length fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'USA',  # 3 letters
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='geo must be exactly 2 uppercase letters'):
        validate_event(invalid_event)


def test_out_of_range_ctr():
    """Test that CTR > 1 fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 1.5,  # > 1
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError):
        validate_event(invalid_event)


def test_negative_cpc():
    """Test that negative CPC fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': -0.5,  # Negative
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError):
        validate_event(invalid_event)


def test_invalid_conversion():
    """Test that conversion not in {0,1} fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 2,  # Not 0 or 1
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='conversion must be 0 or 1'):
        validate_event(invalid_event)


def test_invalid_ad_id():
    """Test that ad_id with wrong format fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'advertisement_1234',  # Wrong format
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='ad_id must match pattern ad_<digits>'):
        validate_event(invalid_event)


def test_invalid_campaign_id():
    """Test that campaign_id with wrong format fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'campaign_567',  # Wrong format
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='campaign_id must match pattern camp_<digits>'):
        validate_event(invalid_event)


def test_invalid_user_id_hash():
    """Test that user_id_hash with wrong format fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'invalid_hash',  # Not hex
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': 'Mozilla/5.0',
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='user_id_hash must be 16-64 lowercase hex characters'):
        validate_event(invalid_event)


def test_empty_user_agent():
    """Test that empty user_agent fails validation."""
    invalid_event = {
        'timestamp': '2024-01-15T10:30:00.000Z',
        'user_id_hash': 'a1b2c3d4e5f67890',
        'ad_id': 'ad_1234',
        'campaign_id': 'camp_567',
        'geo': 'US',
        'platform': 'web',
        'user_agent': '',  # Empty
        'CPC': 0.75,
        'CTR': 0.025,
        'conversion': 0,
        'bounce_rate': 0.45
    }
    
    with pytest.raises(ValueError, match='user_agent cannot be empty'):
        validate_event(invalid_event)
