"""
Tests for the streaming contract.
"""

import pytest
import json
from unittest.mock import Mock, patch, AsyncMock
from streaming.schemas import FeatureMsg, ServingResponse, AlertMsg


def test_feature_msg_valid():
    """Test that a valid FeatureMsg passes validation."""
    valid_feature = {
        "window_start": "2024-01-15T10:30:00.000Z",
        "window_end": "2024-01-15T10:35:00.000Z",
        "geo": "US",
        "platform": "web",
        "ctr_avg": 0.025,
        "bounce_rate_avg": 0.45,
        "event_count": 150
    }
    
    feature = FeatureMsg(**valid_feature)
    assert feature.geo == "US"
    assert feature.platform == "web"
    assert feature.ctr_avg == 0.025
    assert feature.bounce_rate_avg == 0.45
    assert feature.event_count == 150


def test_feature_msg_invalid_geo():
    """Test that invalid geo fails validation."""
    invalid_feature = {
        "window_start": "2024-01-15T10:30:00.000Z",
        "window_end": "2024-01-15T10:35:00.000Z",
        "geo": "usa",  # Invalid: lowercase
        "platform": "web",
        "ctr_avg": 0.025,
        "bounce_rate_avg": 0.45,
        "event_count": 150
    }
    
    with pytest.raises(ValueError, match="geo must be exactly 2 uppercase letters"):
        FeatureMsg(**invalid_feature)


def test_feature_msg_invalid_platform():
    """Test that invalid platform fails validation."""
    invalid_feature = {
        "window_start": "2024-01-15T10:30:00.000Z",
        "window_end": "2024-01-15T10:35:00.000Z",
        "geo": "US",
        "platform": "desktop",  # Invalid: not in allowed list
        "ctr_avg": 0.025,
        "bounce_rate_avg": 0.45,
        "event_count": 150
    }
    
    with pytest.raises(ValueError, match="platform must be one of: web, ios, android"):
        FeatureMsg(**invalid_feature)


def test_feature_msg_from_kafka():
    """Test parsing FeatureMsg from Kafka message bytes."""
    valid_data = {
        "window_start": "2024-01-15T10:30:00.000Z",
        "window_end": "2024-01-15T10:35:00.000Z",
        "geo": "US",
        "platform": "web",
        "ctr_avg": 0.025,
        "bounce_rate_avg": 0.45,
        "event_count": 150
    }
    
    message_bytes = json.dumps(valid_data).encode('utf-8')
    feature = FeatureMsg.from_kafka(message_bytes)
    
    assert feature.geo == "US"
    assert feature.platform == "web"


def test_feature_msg_from_kafka_invalid():
    """Test that invalid Kafka message raises error."""
    invalid_message = b"invalid json"
    
    with pytest.raises(ValueError, match="Failed to parse Kafka message"):
        FeatureMsg.from_kafka(invalid_message)


def test_feature_msg_to_serving_request():
    """Test conversion to serving API request format."""
    feature = FeatureMsg(
        window_start="2024-01-15T10:30:00.000Z",
        window_end="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150
    )
    
    request = feature.to_serving_request()
    
    assert request["ctr_avg"] == 0.025
    assert request["bounce_rate_avg"] == 0.45
    assert request["event_count"] == 150
    assert request["geo"] == "US"
    assert request["platform"] == "web"
    assert request["timestamp"] == "2024-01-15T10:35:00.000Z"


def test_alert_msg_from_feature_and_response():
    """Test creating AlertMsg from FeatureMsg and ServingResponse."""
    feature = FeatureMsg(
        window_start="2024-01-15T10:30:00.000Z",
        window_end="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150
    )
    
    response = ServingResponse(
        anomaly_score=1.2,
        is_anomaly=True,
        threshold=0.61,
        features_used=["ctr_avg", "bounce_rate_avg", "event_count"],
        meta={"geo": "US", "platform": "web"}
    )
    
    alert = AlertMsg.from_feature_and_response(feature, response)
    
    assert alert.ts == "2024-01-15T10:35:00.000Z"
    assert alert.geo == "US"
    assert alert.platform == "web"
    assert alert.ctr_avg == 0.025
    assert alert.bounce_rate_avg == 0.45
    assert alert.event_count == 150
    assert alert.anomaly_score == 1.2
    assert alert.threshold == 0.61
    assert alert.source_window["start"] == "2024-01-15T10:30:00.000Z"
    assert alert.source_window["end"] == "2024-01-15T10:35:00.000Z"


def test_alert_msg_to_kafka():
    """Test conversion of AlertMsg to Kafka message bytes."""
    alert = AlertMsg(
        ts="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150,
        anomaly_score=1.2,
        threshold=0.61,
        source_window={
            "start": "2024-01-15T10:30:00.000Z",
            "end": "2024-01-15T10:35:00.000Z"
        }
    )
    
    message_bytes = alert.to_kafka()
    data = json.loads(message_bytes.decode('utf-8'))
    
    assert data["ts"] == "2024-01-15T10:35:00.000Z"
    assert data["geo"] == "US"
    assert data["anomaly_score"] == 1.2


@pytest.mark.asyncio
@patch('streaming.scorer.httpx.AsyncClient')
async def test_call_serving_api_success(mock_client_class):
    """Test successful serving API call."""
    # Mock the async client
    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client
    
    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "anomaly_score": 1.2,
        "is_anomaly": True,
        "threshold": 0.61,
        "features_used": ["ctr_avg", "bounce_rate_avg", "event_count"],
        "meta": {"geo": "US", "platform": "web"}
    }
    mock_client.post.return_value = mock_response
    
    # Import here to avoid circular imports
    from streaming.scorer import call_serving_api
    
    feature = FeatureMsg(
        window_start="2024-01-15T10:30:00.000Z",
        window_end="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150
    )
    
    response = await call_serving_api(feature)
    
    assert response is not None
    assert response.anomaly_score == 1.2
    assert response.is_anomaly is True
    assert response.threshold == 0.61


@pytest.mark.asyncio
@patch('streaming.scorer.httpx.AsyncClient')
async def test_call_serving_api_retry_success(mock_client_class):
    """Test serving API call with retry on failure then success."""
    # Mock the async client
    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client
    
    # Mock two failures then success
    mock_response_fail = Mock()
    mock_response_fail.status_code = 500
    
    mock_response_success = Mock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        "anomaly_score": 1.2,
        "is_anomaly": True,
        "threshold": 0.61,
        "features_used": ["ctr_avg", "bounce_rate_avg", "event_count"],
        "meta": {"geo": "US", "platform": "web"}
    }
    
    mock_client.post.side_effect = [mock_response_fail, mock_response_fail, mock_response_success]
    
    # Import here to avoid circular imports
    from streaming.scorer import call_serving_api
    
    feature = FeatureMsg(
        window_start="2024-01-15T10:30:00.000Z",
        window_end="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150
    )
    
    response = await call_serving_api(feature)
    
    assert response is not None
    assert response.anomaly_score == 1.2
    assert mock_client.post.call_count == 3


@pytest.mark.asyncio
@patch('streaming.scorer.httpx.AsyncClient')
async def test_call_serving_api_max_retries_exceeded(mock_client_class):
    """Test serving API call with max retries exceeded."""
    # Mock the async client
    mock_client = AsyncMock()
    mock_client_class.return_value = mock_client
    
    # Mock repeated failures
    mock_response_fail = Mock()
    mock_response_fail.status_code = 500
    mock_client.post.return_value = mock_response_fail
    
    # Import here to avoid circular imports
    from streaming.scorer import call_serving_api
    
    feature = FeatureMsg(
        window_start="2024-01-15T10:30:00.000Z",
        window_end="2024-01-15T10:35:00.000Z",
        geo="US",
        platform="web",
        ctr_avg=0.025,
        bounce_rate_avg=0.45,
        event_count=150
    )
    
    response = await call_serving_api(feature)
    
    assert response is None
    # Should have tried RETRY_MAX + 1 times (default 4)
    assert mock_client.post.call_count == 4
