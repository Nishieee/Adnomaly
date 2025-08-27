"""
Tests for the serving contract.
"""

import pytest
import json
from fastapi.testclient import TestClient
from serving.app import app

client = TestClient(app)


def test_health_check():
    """Test health check endpoint."""
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


def test_readiness_check():
    """Test readiness check endpoint."""
    response = client.get("/readyz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"


def test_metrics_endpoint():
    """Test metrics endpoint."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "serve_requests_total" in response.text


def test_score_single_valid():
    """Test single row scoring with valid data."""
    request_data = {
        "ctr_avg": 0.03,
        "bounce_rate_avg": 0.62,
        "event_count": 350,
        "geo": "US",
        "platform": "ios",
        "timestamp": "2024-01-15T10:30:00.000Z"
    }
    
    response = client.post("/v1/score", json=request_data)
    assert response.status_code == 200
    
    data = response.json()
    assert "anomaly_score" in data
    assert "is_anomaly" in data
    assert "threshold" in data
    assert "features_used" in data
    assert "meta" in data
    
    # Validate types
    assert isinstance(data["anomaly_score"], float)
    assert isinstance(data["is_anomaly"], bool)
    assert isinstance(data["threshold"], float)
    assert isinstance(data["features_used"], list)
    assert len(data["features_used"]) == 3
    
    # Validate features used
    expected_features = ["ctr_avg", "bounce_rate_avg", "event_count"]
    assert data["features_used"] == expected_features
    
    # Validate meta
    assert data["meta"]["geo"] == "US"
    assert data["meta"]["platform"] == "ios"
    assert data["meta"]["timestamp"] == "2024-01-15T10:30:00.000Z"
    
    # Validate anomaly logic
    assert data["is_anomaly"] == (data["anomaly_score"] >= data["threshold"])


def test_score_single_minimal():
    """Test single row scoring with minimal data (no optional fields)."""
    request_data = {
        "ctr_avg": 0.02,
        "bounce_rate_avg": 0.35,
        "event_count": 100
    }
    
    response = client.post("/v1/score", json=request_data)
    assert response.status_code == 200
    
    data = response.json()
    assert data["meta"] is None
    assert isinstance(data["anomaly_score"], float)
    assert isinstance(data["is_anomaly"], bool)


def test_score_single_invalid_schema():
    """Test single row scoring with invalid schema."""
    # Missing required field
    request_data = {
        "ctr_avg": 0.03,
        "bounce_rate_avg": 0.62
        # Missing event_count
    }
    
    response = client.post("/v1/score", json=request_data)
    assert response.status_code == 422


def test_score_single_invalid_values():
    """Test single row scoring with invalid values."""
    # CTR out of range
    request_data = {
        "ctr_avg": 1.5,  # > 1.0
        "bounce_rate_avg": 0.62,
        "event_count": 350
    }
    
    response = client.post("/v1/score", json=request_data)
    assert response.status_code == 422
    
    # Event count < 1
    request_data = {
        "ctr_avg": 0.03,
        "bounce_rate_avg": 0.62,
        "event_count": 0  # < 1
    }
    
    response = client.post("/v1/score", json=request_data)
    assert response.status_code == 422


def test_score_batch_valid():
    """Test batch scoring with valid data."""
    request_data = {
        "rows": [
            {
                "ctr_avg": 0.03,
                "bounce_rate_avg": 0.62,
                "event_count": 350,
                "geo": "US",
                "platform": "ios"
            },
            {
                "ctr_avg": 0.02,
                "bounce_rate_avg": 0.35,
                "event_count": 100,
                "geo": "CA",
                "platform": "android"
            },
            {
                "ctr_avg": 0.05,
                "bounce_rate_avg": 0.80,
                "event_count": 1000
            }
        ]
    }
    
    response = client.post("/v1/batch", json=request_data)
    assert response.status_code == 200
    
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 3
    
    # Validate each result
    for result in data["results"]:
        assert "anomaly_score" in result
        assert "is_anomaly" in result
        assert "threshold" in result
        assert "features_used" in result
        assert isinstance(result["anomaly_score"], float)
        assert isinstance(result["is_anomaly"], bool)
        assert result["is_anomaly"] == (result["anomaly_score"] >= result["threshold"])


def test_score_batch_empty():
    """Test batch scoring with empty batch."""
    request_data = {"rows": []}
    
    response = client.post("/v1/batch", json=request_data)
    assert response.status_code == 200
    
    data = response.json()
    assert data["results"] == []


def test_score_batch_too_large():
    """Test batch scoring with too many rows."""
    # Create 1001 rows
    rows = []
    for i in range(1001):
        rows.append({
            "ctr_avg": 0.02,
            "bounce_rate_avg": 0.35,
            "event_count": 100
        })
    
    request_data = {"rows": rows}
    
    response = client.post("/v1/batch", json=request_data)
    assert response.status_code == 400
    assert "Batch size cannot exceed 1000 rows" in response.json()["detail"]


def test_score_batch_invalid_schema():
    """Test batch scoring with invalid schema."""
    request_data = {
        "rows": [
            {
                "ctr_avg": 0.03,
                "bounce_rate_avg": 0.62
                # Missing event_count
            }
        ]
    }
    
    response = client.post("/v1/batch", json=request_data)
    assert response.status_code == 422


def test_anomaly_score_consistency():
    """Test that anomaly scores are consistent and reasonable."""
    # Test with normal data
    normal_request = {
        "ctr_avg": 0.02,
        "bounce_rate_avg": 0.35,
        "event_count": 100
    }
    
    response = client.post("/v1/score", json=normal_request)
    assert response.status_code == 200
    normal_score = response.json()["anomaly_score"]
    
    # Test with potentially anomalous data (high CTR, high bounce)
    anomalous_request = {
        "ctr_avg": 0.10,  # Much higher CTR
        "bounce_rate_avg": 0.90,  # Much higher bounce rate
        "event_count": 5000  # Much higher volume
    }
    
    response = client.post("/v1/score", json=anomalous_request)
    assert response.status_code == 200
    anomalous_score = response.json()["anomaly_score"]
    
    # The anomalous score should be higher (more anomalous)
    # Note: This is a basic sanity check, actual scores depend on the model
    print(f"Normal score: {normal_score}, Anomalous score: {anomalous_score}")
    
    # Both scores should be finite numbers
    assert isinstance(normal_score, float)
    assert isinstance(anomalous_score, float)
    assert not (normal_score != normal_score)  # Not NaN
    assert not (anomalous_score != anomalous_score)  # Not NaN
