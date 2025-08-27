import json
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class FeatureMsg(BaseModel):
    """Feature message from Kafka."""
    window_start: str = Field(..., description="ISO8601 UTC timestamp")
    window_end: str = Field(..., description="ISO8601 UTC timestamp")
    geo: str = Field(..., description="2-letter country code")
    platform: str = Field(..., description="Platform: web, ios, or android")
    ctr_avg: float = Field(..., ge=0.0, le=1.0, description="Average click-through rate")
    bounce_rate_avg: float = Field(..., ge=0.0, le=1.0, description="Average bounce rate")
    event_count: int = Field(..., ge=1, description="Event count")

    @field_validator('geo')
    @classmethod
    def validate_geo(cls, v):
        if not v or len(v) != 2 or not v.isupper():
            raise ValueError('geo must be exactly 2 uppercase letters')
        return v

    @field_validator('platform')
    @classmethod
    def validate_platform(cls, v):
        if v not in ['web', 'ios', 'android']:
            raise ValueError('platform must be one of: web, ios, android')
        return v

    @field_validator('window_start', 'window_end')
    @classmethod
    def validate_timestamp(cls, v):
        if not v.endswith('Z'):
            raise ValueError('Timestamp must end with Z (UTC)')
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('Invalid timestamp format')
        return v

    @classmethod
    def from_kafka(cls, message_bytes: bytes) -> 'FeatureMsg':
        """Parse Kafka message bytes to FeatureMsg."""
        try:
            message_str = message_bytes.decode('utf-8')
            data = json.loads(message_str)
            return cls(**data)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Failed to parse Kafka message: {e}")

    def to_serving_request(self) -> dict:
        """Convert to serving API request format."""
        return {
            "ctr_avg": self.ctr_avg,
            "bounce_rate_avg": self.bounce_rate_avg,
            "event_count": self.event_count,
            "geo": self.geo,
            "platform": self.platform,
            "timestamp": self.window_end
        }


class ServingResponse(BaseModel):
    """Response from serving API."""
    anomaly_score: float = Field(..., description="Anomaly score (higher = more anomalous)")
    is_anomaly: bool = Field(..., description="Whether score exceeds threshold")
    threshold: float = Field(..., description="Threshold used for anomaly detection")
    features_used: List[str] = Field(..., description="Features used for scoring")
    meta: Optional[dict] = Field(None, description="Passthrough metadata")


class AlertMsg(BaseModel):
    """Alert message for Kafka."""
    ts: str = Field(..., description="Timestamp (window_end)")
    geo: str = Field(..., description="Geographic region")
    platform: str = Field(..., description="Platform")
    ctr_avg: float = Field(..., description="Average click-through rate")
    bounce_rate_avg: float = Field(..., description="Average bounce rate")
    event_count: int = Field(..., description="Event count")
    anomaly_score: float = Field(..., description="Anomaly score")
    threshold: float = Field(..., description="Threshold used")
    source_window: dict = Field(..., description="Source window with start and end")

    @classmethod
    def from_feature_and_response(cls, feature: FeatureMsg, response: ServingResponse) -> 'AlertMsg':
        """Create AlertMsg from FeatureMsg and ServingResponse."""
        return cls(
            ts=feature.window_end,
            geo=feature.geo,
            platform=feature.platform,
            ctr_avg=feature.ctr_avg,
            bounce_rate_avg=feature.bounce_rate_avg,
            event_count=feature.event_count,
            anomaly_score=response.anomaly_score,
            threshold=response.threshold,
            source_window={
                "start": feature.window_start,
                "end": feature.window_end
            }
        )

    def to_kafka(self) -> bytes:
        """Convert to Kafka message bytes."""
        return json.dumps(self.model_dump()).encode('utf-8')
