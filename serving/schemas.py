from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class ScoreRequest(BaseModel):
    """Request schema for single row scoring."""
    ctr_avg: float = Field(..., ge=0.0, le=1.0, description="Click-through rate average (0..1)")
    bounce_rate_avg: float = Field(..., ge=0.0, le=1.0, description="Bounce rate average (0..1)")
    event_count: int = Field(..., ge=1, description="Event count (raw, not log)")
    
    # Optional passthrough fields
    geo: Optional[str] = Field(None, description="Geographic region (ignored for scoring)")
    platform: Optional[str] = Field(None, description="Platform (ignored for scoring)")
    timestamp: Optional[str] = Field(None, description="Timestamp (ignored for scoring)")


class ScoreBatchRequest(BaseModel):
    """Request schema for batch scoring."""
    rows: List[ScoreRequest] = Field(..., description="Up to 1000 rows to score")


class ScoreResponse(BaseModel):
    """Response schema for scoring."""
    anomaly_score: float = Field(..., description="Anomaly score (higher = more anomalous)")
    is_anomaly: bool = Field(..., description="Whether the score exceeds threshold")
    threshold: float = Field(..., description="Threshold used for anomaly detection")
    features_used: List[str] = Field(..., description="Features used for scoring")
    meta: Optional[Dict[str, Any]] = Field(None, description="Passthrough metadata if provided")


class ScoreBatchResponse(BaseModel):
    """Response schema for batch scoring."""
    results: List[ScoreResponse] = Field(..., description="Scoring results for each row")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Service status")
