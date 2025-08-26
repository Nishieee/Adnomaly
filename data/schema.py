import re
from typing import Literal
from pydantic import BaseModel, Field, field_validator
from dateutil import parser
from datetime import datetime


class ClickEvent(BaseModel):
    timestamp: str = Field(..., description="ISO8601 UTC timestamp ending with Z")
    user_id_hash: str = Field(..., description="SHA-256 hash of user ID (16-64 hex chars)")
    ad_id: str = Field(..., description="Ad ID with format ad_<digits>")
    campaign_id: str = Field(..., description="Campaign ID with format camp_<digits>")
    geo: str = Field(..., description="2-letter uppercase country code")
    platform: Literal["web", "ios", "android"] = Field(..., description="Platform type")
    user_agent: str = Field(..., description="User agent string")
    CPC: float = Field(..., ge=0, description="Cost per click (typically 0.05-2.0)")
    CTR: float = Field(..., ge=0, le=1, description="Click-through rate (0.0-1.0)")
    conversion: int = Field(..., description="Conversion flag (0 or 1)")
    bounce_rate: float = Field(..., ge=0, le=1, description="Bounce rate (0.0-1.0)")

    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v):
        if not v.endswith('Z'):
            raise ValueError('Timestamp must end with Z (UTC)')
        try:
            dt = parser.parse(v)
            # Check if it's a valid datetime
            if dt.tzinfo is None:
                raise ValueError('Timestamp must include timezone information')
        except Exception as e:
            raise ValueError(f'Invalid timestamp format: {e}')
        return v

    @field_validator('user_id_hash')
    @classmethod
    def validate_user_id_hash(cls, v):
        if not re.match(r'^[0-9a-f]{16,64}$', v):
            raise ValueError('user_id_hash must be 16-64 lowercase hex characters')
        return v

    @field_validator('ad_id')
    @classmethod
    def validate_ad_id(cls, v):
        if not re.match(r'^ad_\d+$', v):
            raise ValueError('ad_id must match pattern ad_<digits>')
        return v

    @field_validator('campaign_id')
    @classmethod
    def validate_campaign_id(cls, v):
        if not re.match(r'^camp_\d+$', v):
            raise ValueError('campaign_id must match pattern camp_<digits>')
        return v

    @field_validator('geo')
    @classmethod
    def validate_geo(cls, v):
        if not re.match(r'^[A-Z]{2}$', v):
            raise ValueError('geo must be exactly 2 uppercase letters')
        return v

    @field_validator('user_agent')
    @classmethod
    def validate_user_agent(cls, v):
        if not v or not v.strip():
            raise ValueError('user_agent cannot be empty')
        return v

    @field_validator('conversion')
    @classmethod
    def validate_conversion(cls, v):
        if v not in [0, 1]:
            raise ValueError('conversion must be 0 or 1')
        return v


def validate_event(d: dict) -> ClickEvent:
    """Validate a dictionary and return a ClickEvent model.
    
    Args:
        d: Dictionary containing event data
        
    Returns:
        ClickEvent: Validated event model
        
    Raises:
        ValueError: If validation fails
    """
    return ClickEvent(**d)
