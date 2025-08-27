"""
Tests for temporal variation in the data generator.
"""

import pytest
from datetime import datetime, timezone
import sys
import os

# Add the data directory to the path to import generator functions
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data'))

# Import the functions we want to test
from generator import diurnal_mult, weekend_mult, current_eps, CTR_BASE, CTR_DIURNAL_AMPL, BOUNCE_BASE, BOUNCE_DIURNAL_AMPL


def test_diurnal_patterns():
    """Test that diurnal patterns work correctly."""
    # Test peak hour (20:00 UTC)
    peak_time = datetime(2024, 1, 15, 20, 0, 0, tzinfo=timezone.utc)
    # Test trough hour (08:00 UTC - 12 hours opposite to peak)
    trough_time = datetime(2024, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
    
    # Test EPS variation
    peak_eps = current_eps(peak_time)
    trough_eps = current_eps(trough_time)
    
    assert peak_eps > trough_eps, f"Peak EPS ({peak_eps}) should be higher than trough EPS ({trough_eps})"
    
    # Test CTR variation
    peak_ctr_mu = CTR_BASE * diurnal_mult(peak_time, CTR_DIURNAL_AMPL)
    trough_ctr_mu = CTR_BASE * diurnal_mult(trough_time, CTR_DIURNAL_AMPL)
    
    assert peak_ctr_mu > trough_ctr_mu, f"Peak CTR ({peak_ctr_mu}) should be higher than trough CTR ({trough_ctr_mu})"
    
    # Test bounce rate variation (inverse relationship)
    peak_bounce_mu = BOUNCE_BASE / max(0.6, diurnal_mult(peak_time, BOUNCE_DIURNAL_AMPL))
    trough_bounce_mu = BOUNCE_BASE / max(0.6, diurnal_mult(trough_time, BOUNCE_DIURNAL_AMPL))
    
    assert peak_bounce_mu < trough_bounce_mu, f"Peak bounce ({peak_bounce_mu}) should be lower than trough bounce ({trough_bounce_mu})"


def test_weekend_multiplier():
    """Test that weekend multiplier works correctly."""
    # Test weekday (Monday)
    weekday_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)  # Monday
    # Test weekend (Saturday)
    weekend_time = datetime(2024, 1, 20, 12, 0, 0, tzinfo=timezone.utc)  # Saturday
    
    weekday_mult = weekend_mult(weekday_time)
    weekend_mult_val = weekend_mult(weekend_time)
    
    assert weekday_mult == 1.0, f"Weekday multiplier should be 1.0, got {weekday_mult}"
    assert weekend_mult_val < 1.0, f"Weekend multiplier should be less than 1.0, got {weekend_mult_val}"


def test_diurnal_multiplier_range():
    """Test that diurnal multiplier stays within expected range."""
    # Test a full day cycle
    for hour in range(24):
        test_time = datetime(2024, 1, 15, hour, 0, 0, tzinfo=timezone.utc)
        mult = diurnal_mult(test_time, 0.6)  # 60% amplitude
        
        # Should be between 0.4 and 1.6 (1.0 ± 0.6)
        assert 0.4 <= mult <= 1.6, f"Diurnal multiplier at hour {hour} is {mult}, should be between 0.4 and 1.6"


def test_peak_hour_behavior():
    """Test that peak hour has maximum multiplier."""
    # Test around peak hour (20:00 UTC)
    peak_hour = 20
    peak_time = datetime(2024, 1, 15, peak_hour, 0, 0, tzinfo=timezone.utc)
    peak_mult = diurnal_mult(peak_time, 0.6)
    
    # Test hour before peak
    before_peak = datetime(2024, 1, 15, peak_hour - 1, 0, 0, tzinfo=timezone.utc)
    before_mult = diurnal_mult(before_peak, 0.6)
    
    # Test hour after peak
    after_peak = datetime(2024, 1, 15, peak_hour + 1, 0, 0, tzinfo=timezone.utc)
    after_mult = diurnal_mult(after_peak, 0.6)
    
    # Peak should be higher than adjacent hours
    assert peak_mult >= before_mult, f"Peak multiplier ({peak_mult}) should be >= before peak ({before_mult})"
    assert peak_mult >= after_mult, f"Peak multiplier ({peak_mult}) should be >= after peak ({after_mult})"


def test_metric_correlation():
    """Test that CTR and bounce rate have expected correlation with time."""
    # Test at peak time
    peak_time = datetime(2024, 1, 15, 20, 0, 0, tzinfo=timezone.utc)
    
    # At peak time, CTR should be higher and bounce should be lower
    ctr_mu = CTR_BASE * diurnal_mult(peak_time, CTR_DIURNAL_AMPL)
    bounce_mu = BOUNCE_BASE / max(0.6, diurnal_mult(peak_time, BOUNCE_DIURNAL_AMPL))
    
    # CTR should be above baseline at peak
    assert ctr_mu > CTR_BASE, f"Peak CTR ({ctr_mu}) should be above baseline ({CTR_BASE})"
    
    # Bounce should be below baseline at peak
    assert bounce_mu < BOUNCE_BASE, f"Peak bounce ({bounce_mu}) should be below baseline ({BOUNCE_BASE})"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
