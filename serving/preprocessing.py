import json
import numpy as np
from typing import List, Tuple
from .config import config


class Preprocessor:
    """Preprocessor that applies the exact same transformations as training."""
    
    def __init__(self):
        # Load preprocessing parameters
        with open(config.preprocess_path, 'r') as f:
            preprocess_params = json.load(f)
        
        self.mean = np.array(preprocess_params['mean'], dtype=np.float32)
        self.scale = np.array(preprocess_params['scale'], dtype=np.float32)
        self.cnt_cap = float(preprocess_params['cnt_cap'])
        
        print(f"Loaded preprocessor: mean={self.mean}, scale={self.scale}, cnt_cap={self.cnt_cap}")
    
    def prepare_row(self, ctr_avg: float, bounce_rate_avg: float, event_count: int) -> np.ndarray:
        """
        Prepare a single row for inference.
        
        Args:
            ctr_avg: Click-through rate (0..1)
            bounce_rate_avg: Bounce rate (0..1)
            event_count: Raw event count (≥1)
        
        Returns:
            Preprocessed features as numpy array of shape (1, 3)
        """
        # Clip CTR and bounce rate to [0, 1]
        ctr_avg = np.clip(ctr_avg, 0.0, 1.0)
        bounce_rate_avg = np.clip(bounce_rate_avg, 0.0, 1.0)
        
        # Cap event count and apply log1p
        event_count = min(event_count, self.cnt_cap)
        event_count_log = np.log1p(event_count)
        
        # Create feature array
        features = np.array([[ctr_avg, bounce_rate_avg, event_count_log]], dtype=np.float32)
        
        # Standardize using training statistics
        features_standardized = (features - self.mean) / self.scale
        
        return features_standardized
    
    def prepare_batch(self, rows: List[Tuple[float, float, int]]) -> np.ndarray:
        """
        Prepare a batch of rows for inference.
        
        Args:
            rows: List of (ctr_avg, bounce_rate_avg, event_count) tuples
        
        Returns:
            Preprocessed features as numpy array of shape (N, 3)
        """
        if not rows:
            return np.empty((0, 3), dtype=np.float32)
        
        # Extract features
        ctr_avgs = []
        bounce_rate_avgs = []
        event_counts = []
        
        for ctr, br, cnt in rows:
            ctr_avgs.append(np.clip(ctr, 0.0, 1.0))
            bounce_rate_avgs.append(np.clip(br, 0.0, 1.0))
            event_counts.append(min(cnt, self.cnt_cap))
        
        # Convert to numpy arrays
        ctr_avgs = np.array(ctr_avgs, dtype=np.float32)
        bounce_rate_avgs = np.array(bounce_rate_avgs, dtype=np.float32)
        event_counts = np.array(event_counts, dtype=np.float32)
        
        # Apply log1p to event counts
        event_counts_log = np.log1p(event_counts)
        
        # Stack features
        features = np.column_stack([ctr_avgs, bounce_rate_avgs, event_counts_log])
        
        # Standardize using training statistics
        features_standardized = (features - self.mean) / self.scale
        
        return features_standardized.astype(np.float32)


# Global preprocessor instance
preprocessor = Preprocessor()
