import os
from pathlib import Path
from typing import Optional


class Config:
    """Configuration for the serving service."""
    
    def __init__(self):
        # Model paths
        self.model_path = Path(os.getenv('MODEL_PATH', 'model/artifacts/isoforest.onnx'))
        self.preprocess_path = Path(os.getenv('PREPROCESS_PATH', 'model/artifacts/preprocess.json'))
        self.threshold_path = Path(os.getenv('THRESHOLD_PATH', 'model/artifacts/threshold.json'))
        
        # Logging
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        
        # Validate paths exist
        self._validate_paths()
    
    def _validate_paths(self):
        """Validate that all required files exist."""
        missing_files = []
        
        if not self.model_path.exists():
            missing_files.append(str(self.model_path))
        
        if not self.preprocess_path.exists():
            missing_files.append(str(self.preprocess_path))
        
        if not self.threshold_path.exists():
            missing_files.append(str(self.threshold_path))
        
        if missing_files:
            raise FileNotFoundError(
                f"Required model files not found: {', '.join(missing_files)}. "
                "Please ensure the model artifacts are available."
            )
    
    def __repr__(self):
        return f"Config(model_path={self.model_path}, preprocess_path={self.preprocess_path}, threshold_path={self.threshold_path})"


# Global config instance
config = Config()
