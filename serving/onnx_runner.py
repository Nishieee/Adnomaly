import numpy as np
import onnxruntime as ort
from typing import Union, List
from .config import config


class ONNXRunner:
    """ONNX model runner for inference."""
    
    def __init__(self):
        # Create inference session
        self.session = ort.InferenceSession(
            str(config.model_path),
            providers=['CPUExecutionProvider']
        )
        
        # Get input details
        self.input_name = self.session.get_inputs()[0].name
        self.input_shape = self.session.get_inputs()[0].shape
        
        print(f"Loaded ONNX model: input_name={self.input_name}, input_shape={self.input_shape}")
    
    def predict(self, features: np.ndarray) -> np.ndarray:
        """
        Run inference on preprocessed features.
        
        Args:
            features: Preprocessed features of shape (N, 3)
        
        Returns:
            Model scores where higher = more normal
        """
        # Ensure correct shape and dtype
        if features.ndim == 1:
            features = features.reshape(1, -1)
        
        features = features.astype(np.float32)
        
        # Run inference
        outputs = self.session.run(None, {self.input_name: features})
        
        # Extract scores (first output)
        scores = outputs[0]
        
        return scores.flatten()
    
    def predict_anomaly_scores(self, features: np.ndarray) -> np.ndarray:
        """
        Run inference and return anomaly scores (higher = more anomalous).
        
        Args:
            features: Preprocessed features of shape (N, 3)
        
        Returns:
            Anomaly scores where higher = more anomalous
        """
        # Get normal scores (higher = more normal)
        normal_scores = self.predict(features)
        
        # Flip to get anomaly scores (higher = more anomalous)
        anomaly_scores = -normal_scores
        
        return anomaly_scores


# Global ONNX runner instance
onnx_runner = ONNXRunner()
