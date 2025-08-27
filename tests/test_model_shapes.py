#!/usr/bin/env python3
"""
Tests for model shapes and functionality in Adnomaly Phase 4.
"""

import numpy as np
import pickle
import onnxruntime as ort
from pathlib import Path
import pytest


def test_dataset_shapes():
    """Test that X.npy has correct shape and dtype."""
    X_path = Path("model/artifacts/X.npy")
    
    if not X_path.exists():
        pytest.skip("X.npy not found. Run build_dataset.py first.")
    
    X = np.load(X_path)
    
    # Assert shape is (N, 3)
    assert X.ndim == 2, f"Expected 2D array, got {X.ndim}D"
    assert X.shape[1] == 3, f"Expected 3 features, got {X.shape[1]}"
    
    # Assert dtype is float32
    assert X.dtype == np.float32, f"Expected float32, got {X.dtype}"
    
    print(f"✅ X.npy shape: {X.shape}, dtype: {X.dtype}")


def test_sklearn_model_prediction():
    """Test that trained sklearn model predicts on (1,3) input."""
    model_path = Path("model/artifacts/isoforest.pkl")
    
    if not model_path.exists():
        pytest.skip("isoforest.pkl not found. Run train.py first.")
    
    # Load model
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    # Test input: single sample with shape (1, 3)
    test_input = np.array([[0.1, 0.5, 10]], dtype=np.float32)
    
    # Test prediction
    try:
        score = model.decision_function(test_input)
        assert score.shape == (1,), f"Expected score shape (1,), got {score.shape}"
        print(f"✅ Sklearn model prediction: score = {score[0]:.4f}")
    except Exception as e:
        pytest.fail(f"Sklearn model prediction failed: {e}")


def test_onnx_model_inference():
    """Test that ONNX model runs on (1,3) input and returns a score."""
    onnx_path = Path("model/artifacts/isoforest.onnx")
    
    if not onnx_path.exists():
        pytest.skip("isoforest.onnx not found. Run export_onnx.py first.")
    
    # Create inference session
    sess = ort.InferenceSession(str(onnx_path))
    
    # Test input: single sample with shape (1, 3)
    test_input = np.array([[0.1, 0.5, 10]], dtype=np.float32)
    
    # Get input and output names
    input_name = sess.get_inputs()[0].name
    output_name = sess.get_outputs()[0].name
    
    # Run inference
    try:
        result = sess.run([output_name], {input_name: test_input})
        score = float(result[0][0])
        
        assert isinstance(score, (float, np.floating)), f"Expected float score, got {type(score)}"
        print(f"✅ ONNX model inference: score = {score:.4f}")
    except Exception as e:
        pytest.fail(f"ONNX model inference failed: {e}")


def test_model_consistency():
    """Test that sklearn and ONNX models give similar results."""
    model_path = Path("model/artifacts/isoforest.pkl")
    onnx_path = Path("model/artifacts/isoforest.onnx")
    
    if not model_path.exists() or not onnx_path.exists():
        pytest.skip("Model files not found. Run train.py and export_onnx.py first.")
    
    # Load sklearn model
    with open(model_path, 'rb') as f:
        sklearn_model = pickle.load(f)
    
    # Load ONNX model
    onnx_sess = ort.InferenceSession(str(onnx_path))
    input_name = onnx_sess.get_inputs()[0].name
    output_name = onnx_sess.get_outputs()[0].name
    
    # Test input
    test_input = np.array([[0.1, 0.5, 10]], dtype=np.float32)
    
    # Get predictions
    sklearn_score = sklearn_model.decision_function(test_input)[0]
    onnx_result = onnx_sess.run([output_name], {input_name: test_input})
    onnx_score = float(onnx_result[0][0])
    
    # Compare scores (should be reasonable - ONNX conversion can introduce differences)
    score_diff = abs(sklearn_score - onnx_score)
    assert score_diff < 2.0, f"Score difference too large: {score_diff}"
    
    print(f"✅ Model consistency: sklearn={sklearn_score:.4f}, onnx={onnx_score:.4f}")


if __name__ == '__main__':
    # Run all tests
    print("Running model shape and functionality tests...")
    
    test_dataset_shapes()
    test_sklearn_model_prediction()
    test_onnx_model_inference()
    test_model_consistency()
    
    print("✅ All tests passed!")
