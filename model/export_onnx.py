#!/usr/bin/env python3
"""
Export trained Isolation Forest model to ONNX format for Adnomaly Phase 4.
Converts sklearn model to ONNX using skl2onnx.
"""

import numpy as np
import pickle
from pathlib import Path
import skl2onnx
from skl2onnx.common.data_types import FloatTensorType


def load_trained_model():
    """Load the trained sklearn model."""
    model_path = Path("model/artifacts/isoforest.pkl")
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found at {model_path}. Run train.py first.")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"Loaded trained model: {type(model).__name__}")
    return model


def convert_to_onnx(model):
    """Convert sklearn model to ONNX format."""
    print("\nConverting to ONNX...")
    print("Input specification:")
    print("  Name: 'input'")
    print("  Shape: [None, 3]")
    print("  Type: float32")
    
    # Define input type: [None, 3] float32
    initial_type = [('input', FloatTensorType([None, 3]))]
    
    # Convert to ONNX
    onnx_model = skl2onnx.convert_sklearn(
        model,
        initial_types=initial_type,
        target_opset={'': 13, 'ai.onnx.ml': 3}  # Specify both opset versions
    )
    
    print("✅ ONNX conversion completed!")
    return onnx_model


def save_onnx_model(onnx_model):
    """Save the ONNX model."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    onnx_path = artifacts_dir / "isoforest.onnx"
    
    with open(onnx_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    
    print(f"Saved ONNX model to {onnx_path}")
    
    # Print model info
    print(f"ONNX model size: {onnx_path.stat().st_size / 1024:.1f} KB")


def test_onnx_model(onnx_model):
    """Test the ONNX model with a sample input."""
    import onnxruntime as ort
    
    # Create a test session
    sess = ort.InferenceSession(onnx_model.SerializeToString())
    
    # Test input: single sample with shape (1, 3)
    test_input = np.array([[0.1, 0.5, 10]], dtype=np.float32)
    
    # Run inference
    input_name = sess.get_inputs()[0].name
    output_name = sess.get_outputs()[0].name
    
    result = sess.run([output_name], {input_name: test_input})
    score = float(result[0][0])
    
    print(f"ONNX model test on sample: score = {score:.4f}")
    print("✅ ONNX model inference test passed!")


def main():
    """Main function to export model to ONNX."""
    print("Exporting Isolation Forest to ONNX for Adnomaly Phase 4")
    print("=" * 60)
    
    try:
        # Load trained model
        model = load_trained_model()
        
        # Convert to ONNX
        onnx_model = convert_to_onnx(model)
        
        # Save ONNX model
        save_onnx_model(onnx_model)
        
        # Test ONNX model
        test_onnx_model(onnx_model)
        
        print("\n✅ ONNX export completed successfully!")
        
    except Exception as e:
        print(f"❌ Error exporting to ONNX: {e}")
        raise


if __name__ == '__main__':
    main()
