#!/usr/bin/env python3
"""
Train Isolation Forest model for Adnomaly Phase 4 - Model Training.
Trains on the prepared dataset with time-based split and saves the model.
"""

import numpy as np
import pickle
from pathlib import Path
from sklearn.ensemble import IsolationForest


def load_dataset():
    """Load the prepared dataset."""
    artifacts_dir = Path("model/artifacts")
    
    # Load X.npy
    X_path = artifacts_dir / "X.npy"
    if not X_path.exists():
        raise FileNotFoundError(f"X.npy not found at {X_path}. Run build_dataset.py first.")
    
    X = np.load(X_path)
    print(f"Loaded X.npy: {X.shape}, dtype: {X.dtype}")
    
    # Load timestamps.npy
    timestamps_path = artifacts_dir / "timestamps.npy"
    if not timestamps_path.exists():
        raise FileNotFoundError(f"timestamps.npy not found at {timestamps_path}. Run build_dataset.py first.")
    
    timestamps = np.load(timestamps_path, allow_pickle=True)
    print(f"Loaded timestamps.npy: {timestamps.shape}")
    
    return X, timestamps


def time_based_split(X, timestamps):
    """Perform time-based split: first 80% train, last 20% eval base."""
    n_samples = len(X)
    split_idx = int(0.8 * n_samples)
    
    # Split data
    X_train = X[:split_idx]
    X_eval = X[split_idx:]
    
    # Split timestamps
    timestamps_train = timestamps[:split_idx]
    timestamps_eval = timestamps[split_idx:]
    
    print(f"Time-based split:")
    print(f"  Train: {X_train.shape[0]} samples ({X_train.shape[0]/n_samples*100:.1f}%)")
    print(f"  Eval:  {X_eval.shape[0]} samples ({X_eval.shape[0]/n_samples*100:.1f}%)")
    print(f"  Train time range: {timestamps_train[0]} to {timestamps_train[-1]}")
    print(f"  Eval time range:  {timestamps_eval[0]} to {timestamps_eval[-1]}")
    
    return X_train, X_eval, timestamps_train, timestamps_eval


def train_isolation_forest(X_train):
    """Train Isolation Forest with exact hyperparameters."""
    print("\nTraining Isolation Forest...")
    print("Hyperparameters:")
    print("  n_estimators: 200")
    print("  max_samples: 'auto'")
    print("  contamination: 0.02")
    print("  random_state: 42")
    print("  n_jobs: -1")
    
    # Create and train the model
    model = IsolationForest(
        n_estimators=200,
        max_samples='auto',
        contamination=0.02,
        random_state=42,
        n_jobs=-1
    )
    
    # Fit the model
    model.fit(X_train)
    
    print("✅ Model training completed!")
    
    return model


def save_model(model, X_eval, timestamps_eval):
    """Save the trained model and eval data."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Save sklearn model
    model_path = artifacts_dir / "isoforest.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    print(f"Saved model to {model_path}")
    
    # Save eval data for later use
    eval_data_path = artifacts_dir / "eval_data.npz"
    np.savez(eval_data_path, X_eval=X_eval, timestamps_eval=timestamps_eval, allow_pickle=True)
    print(f"Saved eval data to {eval_data_path}")
    
    # Test the model on a single sample
    test_sample = X_eval[:1] if len(X_eval) > 0 else np.array([[0.1, 0.5, 10]], dtype=np.float32)
    score = model.decision_function(test_sample)
    print(f"Model test on sample: score = {score[0]:.4f}")


def main():
    """Main function to train the model."""
    print("Training Isolation Forest for Adnomaly Phase 4 - Model Training")
    print("=" * 70)
    
    try:
        # Load dataset
        X, timestamps = load_dataset()
        
        # Perform time-based split
        X_train, X_eval, timestamps_train, timestamps_eval = time_based_split(X, timestamps)
        
        # Train model
        model = train_isolation_forest(X_train)
        
        # Save model and eval data
        save_model(model, X_eval, timestamps_eval)
        
        print("\n✅ Model training completed successfully!")
        
    except Exception as e:
        print(f"❌ Error training model: {e}")
        raise


if __name__ == '__main__':
    main()
