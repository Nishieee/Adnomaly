#!/usr/bin/env python3
"""
Build dataset for Adnomaly Phase 4 - Model Training.
Loads offline data from parquet files and prepares numpy arrays for training.
"""

import os
import glob
import json
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import StandardScaler


def load_offline_data():
    """Load offline data from parquet files in the feature store."""
    # Path to the offline store parquet files
    parquet_pattern = "data/offline_store/features/traffic_5m_by_geo_platform/ingest_date=*/part-*.parquet"
    parquet_files = glob.glob(parquet_pattern)
    
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found matching pattern: {parquet_pattern}")
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Load and concatenate all parquet files
    dfs = []
    for file_path in sorted(parquet_files):
        print(f"Loading {file_path}")
        df = pd.read_parquet(file_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Combined dataset shape: {combined_df.shape}")
    
    return combined_df


def prepare_features(df):
    """Prepare features according to specifications."""
    # Select only the required columns in the exact order (using correct column names)
    required_columns = ['window_end', 'geo', 'platform', 'avg_ctr', 'avg_bounce_rate', 'event_count']
    
    # Check if all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Select only the required columns
    df = df[required_columns].copy()
    
    # Drop rows with nulls in any of the 3 features
    feature_columns = ['avg_ctr', 'avg_bounce_rate', 'event_count']
    df = df.dropna(subset=feature_columns)
    print(f"After dropping nulls: {df.shape}")
    
    # Enforce bounds and apply preprocessing
    # avg_ctr: 0 ≤ avg_ctr ≤ 1
    df['avg_ctr'] = np.clip(df['avg_ctr'], 0.0, 1.0)
    
    # avg_bounce_rate: 0 ≤ avg_bounce_rate ≤ 1
    df['avg_bounce_rate'] = np.clip(df['avg_bounce_rate'], 0.0, 1.0)
    
    # event_count: event_count ≥ 1, tame heavy tails, log transform
    df['event_count'] = np.maximum(df['event_count'], 1)
    p999 = np.quantile(df['event_count'], 0.999)
    # Use a more reasonable cap - don't cap too aggressively
    cap_value = max(p999, 1000)  # At least 1000, or the 99.9th percentile
    df['event_count'] = np.minimum(df['event_count'], cap_value)
    df['event_count'] = np.log1p(df['event_count'])
    # Store the actual cap value used
    actual_cap = cap_value
    
    print(f"After preprocessing: {df.shape}")
    print(f"Event count 99.9th percentile: {p999:.1f}")
    print(f"Event count cap used: {actual_cap:.1f}")
    print(f"Log1p event count range: {df['event_count'].min():.4f} - {df['event_count'].max():.4f}")
    
    # Sort by window_end ascending for time-based split
    df = df.sort_values('window_end')
    
    # Extract features in the exact order: avg_ctr, avg_bounce_rate, event_count
    X = df[['avg_ctr', 'avg_bounce_rate', 'event_count']].values.astype(np.float32)
    
    # Extract timestamps as ISO8601 strings
    timestamps = df['window_end'].dt.strftime('%Y-%m-%dT%H:%M:%SZ').values
    
    return X, timestamps, actual_cap


def split_train_val(X, timestamps, train_ratio=0.8):
    """Time-based train/validation split."""
    n_samples = len(X)
    split_idx = int(n_samples * train_ratio)
    
    X_train = X[:split_idx]
    X_val = X[split_idx:]
    timestamps_train = timestamps[:split_idx]
    timestamps_val = timestamps[split_idx:]
    
    print(f"Train set: {X_train.shape[0]} samples")
    print(f"Validation set: {X_val.shape[0]} samples")
    
    return X_train, X_val, timestamps_train, timestamps_val


def standardize_features(X_train, X_val):
    """Standardize features using train set statistics."""
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    
    print(f"Standardization stats:")
    print(f"  Mean: {scaler.mean_}")
    print(f"  Scale: {scaler.scale_}")
    
    return X_train_scaled, X_val_scaled, scaler


def save_artifacts(X_train, X_val, timestamps_train, timestamps_val, scaler, p999):
    """Save numpy arrays and preprocessing parameters to artifacts directory."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Save combined X.npy (train + val) as float32 array with shape (N, 3)
    X_combined = np.vstack([X_train, X_val])
    X_path = artifacts_dir / "X.npy"
    np.save(X_path, X_combined)
    print(f"Saved X.npy: {X_combined.shape}, dtype: {X_combined.dtype}")
    
    # Save combined timestamps
    timestamps_combined = np.concatenate([timestamps_train, timestamps_val])
    timestamps_path = artifacts_dir / "timestamps.npy"
    np.save(timestamps_path, timestamps_combined, allow_pickle=True)
    print(f"Saved timestamps.npy: {timestamps_combined.shape}")
    
    # Save preprocessing parameters
    preprocess_path = artifacts_dir / "preprocess.json"
    preprocess_params = {
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "cnt_cap": float(p999)
    }
    with open(preprocess_path, 'w') as f:
        json.dump(preprocess_params, f, indent=2)
    print(f"Saved preprocessing parameters to {preprocess_path}")
    
    # Save train/val split indices for evaluation
    split_info = {
        "train_size": len(X_train),
        "val_size": len(X_val),
        "total_size": len(X_combined)
    }
    split_path = artifacts_dir / "split_info.json"
    with open(split_path, 'w') as f:
        json.dump(split_info, f, indent=2)
    print(f"Saved split info to {split_path}")
    
    # Print summary statistics
    print("\nFeature Summary (after standardization):")
    print(f"Train set:")
    print(f"  avg_ctr: min={X_train[:, 0].min():.4f}, max={X_train[:, 0].max():.4f}, mean={X_train[:, 0].mean():.4f}")
    print(f"  avg_bounce_rate: min={X_train[:, 1].min():.4f}, max={X_train[:, 1].max():.4f}, mean={X_train[:, 1].mean():.4f}")
    print(f"  event_count (log1p): min={X_train[:, 2].min():.4f}, max={X_train[:, 2].max():.4f}, mean={X_train[:, 2].mean():.4f}")
    print(f"Validation set:")
    print(f"  avg_ctr: min={X_val[:, 0].min():.4f}, max={X_val[:, 0].max():.4f}, mean={X_val[:, 0].mean():.4f}")
    print(f"  avg_bounce_rate: min={X_val[:, 1].min():.4f}, max={X_val[:, 1].max():.4f}, mean={X_val[:, 1].mean():.4f}")
    print(f"  event_count (log1p): min={X_val[:, 2].min():.4f}, max={X_val[:, 2].max():.4f}, mean={X_val[:, 2].mean():.4f}")
    print(f"Original event_count 99.9th percentile cap: {p999:.1f}")


def main():
    """Main function to build the dataset."""
    print("Building dataset for Adnomaly Phase 4 - Model Training")
    print("=" * 60)
    
    try:
        # Load offline data
        df = load_offline_data()
        
        # Prepare features
        X, timestamps, p999 = prepare_features(df)
        
        # Time-based train/validation split
        X_train, X_val, timestamps_train, timestamps_val = split_train_val(X, timestamps)
        
        # Standardize features using train set statistics
        X_train_scaled, X_val_scaled, scaler = standardize_features(X_train, X_val)
        
        # Save artifacts
        save_artifacts(X_train_scaled, X_val_scaled, timestamps_train, timestamps_val, scaler, p999)
        
        print("\n✅ Dataset build completed successfully!")
        print(f"Final dataset shape: {X.shape}")
        
    except Exception as e:
        print(f"❌ Error building dataset: {e}")
        raise


if __name__ == '__main__':
    main()
