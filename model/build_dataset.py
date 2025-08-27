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
    df['event_count'] = np.minimum(df['event_count'], p999)
    df['event_count'] = np.log1p(df['event_count'])
    
    print(f"After preprocessing: {df.shape}")
    print(f"Event count 99.9th percentile cap: {p999:.1f}")
    
    # Sort by window_end ascending
    df = df.sort_values('window_end')
    
    # Extract features in the exact order: avg_ctr, avg_bounce_rate, event_count
    X = df[['avg_ctr', 'avg_bounce_rate', 'event_count']].values.astype(np.float32)
    
    # Extract timestamps as ISO8601 strings
    timestamps = df['window_end'].dt.strftime('%Y-%m-%dT%H:%M:%SZ').values
    
    return X, timestamps, p999


def save_artifacts(X, timestamps, p999):
    """Save numpy arrays and preprocessing parameters to artifacts directory."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Save X.npy as float32 array with shape (N, 3)
    X_path = artifacts_dir / "X.npy"
    np.save(X_path, X)
    print(f"Saved X.npy: {X.shape}, dtype: {X.dtype}")
    
    # Save timestamps.npy
    timestamps_path = artifacts_dir / "timestamps.npy"
    np.save(timestamps_path, timestamps, allow_pickle=True)
    print(f"Saved timestamps.npy: {timestamps.shape}")
    
    # Save preprocessing parameters
    preprocess_path = artifacts_dir / "preprocess.json"
    with open(preprocess_path, 'w') as f:
        json.dump({"cnt_cap": float(p999)}, f, indent=2)
    print(f"Saved preprocessing parameters to {preprocess_path}")
    
    # Print summary statistics
    print("\nFeature Summary (after preprocessing):")
    print(f"avg_ctr: min={X[:, 0].min():.4f}, max={X[:, 0].max():.4f}, mean={X[:, 0].mean():.4f}")
    print(f"avg_bounce_rate: min={X[:, 1].min():.4f}, max={X[:, 1].max():.4f}, mean={X[:, 1].mean():.4f}")
    print(f"event_count (log1p): min={X[:, 2].min():.4f}, max={X[:, 2].max():.4f}, mean={X[:, 2].mean():.4f}")
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
        
        # Save artifacts
        save_artifacts(X, timestamps, p999)
        
        print("\n✅ Dataset build completed successfully!")
        print(f"Final dataset shape: {X.shape}")
        
    except Exception as e:
        print(f"❌ Error building dataset: {e}")
        raise


if __name__ == '__main__':
    main()
