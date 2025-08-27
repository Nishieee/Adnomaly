#!/usr/bin/env python3
"""
Evaluate Isolation Forest model for Adnomaly Phase 4 - Model Training.
Injects synthetic anomalies and computes evaluation metrics.
"""

import json
import numpy as np
import pickle
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score, roc_curve


def load_eval_data():
    """Load evaluation data."""
    eval_data_path = Path("model/artifacts/eval_data.npz")
    if not eval_data_path.exists():
        raise FileNotFoundError(f"Eval data not found at {eval_data_path}. Run train.py first.")
    
    data = np.load(eval_data_path, allow_pickle=True)
    X_eval = data['X_eval']
    timestamps_eval = data['timestamps_eval']
    
    print(f"Loaded eval data: {X_eval.shape}")
    return X_eval, timestamps_eval


def load_trained_model():
    """Load the trained model."""
    model_path = Path("model/artifacts/isoforest.pkl")
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found at {model_path}. Run train.py first.")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"Loaded trained model: {type(model).__name__}")
    return model


def inject_anomalies_unscaled(
    X_val_unscaled: np.ndarray,
    *,
    rate: float = 0.02,
    seed: int = 42,
    cnt_cap: float = 1000.0,
    scaler_mean: np.ndarray,
    scaler_scale: np.ndarray,
    stratify_keys: np.ndarray | None = None,
):
    """
    X_val_unscaled columns (order must match training):
        [0] ctr_avg           (0..1)
        [1] bounce_rate_avg   (0..1)
        [2] event_count       (>=1, not log)
    scaler_mean/scale are from preprocess.json (train-split StandardScaler).
    stratify_keys: optional array of strings like "US|ios" per row to spread anomalies across slices.

    Returns:
        X_val_scaled_with_anoms : np.ndarray
        y_true                  : np.ndarray (1=anomaly, 0=normal)
        idx_anom                : np.ndarray (indices of injected anomalies)
    """
    rng = np.random.default_rng(seed)
    n = X_val_unscaled.shape[0]
    k = max(1, int(round(rate * n)))

    # choose indices for anomalies
    if stratify_keys is not None:
        # even spread across slices
        uniq, inv = np.unique(stratify_keys, return_inverse=True)
        per = max(1, k // len(uniq))
        idxs = []
        for s in range(len(uniq)):
            candidates = np.where(inv == s)[0]
            if candidates.size:
                take = min(per, candidates.size)
                idxs.append(rng.choice(candidates, size=take, replace=False))
        idx_anom = np.unique(np.concatenate(idxs))[:k]
        if idx_anom.size < k:
            # top up uniformly if some slices were too small
            pool = np.setdiff1d(np.arange(n), idx_anom, assume_unique=True)
            extra = rng.choice(pool, size=k - idx_anom.size, replace=False)
            idx_anom = np.concatenate([idx_anom, extra])
    else:
        idx_anom = rng.choice(n, size=k, replace=False)

    X_mod = X_val_unscaled.copy()

    # draw per-row multipliers/offsets - more realistic, harder to detect
    # Create heterogeneous anomaly types
    anomaly_types = rng.choice(['ctr_spike', 'bounce_spike', 'volume_spike', 'mixed'], size=idx_anom.size)
    
    ctr_mult = np.ones(idx_anom.size)
    br_add = np.zeros(idx_anom.size)
    cnt_mult = np.ones(idx_anom.size)
    
    # Type 1: CTR spike (subtle)
    ctr_mask = (anomaly_types == 'ctr_spike')
    ctr_mult[ctr_mask] = rng.uniform(1.05, 1.15, size=np.sum(ctr_mask))  # +5-15% instead of +30-60%
    
    # Type 2: Bounce rate spike (subtle)
    br_mask = (anomaly_types == 'bounce_spike')
    br_add[br_mask] = rng.uniform(0.02, 0.05, size=np.sum(br_mask))  # +2-5% instead of +8-15%
    
    # Type 3: Volume spike (subtle)
    vol_mask = (anomaly_types == 'volume_spike')
    cnt_mult[vol_mask] = rng.uniform(1.1, 1.3, size=np.sum(vol_mask))  # +10-30% instead of +50-100%
    
    # Type 4: Mixed anomalies (some combinations)
    mixed_mask = (anomaly_types == 'mixed')
    ctr_mult[mixed_mask] = rng.uniform(0.95, 1.08, size=np.sum(mixed_mask))  # Some decrease CTR
    br_add[mixed_mask] = rng.uniform(0.01, 0.04, size=np.sum(mixed_mask))
    cnt_mult[mixed_mask] = rng.uniform(1.05, 1.25, size=np.sum(mixed_mask))
    
    tiny_jitter = rng.normal(0.0, 0.001, size=idx_anom.size)  # smaller jitter

    # apply in unscaled space
    i = idx_anom
    X_mod[i, 0] = np.clip(X_mod[i, 0] * ctr_mult + tiny_jitter, 0.0, 1.0)        # ctr
    X_mod[i, 1] = np.clip(X_mod[i, 1] + br_add + tiny_jitter, 0.0, 1.0)          # bounce
    X_mod[i, 2] = np.minimum(X_mod[i, 2] * cnt_mult, cnt_cap)                    # count

    # preprocess exactly like training: cap -> log1p -> standardize
    X_prep = X_mod.copy()
    # count pipeline
    X_prep[:, 2] = np.minimum(X_prep[:, 2], cnt_cap)
    X_prep[:, 2] = np.log1p(X_prep[:, 2])
    # ctr/bounce were already in [0,1]; keep as is

    # standardize (train-split stats)
    X_scaled = (X_prep - scaler_mean) / scaler_scale

    # labels
    y_true = np.zeros(n, dtype=int)
    y_true[idx_anom] = 1
    return X_scaled.astype(np.float32), y_true, idx_anom


def inject_anomalies(X_eval):
    """Inject synthetic anomalies using improved method."""
    # Load preprocess parameters
    with open("model/artifacts/preprocess.json", "r") as f:
        pp = json.load(f)
    mean = np.array(pp["mean"], dtype=np.float64)
    scale = np.array(pp["scale"], dtype=np.float64)
    cnt_cap = float(pp["cnt_cap"])
    
    # We need to reconstruct the unscaled validation data
    # Since X_eval is already scaled, we need to reverse the preprocessing
    X_val_unscaled = X_eval.copy()
    
    # Reverse standardization
    X_val_unscaled = X_val_unscaled * scale + mean
    
    # Reverse log1p for event_count
    X_val_unscaled[:, 2] = np.exp(X_val_unscaled[:, 2]) - 1
    
    # Ensure event_count is at least 1
    X_val_unscaled[:, 2] = np.maximum(X_val_unscaled[:, 2], 1)
    
    # Inject anomalies in unscaled space
    X_val_scaled_with_anoms, y_true, idx_anom = inject_anomalies_unscaled(
        X_val_unscaled,
        rate=0.02,
        seed=42,
        cnt_cap=cnt_cap,
        scaler_mean=mean,
        scaler_scale=scale,
        stratify_keys=None,  # No stratification for now
    )
    
    n_samples = len(X_eval)
    n_anomalies = np.sum(y_true)
    print(f"Injected {n_anomalies} anomalies out of {n_samples} samples")
    print(f"Anomaly rate: {n_anomalies/n_samples*100:.1f}%")
    
    return X_val_scaled_with_anoms, y_true


def compute_metrics(model, X_eval_with_anomalies, y_true):
    """Compute evaluation metrics."""
    print("\nComputing metrics...")
    
    # 1) Get scores where higher = more normal
    scores_normal_high = model.score_samples(X_eval_with_anomalies)
    
    # 2) Flip so higher = more anomalous (for ROC and threshold)
    scores = -scores_normal_high
    
    # 3) Choose threshold by quantile to match target alert rate (2%)
    tau = float(np.quantile(scores, 0.98))  # top 2% as anomalies
    
    # 4) Classify with the calibrated threshold
    y_pred = (scores >= tau).astype(int)
    
    # 5) Compute metrics
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_true, scores)
    
    print(f"Threshold (tau): {tau:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")
    print(f"ROC AUC: {roc_auc:.4f}")
    
    return {
        'precision': float(precision),
        'recall': float(recall),
        'f1': float(f1),
        'roc_auc': float(roc_auc),
        'threshold': float(tau)
    }, scores, y_pred, tau


def save_metrics(metrics, tau):
    """Save metrics and threshold to JSON files."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Save metrics
    metrics_path = artifacts_dir / "metrics.json"
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"Saved metrics to {metrics_path}")
    
    # Save threshold
    threshold_path = artifacts_dir / "threshold.json"
    with open(threshold_path, 'w') as f:
        json.dump({"threshold": tau}, f, indent=2)
    print(f"Saved threshold to {threshold_path}")


def plot_roc_curve(y_true, scores):
    """Plot and save ROC curve."""
    artifacts_dir = Path("model/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    # Compute ROC curve
    fpr, tpr, _ = roc_curve(y_true, scores)
    
    # Create plot
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, linewidth=2)
    plt.plot([0, 1], [0, 1], 'k--', linewidth=1)
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curve - Isolation Forest Anomaly Detection')
    plt.grid(True, alpha=0.3)
    
    # Save plot
    roc_path = artifacts_dir / "roc.png"
    plt.savefig(roc_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Saved ROC curve to {roc_path}")


def main():
    """Main function to evaluate the model."""
    print("Evaluating Isolation Forest for Adnomaly Phase 4 - Model Training")
    print("=" * 70)
    
    try:
        # Set random seed for reproducible anomaly injection
        np.random.seed(42)
        
        # Load eval data and model
        X_eval, timestamps_eval = load_eval_data()
        model = load_trained_model()
        
        # Inject synthetic anomalies
        X_eval_with_anomalies, y_true = inject_anomalies(X_eval)
        
        # Compute metrics
        metrics, scores, y_pred, tau = compute_metrics(model, X_eval_with_anomalies, y_true)
        
        # Test random baseline to verify evaluation isn't broken
        print("\nTesting random baseline...")
        y_random = np.random.choice([0, 1], size=len(y_true), p=[0.98, 0.02])
        roc_auc_random = roc_auc_score(y_random, scores)
        print(f"Random labels ROC AUC: {roc_auc_random:.4f}")
        if roc_auc_random > 0.6:
            print("⚠️  Warning: Random labels AUC too high - evaluation may be broken")
        else:
            print("✅ Random baseline test passed")
        
        # Save metrics and threshold
        save_metrics(metrics, tau)
        
        # Plot and save ROC curve
        plot_roc_curve(y_true, scores)
        
        print("\n✅ Model evaluation completed successfully!")
        print(f"Final metrics: {metrics}")
        
    except Exception as e:
        print(f"❌ Error evaluating model: {e}")
        raise


if __name__ == '__main__':
    main()
