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


def inject_anomalies(X_eval):
    """Inject synthetic anomalies according to specifications."""
    n_samples = len(X_eval)
    
    # Pick 2% of eval rows uniformly at random (at least 200 if available)
    n_anomalies = max(int(0.02 * n_samples), min(200, n_samples))
    
    # Randomly select indices for anomalies
    anomaly_indices = np.random.choice(n_samples, size=n_anomalies, replace=False)
    
    # Create a copy of the eval data
    X_eval_with_anomalies = X_eval.copy()
    
    # Create labels: 0 for normal, 1 for anomaly
    y_true = np.zeros(n_samples, dtype=int)
    y_true[anomaly_indices] = 1
    
    # Inject anomalies according to exact rules
    for idx in anomaly_indices:
        # ctr_avg = min(ctr_avg * 4.0, 1.0)
        X_eval_with_anomalies[idx, 0] = min(X_eval[idx, 0] * 4.0, 1.0)
        
        # bounce_rate_avg = min(bounce_rate_avg * 1.5, 1.0)
        X_eval_with_anomalies[idx, 1] = min(X_eval[idx, 1] * 1.5, 1.0)
        
        # event_count = event_count * 3
        X_eval_with_anomalies[idx, 2] = X_eval[idx, 2] * 3
    
    print(f"Injected {n_anomalies} anomalies out of {n_samples} samples")
    print(f"Anomaly rate: {n_anomalies/n_samples*100:.1f}%")
    
    return X_eval_with_anomalies, y_true


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
