import os
import time
import threading
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from typing import Dict


# Prometheus metrics
SCORED_TOTAL = Counter(
    'scored_total',
    'Total number of features scored',
    ['status']  # 'ok' or 'error'
)

ALERTS_TOTAL = Counter(
    'alerts_total',
    'Total number of alerts produced'
)

SCORE_LATENCY = Histogram(
    'score_latency_seconds',
    'Time taken to score a feature',
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

KAFKA_LAG = Gauge(
    'kafka_lag',
    'Kafka consumer lag per partition',
    ['topic', 'partition']
)

REQUESTS_INFLIGHT = Gauge(
    'requests_inflight',
    'Number of requests currently in flight'
)

# Global state for tracking in-flight requests
_inflight_count = 0
_inflight_lock = threading.Lock()


def start_metrics_server(port: int = 9108):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    print(f"Metrics server started on port {port}")


def record_scored(status: str):
    """Record a scored feature."""
    SCORED_TOTAL.labels(status=status).inc()


def record_alert():
    """Record an alert produced."""
    ALERTS_TOTAL.inc()


def record_score_latency(seconds: float):
    """Record scoring latency."""
    SCORE_LATENCY.observe(seconds)


def update_kafka_lag(topic: str, partition_lags: Dict[int, int]):
    """Update Kafka lag metrics."""
    for partition, lag in partition_lags.items():
        KAFKA_LAG.labels(topic=topic, partition=str(partition)).set(lag)


def increment_inflight():
    """Increment in-flight request counter."""
    global _inflight_count
    with _inflight_lock:
        _inflight_count += 1
        REQUESTS_INFLIGHT.set(_inflight_count)


def decrement_inflight():
    """Decrement in-flight request counter."""
    global _inflight_count
    with _inflight_lock:
        _inflight_count = max(0, _inflight_count - 1)
        REQUESTS_INFLIGHT.set(_inflight_count)


def get_inflight_count() -> int:
    """Get current in-flight request count."""
    with _inflight_lock:
        return _inflight_count
