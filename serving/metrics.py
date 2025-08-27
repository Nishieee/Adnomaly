from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Request, Response
from fastapi.responses import Response as FastAPIResponse
import time
from typing import Callable


# Prometheus metrics
REQUESTS = Counter(
    'serve_requests_total',
    'Total number of requests',
    ['route', 'status']
)

LATENCY = Histogram(
    'serve_latency_seconds',
    'Request latency in seconds',
    ['route']
)

SCORES = Histogram(
    'serve_anomaly_score',
    'Anomaly scores distribution',
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]
)

ANOMALY_FLAGS = Counter(
    'serve_anomalies_total',
    'Total number of anomalies detected'
)


async def metrics_middleware(request: Request, call_next: Callable):
    """FastAPI middleware to collect Prometheus metrics."""
    start_time = time.time()
    
    # Get route name
    route = request.url.path
    
    try:
        # Process request
        response = await call_next(request)
        
        # Record metrics
        duration = time.time() - start_time
        status = str(response.status_code)
        
        REQUESTS.labels(route=route, status=status).inc()
        LATENCY.labels(route=route).observe(duration)
        
        return response
        
    except Exception as e:
        # Record error metrics
        duration = time.time() - start_time
        status = "500"
        
        REQUESTS.labels(route=route, status=status).inc()
        LATENCY.labels(route=route).observe(duration)
        
        raise


def record_score(score: float):
    """Record an anomaly score."""
    SCORES.observe(score)


def record_anomaly():
    """Record an anomaly detection."""
    ANOMALY_FLAGS.inc()


def get_metrics():
    """Get Prometheus metrics as text."""
    return generate_latest()


def metrics_response():
    """Create a FastAPI response with Prometheus metrics."""
    return FastAPIResponse(
        content=get_metrics(),
        media_type=CONTENT_TYPE_LATEST
    )
