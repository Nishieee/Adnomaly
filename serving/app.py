import json
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .schemas import (
    ScoreRequest, ScoreBatchRequest, ScoreResponse, 
    ScoreBatchResponse, HealthResponse
)
from .config import config
from .preprocessing import preprocessor
from .onnx_runner import onnx_runner
from .metrics import metrics_middleware, record_score, record_anomaly, metrics_response

# Configure logging
logging.basicConfig(level=getattr(logging, config.log_level))
logger = logging.getLogger(__name__)

# Load threshold
with open(config.threshold_path, 'r') as f:
    threshold_data = json.load(f)
    THRESHOLD = float(threshold_data['threshold'])

logger.info(f"Loaded threshold: {THRESHOLD}")

# Features used for scoring
FEATURES_USED = ["ctr_avg", "bounce_rate_avg", "event_count"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Adnomaly serving service...")
    logger.info(f"Configuration: {config}")
    logger.info(f"Threshold: {THRESHOLD}")
    yield
    logger.info("Shutting down Adnomaly serving service...")


# Create FastAPI app
app = FastAPI(
    title="Adnomaly Model Serving",
    description="Real-time anomaly detection service for clickstream analytics",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add metrics middleware
app.middleware("http")(metrics_middleware)


@app.get("/healthz", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="ok")


@app.get("/readyz")
async def readiness_check():
    """Readiness check endpoint - verifies model and dependencies are loaded."""
    try:
        # Verify model can run inference
        test_features = preprocessor.prepare_row(0.02, 0.35, 100)
        test_score = onnx_runner.predict_anomaly_scores(test_features)
        
        if len(test_score) == 1:
            return {"status": "ready"}
        else:
            raise HTTPException(status_code=503, detail="Model inference failed")
            
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service not ready: {str(e)}")


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return metrics_response()


@app.post("/v1/score", response_model=ScoreResponse)
async def score_single(request: ScoreRequest):
    """Score a single row."""
    try:
        # Prepare features
        features = preprocessor.prepare_row(
            request.ctr_avg,
            request.bounce_rate_avg,
            request.event_count
        )
        
        # Get anomaly score
        anomaly_score = onnx_runner.predict_anomaly_scores(features)[0]
        
        # Determine if anomaly
        is_anomaly = anomaly_score >= THRESHOLD
        
        # Record metrics
        record_score(anomaly_score)
        if is_anomaly:
            record_anomaly()
        
        # Prepare metadata
        meta = None
        if any([request.geo, request.platform, request.timestamp]):
            meta = {}
            if request.geo:
                meta["geo"] = request.geo
            if request.platform:
                meta["platform"] = request.platform
            if request.timestamp:
                meta["timestamp"] = request.timestamp
        
        return ScoreResponse(
            anomaly_score=float(anomaly_score),
            is_anomaly=is_anomaly,
            threshold=THRESHOLD,
            features_used=FEATURES_USED,
            meta=meta
        )
        
    except Exception as e:
        logger.error(f"Error scoring single row: {e}")
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")


@app.post("/v1/batch", response_model=ScoreBatchResponse)
async def score_batch(request: ScoreBatchRequest):
    """Score a batch of rows."""
    try:
        if len(request.rows) > 1000:
            raise HTTPException(status_code=400, detail="Batch size cannot exceed 1000 rows")
        
        if not request.rows:
            return ScoreBatchResponse(results=[])
        
        # Extract features from all rows
        rows_data = [
            (row.ctr_avg, row.bounce_rate_avg, row.event_count)
            for row in request.rows
        ]
        
        # Prepare features
        features = preprocessor.prepare_batch(rows_data)
        
        # Get anomaly scores
        anomaly_scores = onnx_runner.predict_anomaly_scores(features)
        
        # Process results
        results = []
        for i, row in enumerate(request.rows):
            score = anomaly_scores[i]
            is_anomaly = score >= THRESHOLD
            
            # Record metrics
            record_score(score)
            if is_anomaly:
                record_anomaly()
            
            # Prepare metadata
            meta = None
            if any([row.geo, row.platform, row.timestamp]):
                meta = {}
                if row.geo:
                    meta["geo"] = row.geo
                if row.platform:
                    meta["platform"] = row.platform
                if row.timestamp:
                    meta["timestamp"] = row.timestamp
            
            results.append(ScoreResponse(
                anomaly_score=float(score),
                is_anomaly=is_anomaly,
                threshold=THRESHOLD,
                features_used=FEATURES_USED,
                meta=meta
            ))
        
        return ScoreBatchResponse(results=results)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scoring batch: {e}")
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
