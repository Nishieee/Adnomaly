#!/usr/bin/env bash
set -euo pipefail

# Configuration
SERVICE=adnomaly-serving
REGION=us-central1
PROJECT=${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project)}
IMAGE=gcr.io/$PROJECT/$SERVICE:$(date +%Y%m%d-%H%M%S)

echo "Deploying Adnomaly serving service to Cloud Run..."
echo "Project: $PROJECT"
echo "Region: $REGION"
echo "Service: $SERVICE"
echo "Image: $IMAGE"

# Build Docker image
echo "Building Docker image..."
docker build -t $IMAGE -f serving/Dockerfile .

# Configure Docker for GCR
echo "Configuring Docker for Google Container Registry..."
gcloud auth configure-docker

# Push image to GCR
echo "Pushing image to Google Container Registry..."
docker push $IMAGE

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE \
  --image $IMAGE \
  --region $REGION \
  --platform managed \
  --memory 1Gi \
  --allow-unauthenticated \
  --cpu 1 \
  --port 8080 \
  --max-instances 1 \
  --min-instances 0 \
  --set-env-vars=MODEL_PATH=model/artifacts/isoforest.onnx,PREPROCESS_PATH=model/artifacts/preprocess.json,THRESHOLD_PATH=model/artifacts/threshold.json

echo "✅ Deployment complete!"
echo "Service URL: $(gcloud run services describe $SERVICE --region $REGION --format 'value(status.url)')"
echo ""
echo "Test the service:"
echo "curl -X POST \$(gcloud run services describe $SERVICE --region $REGION --format 'value(status.url)')/v1/score \\"
echo "  -H 'content-type: application/json' \\"
echo "  -d '{\"ctr_avg\":0.03,\"bounce_rate_avg\":0.62,\"event_count\":350,\"geo\":\"US\",\"platform\":\"ios\"}'"
