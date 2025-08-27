#!/usr/bin/env python3
"""
Real-time scoring service for Adnomaly Phase 6.
Reads features from Kafka, calls the serving API, and writes alerts.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Optional
import httpx

from .schemas import FeatureMsg, ServingResponse, AlertMsg
from .kafka_utils import build_consumer, build_producer, safe_poll, delivery_report, get_partition_lag, commit_sync
from .metrics import (
    start_metrics_server, record_scored, record_alert, record_score_latency,
    update_kafka_lag, increment_inflight, decrement_inflight
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment configuration
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
FEATURES_TOPIC = os.getenv('FEATURES_TOPIC', 'features')
ALERTS_TOPIC = os.getenv('ALERTS_TOPIC', 'alerts')
SERVING_URL = os.getenv('SERVING_URL', 'http://localhost:8080/v1/score')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'scorer-v1')
MAX_INFLIGHT = int(os.getenv('MAX_INFLIGHT', '32'))
HTTP_TIMEOUT_SECONDS = float(os.getenv('HTTP_TIMEOUT_SECONDS', '2.5'))
RETRY_MAX = int(os.getenv('RETRY_MAX', '3'))
PROM_PORT = int(os.getenv('PROM_PORT', '9108'))

# Global state
shutdown_event = asyncio.Event()
http_client: Optional[httpx.AsyncClient] = None


async def call_serving_api(feature: FeatureMsg) -> Optional[ServingResponse]:
    """Call the serving API with retry logic."""
    global http_client
    
    if http_client is None:
        http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS)
    
    request_data = feature.to_serving_request()
    logger.info(f"Calling serving API with data: {request_data}")
    
    for attempt in range(RETRY_MAX + 1):
        try:
            start_time = time.time()
            increment_inflight()
            
            response = await http_client.post(SERVING_URL, json=request_data)
            
            decrement_inflight()
            latency = time.time() - start_time
            record_score_latency(latency)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Serving API response: {data}")
                return ServingResponse(**data)
            elif response.status_code >= 500:
                # Server error, retry
                logger.warning(f"Serving API error {response.status_code}, attempt {attempt + 1}/{RETRY_MAX + 1}")
                if attempt < RETRY_MAX:
                    await asyncio.sleep(2 ** attempt + 0.1)  # Exponential backoff with jitter
                    continue
                else:
                    logger.error(f"Serving API failed after {RETRY_MAX + 1} attempts")
                    return None
            else:
                # Client error, don't retry
                logger.error(f"Serving API client error {response.status_code}: {response.text}")
                return None
                
        except httpx.TimeoutException:
            decrement_inflight()
            logger.warning(f"Serving API timeout, attempt {attempt + 1}/{RETRY_MAX + 1}")
            if attempt < RETRY_MAX:
                await asyncio.sleep(2 ** attempt + 0.1)
                continue
            else:
                logger.error(f"Serving API timeout after {RETRY_MAX + 1} attempts")
                return None
                
        except Exception as e:
            decrement_inflight()
            logger.error(f"Error calling serving API: {e}")
            return None
    
    return None


async def process_feature(feature: FeatureMsg, producer, consumer, message) -> bool:
    """Process a single feature message."""
    try:
        logger.info(f"Processing feature: {feature.geo}/{feature.platform} with CTR={feature.ctr_avg}, bounce={feature.bounce_rate_avg}, count={feature.event_count}")
        
        # Call serving API
        response = await call_serving_api(feature)
        
        if response is None:
            logger.error("Serving API call failed")
            record_scored('error')
            return False
        
        logger.info(f"Got response: anomaly_score={response.anomaly_score}, is_anomaly={response.is_anomaly}")
        record_scored('ok')
        
        # Check if anomaly detected
        if response.is_anomaly:
            # Create alert message
            alert = AlertMsg.from_feature_and_response(feature, response)
            
            # Produce alert to Kafka
            producer.produce(
                ALERTS_TOPIC,
                alert.to_kafka(),
                callback=delivery_report
            )
            
            record_alert()
            logger.info(f"Alert produced for {feature.geo}/{feature.platform} with score {response.anomaly_score}")
        
        # Commit offset only after successful processing
        if not commit_sync(consumer, message):
            logger.error("Failed to commit offset")
            return False
        
        logger.info("Successfully processed feature and committed offset")
        return True
        
    except Exception as e:
        logger.error(f"Error processing feature: {e}")
        record_scored('error')
        return False


async def lag_monitor(consumer, topic: str):
    """Monitor Kafka lag every 10 seconds."""
    while not shutdown_event.is_set():
        try:
            partition_lags = get_partition_lag(consumer, topic)
            update_kafka_lag(topic, partition_lags)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error monitoring lag: {e}")
            await asyncio.sleep(10)


async def main():
    """Main function."""
    global http_client
    
    logger.info("Starting Adnomaly real-time scorer...")
    logger.info(f"Configuration:")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP}")
    logger.info(f"  Features topic: {FEATURES_TOPIC}")
    logger.info(f"  Alerts topic: {ALERTS_TOPIC}")
    logger.info(f"  Serving URL: {SERVING_URL}")
    logger.info(f"  Consumer group: {CONSUMER_GROUP}")
    logger.info(f"  Max inflight: {MAX_INFLIGHT}")
    logger.info(f"  HTTP timeout: {HTTP_TIMEOUT_SECONDS}s")
    logger.info(f"  Retry max: {RETRY_MAX}")
    logger.info(f"  Prometheus port: {PROM_PORT}")
    
    # Start metrics server
    start_metrics_server(PROM_PORT)
    
    # Create Kafka consumer and producer
    consumer = build_consumer(CONSUMER_GROUP, KAFKA_BOOTSTRAP)
    producer = build_producer(KAFKA_BOOTSTRAP)
    
    # Subscribe to features topic
    consumer.subscribe([FEATURES_TOPIC])
    logger.info(f"Subscribed to topic: {FEATURES_TOPIC}")
    
    # Start lag monitor
    lag_task = asyncio.create_task(lag_monitor(consumer, FEATURES_TOPIC))
    
    # Process messages
    processed_count = 0
    start_time = time.time()
    
    try:
        while not shutdown_event.is_set():
            # Poll for messages
            message, error = safe_poll(consumer, timeout=1.0)
            
            if error:
                logger.error(f"Kafka error: {error}")
                continue
            
            if message is None:
                continue
            
            logger.info(f"Received message from partition {message.partition()} at offset {message.offset()}")
            
            try:
                # Parse feature message
                feature = FeatureMsg.from_kafka(message.value())
                processed_count += 1
                logger.info(f"Parsed feature message #{processed_count}")
                
                # Process feature synchronously for now
                success = await process_feature(feature, producer, consumer, message)
                
                if success:
                    logger.info(f"Successfully processed message #{processed_count}")
                else:
                    logger.error(f"Failed to process message #{processed_count}")
                
                # Log progress
                if processed_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Processed {processed_count} features ({rate:.1f}/sec)")
                
            except ValueError as e:
                logger.error(f"Invalid message format: {e}")
                record_scored('error')
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                record_scored('error')
                continue
    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Cleanup
        logger.info("Shutting down...")
        
        # Cancel lag monitor
        lag_task.cancel()
        
        # Close HTTP client
        if http_client:
            await http_client.aclose()
        
        # Flush producer
        producer.flush()
        
        # Close Kafka connections
        consumer.close()
        
        logger.info(f"Final stats: {processed_count} features processed")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    shutdown_event.set()


if __name__ == '__main__':
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the main function
    asyncio.run(main())
