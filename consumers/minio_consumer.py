#!/usr/bin/env python3
"""
MinIO consumer for Adnomaly project.
Reads events from Kafka and stores them in MinIO object storage.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.schema import validate_event

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'clickstream')
GROUP_ID = os.getenv('GROUP_ID', 'minio-consumer')
AUTO_OFFSET_RESET = os.getenv('AUTO_OFFSET_RESET', 'latest')

# MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'adnomaly-data')
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'

# Batch configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '60'))  # seconds


def create_minio_client():
    """Create and return a MinIO client."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=f"http{'s' if MINIO_USE_SSL else ''}://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'  # MinIO default region
        )
        
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                s3_client.create_bucket(Bucket=MINIO_BUCKET)
                print(f"Created bucket: {MINIO_BUCKET}")
            else:
                raise
        
        return s3_client
    except Exception as e:
        print(f"MinIO client error: {e}", file=sys.stderr)
        return None


def create_kafka_consumer() -> Consumer:
    """Create and return a Kafka consumer."""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET,
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 2000
    }
    consumer = Consumer(config)
    consumer.subscribe([TOPIC])
    return consumer


def upload_batch_to_minio(s3_client, events: List[Dict[str, Any]], batch_id: str) -> bool:
    """Upload a batch of events to MinIO."""
    try:
        # Create JSON content
        content = json.dumps(events, indent=2)
        
        # Use the timestamp from the first event in the batch for the path
        # This ensures diverse timestamps create diverse file paths
        if events:
            # Parse the timestamp from the first event
            first_event_timestamp = events[0]['timestamp']
            # Convert ISO8601 timestamp to datetime
            event_datetime = datetime.fromisoformat(first_event_timestamp.replace('Z', '+00:00'))
            # Create path based on event timestamp
            timestamp_path = event_datetime.strftime('%Y/%m/%d/%H')
        else:
            # Fallback to current time if no events
            timestamp_path = datetime.now().strftime('%Y/%m/%d/%H')
        
        object_key = f"clickstream/{timestamp_path}/batch_{batch_id}.json"
        
        # Upload to MinIO
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=object_key,
            Body=content.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Uploaded batch {batch_id} to s3://{MINIO_BUCKET}/{object_key}")
        return True
        
    except Exception as e:
        print(f"Error uploading batch {batch_id}: {e}", file=sys.stderr)
        return False


def validate_and_add_to_batch(event_data: Dict[str, Any], batch: List[Dict[str, Any]]) -> bool:
    """Validate an event and add it to the batch if valid."""
    try:
        # Validate the event
        validate_event(event_data)
        
        # Add to batch
        batch.append(event_data)
        return True
        
    except Exception as e:
        print(f"Warning: Invalid event skipped - {e}", file=sys.stderr)
        return False


def main():
    """Main function to run the MinIO consumer."""
    print(f"Starting MinIO consumer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Group ID: {GROUP_ID}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print(f"Batch size: {BATCH_SIZE}")
    print("Press Ctrl+C to stop")
    
    # Create MinIO client
    s3_client = create_minio_client()
    if not s3_client:
        print("Failed to connect to MinIO. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\nShutting down...")
        consumer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    events_processed = 0
    events_stored = 0
    batches_uploaded = 0
    start_time = time.time()
    batch_start_time = time.time()
    current_batch = []
    batch_id = 1
    
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                # Check if we need to flush the batch due to timeout
                if current_batch and (time.time() - batch_start_time) > BATCH_TIMEOUT:
                    if upload_batch_to_minio(s3_client, current_batch, batch_id):
                        events_stored += len(current_batch)
                        batches_uploaded += 1
                    current_batch = []
                    batch_id += 1
                    batch_start_time = time.time()
                continue
                
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue
                
            if msg.value():
                events_processed += 1
                event_data = json.loads(msg.value().decode('utf-8'))
                
                if validate_and_add_to_batch(event_data, current_batch):
                    # Check if batch is full
                    if len(current_batch) >= BATCH_SIZE:
                        if upload_batch_to_minio(s3_client, current_batch, batch_id):
                            events_stored += len(current_batch)
                            batches_uploaded += 1
                        current_batch = []
                        batch_id += 1
                        batch_start_time = time.time()
                
                # Print statistics every 100 events
                if events_processed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = events_processed / elapsed if elapsed > 0 else 0
                    print(f"Processed: {events_processed}, Stored: {events_stored}, "
                          f"Batches: {batches_uploaded}, Rate: {rate:.1f} events/sec")
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in consumer: {e}", file=sys.stderr)
    finally:
        # Upload any remaining events in the batch
        if current_batch:
            if upload_batch_to_minio(s3_client, current_batch, batch_id):
                events_stored += len(current_batch)
                batches_uploaded += 1
        
        print(f"Final stats - Processed: {events_processed}, Stored: {events_stored}, "
              f"Batches: {batches_uploaded}")
        consumer.close()


if __name__ == '__main__':
    main()
