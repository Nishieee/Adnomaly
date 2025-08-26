#!/usr/bin/env python3
"""
Database consumer for Adnomaly project.
Reads events from Kafka and stores them in PostgreSQL database.
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Consumer

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.schema import validate_event

# Environment variables with defaults
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
TOPIC = os.getenv('TOPIC', 'clickstream')
GROUP_ID = os.getenv('GROUP_ID', 'db-consumer')
AUTO_OFFSET_RESET = os.getenv('AUTO_OFFSET_RESET', 'latest')

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'adnomaly')
DB_USER = os.getenv('DB_USER', 'adnomaly_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'adnomaly_password')


def create_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}", file=sys.stderr)
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


def store_event_in_db(conn, event_data: Dict[str, Any]) -> bool:
    """Store a clickstream event in the database."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO clickstream_events 
                (timestamp, user_id_hash, ad_id, campaign_id, geo, platform, 
                 user_agent, cpc, ctr, conversion, bounce_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                event_data['timestamp'],
                event_data['user_id_hash'],
                event_data['ad_id'],
                event_data['campaign_id'],
                event_data['geo'],
                event_data['platform'],
                event_data['user_agent'],
                event_data['CPC'],
                event_data['CTR'],
                event_data['conversion'],
                event_data['bounce_rate']
            ))
            conn.commit()
            return True
    except psycopg2.Error as e:
        print(f"Database error storing event: {e}", file=sys.stderr)
        conn.rollback()
        return False


def validate_and_store_event(conn, event_data: Dict[str, Any]) -> bool:
    """Validate an event and store it in the database if valid."""
    try:
        # Validate the event
        validate_event(event_data)
        
        # Store in database
        return store_event_in_db(conn, event_data)
        
    except Exception as e:
        print(f"Warning: Invalid event skipped - {e}", file=sys.stderr)
        return False


def main():
    """Main function to run the database consumer."""
    print(f"Starting database consumer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Group ID: {GROUP_ID}")
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("Press Ctrl+C to stop")
    
    # Create database connection
    db_conn = create_db_connection()
    if not db_conn:
        print("Failed to connect to database. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    def signal_handler(signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\nShutting down...")
        consumer.close()
        db_conn.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    events_processed = 0
    events_stored = 0
    start_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(1.0)  # 1 second timeout
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}", file=sys.stderr)
                continue
            if msg.value():
                events_processed += 1
                event_data = json.loads(msg.value().decode('utf-8'))
                
                if validate_and_store_event(db_conn, event_data):
                    events_stored += 1
                
                # Print statistics every 100 events
                if events_processed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = events_processed / elapsed if elapsed > 0 else 0
                    print(f"Processed: {events_processed}, Stored: {events_stored}, Rate: {rate:.1f} events/sec")
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error in consumer: {e}", file=sys.stderr)
    finally:
        print(f"Final stats - Processed: {events_processed}, Stored: {events_stored}")
        consumer.close()
        db_conn.close()


if __name__ == '__main__':
    main()
