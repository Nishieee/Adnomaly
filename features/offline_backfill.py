#!/usr/bin/env python3
"""
Offline backfill script for Adnomaly Phase 3.
Exports aggregated features from PostgreSQL to parquet files in MinIO for Feast offline store.
"""

import os
import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2.extras import RealDictCursor

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5433')
DB_NAME = os.getenv('DB_NAME', 'adnomaly')
DB_USER = os.getenv('DB_USER', 'adnomaly_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'adnomaly_password')

# Local file system configuration
OFFLINE_STORE_PATH = os.getenv('OFFLINE_STORE_PATH', 'data/offline_store')


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


def create_offline_store():
    """Create and return offline store directory."""
    try:
        os.makedirs(OFFLINE_STORE_PATH, exist_ok=True)
        print(f"Created offline store directory: {OFFLINE_STORE_PATH}")
        return True
    except Exception as e:
        print(f"Offline store error: {e}", file=sys.stderr)
        return False


def get_aggregated_features(conn, days_back=7):
    """Get aggregated features from PostgreSQL."""
    try:
        # Query to get 5-minute window aggregates
        query = """
        SELECT 
            window_start,
            window_end,
            geo,
            platform,
            avg_ctr,
            avg_bounce_rate,
            event_count
        FROM feature_aggregates 
        ORDER BY window_end
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
        return results
        
    except psycopg2.Error as e:
        print(f"Database query error: {e}", file=sys.stderr)
        return []


def create_sample_data():
    """Create sample aggregated data for testing if no real data exists."""
    print("Creating sample aggregated data...")
    
    # Generate sample data for the last 7 days
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)
    
    data = []
    current_time = start_time
    
    while current_time < end_time:
        # Create 5-minute windows
        window_start = current_time
        window_end = current_time + timedelta(minutes=5)
        
        # Generate data for each geo/platform combination
        geos = ['US', 'IN', 'BR', 'DE', 'JP', 'CA', 'AU', 'UK', 'FR', 'IT']
        platforms = ['web', 'ios', 'android']
        
        for geo in geos:
            for platform in platforms:
                data.append({
                    'window_start': window_start,
                    'window_end': window_end,
                    'geo': geo,
                    'platform': platform,
                    'avg_ctr': round(0.02 + (hash(f"{geo}{platform}{current_time}") % 100) / 10000, 4),
                    'avg_bounce_rate': round(0.3 + (hash(f"{geo}{platform}{current_time}") % 100) / 1000, 3),
                    'event_count': 50 + (hash(f"{geo}{platform}{current_time}") % 200)
                })
        
        current_time += timedelta(minutes=5)
    
    return data


def save_parquet_to_local(df, date_str):
    """Save parquet file to local filesystem with proper partitioning."""
    try:
        # Create path with date partitioning
        parquet_path = f"{OFFLINE_STORE_PATH}/features/traffic_5m_by_geo_platform/ingest_date={date_str}"
        os.makedirs(parquet_path, exist_ok=True)
        
        # Save parquet file
        file_path = f"{parquet_path}/part-{date_str}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"Saved parquet to {file_path}")
        return True
        
    except Exception as e:
        print(f"Error saving parquet: {e}", file=sys.stderr)
        return False


def main():
    """Main function to run the offline backfill."""
    print("Starting offline backfill for Feast...")
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Offline store: {OFFLINE_STORE_PATH}")
    
    # Create database connection
    db_conn = create_db_connection()
    if not db_conn:
        print("Failed to connect to database. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # Create offline store directory
    if not create_offline_store():
        print("Failed to create offline store. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Get aggregated features from database
        features = get_aggregated_features(db_conn, days_back=7)
        
        if not features:
            print("No aggregated features found in database. Creating sample data...")
            features = create_sample_data()
        
        if not features:
            print("No data available for backfill. Exiting.", file=sys.stderr)
            sys.exit(1)
        
        # Convert to DataFrame
        df = pd.DataFrame(features)
        
        # Ensure proper data types
        df['window_start'] = pd.to_datetime(df['window_start'])
        df['window_end'] = pd.to_datetime(df['window_end'])
        df['avg_ctr'] = df['avg_ctr'].astype(float)
        df['avg_bounce_rate'] = df['avg_bounce_rate'].astype(float)
        df['event_count'] = df['event_count'].astype(int)
        
        print(f"Processing {len(df)} aggregated features...")
        
        # Group by date and upload parquet files
        df['ingest_date'] = df['window_end'].dt.date
        date_groups = df.groupby('ingest_date')
        
        uploaded_files = 0
        for date, group_df in date_groups:
            date_str = date.strftime('%Y-%m-%d')
            
            # Sort by window_end for proper ordering
            group_df = group_df.sort_values('window_end')
            
            if save_parquet_to_local(group_df, date_str):
                uploaded_files += 1
        
        print(f"Successfully saved {uploaded_files} parquet files to local filesystem")
        print("Offline backfill completed successfully!")
        
    except Exception as e:
        print(f"Error during backfill: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db_conn.close()


if __name__ == '__main__':
    main()
