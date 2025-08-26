#!/usr/bin/env python3
"""
Database query tool for Adnomaly project.
Simple utility to query stored clickstream data.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

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


def get_total_events(conn):
    """Get total number of events in the database."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM clickstream_events")
        return cursor.fetchone()[0]


def get_recent_events(conn, limit=10):
    """Get recent events from the database."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT timestamp, user_id_hash, ad_id, campaign_id, geo, platform, 
                   cpc, ctr, conversion, bounce_rate
            FROM clickstream_events 
            ORDER BY timestamp DESC 
            LIMIT %s
        """, (limit,))
        return cursor.fetchall()


def get_daily_stats(conn):
    """Get daily statistics."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT * FROM daily_stats 
            ORDER BY date DESC 
            LIMIT 10
        """)
        return cursor.fetchall()


def get_geo_stats(conn):
    """Get statistics by geography."""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            SELECT geo, COUNT(*) as event_count, 
                   AVG(ctr) as avg_ctr, 
                   AVG(bounce_rate) as avg_bounce_rate,
                   AVG(cpc) as avg_cpc
            FROM clickstream_events 
            GROUP BY geo 
            ORDER BY event_count DESC
        """)
        return cursor.fetchall()


def main():
    """Main function to run database queries."""
    print("Adnomaly Database Query Tool")
    print("=" * 40)
    
    # Create database connection
    conn = create_db_connection()
    if not conn:
        print("Failed to connect to database. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Get total events
        total_events = get_total_events(conn)
        print(f"Total events in database: {total_events}")
        print()
        
        if total_events > 0:
            # Get recent events
            print("Recent events:")
            print("-" * 20)
            recent_events = get_recent_events(conn, 5)
            for event in recent_events:
                print(f"  {event['timestamp']} | {event['geo']} | {event['platform']} | "
                      f"CTR: {event['ctr']:.4f} | CPC: ${event['cpc']:.3f}")
            print()
            
            # Get daily stats
            print("Daily statistics:")
            print("-" * 20)
            daily_stats = get_daily_stats(conn)
            for stat in daily_stats:
                print(f"  {stat['date']} | {stat['geo']} | {stat['platform']} | "
                      f"Events: {stat['total_events']} | Avg CTR: {stat['avg_ctr']:.4f}")
            print()
            
            # Get geo stats
            print("Statistics by geography:")
            print("-" * 20)
            geo_stats = get_geo_stats(conn)
            for stat in geo_stats:
                print(f"  {stat['geo']} | Events: {stat['event_count']} | "
                      f"Avg CTR: {stat['avg_ctr']:.4f} | Avg CPC: ${stat['avg_cpc']:.3f}")
        else:
            print("No events found in database.")
            
    except Exception as e:
        print(f"Error querying database: {e}", file=sys.stderr)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
