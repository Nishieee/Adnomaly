#!/usr/bin/env python3
"""
Test script for Adnomaly Phase 3 Feast integration.
Tests online feature retrieval from Redis.
"""

import os
import sys
from feast import FeatureStore

# Feast configuration
FEATURE_REPO_PATH = os.getenv('FEATURE_REPO_PATH', 'features/feature_repo')


def test_feast_connection():
    """Test basic Feast connection and feature store creation."""
    try:
        store = FeatureStore(repo_path=FEATURE_REPO_PATH)
        print("✅ Successfully created FeatureStore")
        return store
    except Exception as e:
        print(f"❌ Failed to create FeatureStore: {e}", file=sys.stderr)
        return None


def test_online_features(store):
    """Test online feature retrieval."""
    try:
        # Test with a sample entity
        entity_rows = [{"geo": "US", "platform": "ios"}]
        
        features = store.get_online_features(
            features=[
                "traffic_5m_by_geo_platform:ctr_avg",
                "traffic_5m_by_geo_platform:bounce_rate_avg",
                "traffic_5m_by_geo_platform:event_count"
            ],
            entity_rows=entity_rows
        ).to_dict()
        
        print("✅ Successfully retrieved online features")
        print(f"Features: {features}")
        
        # Check if we got actual values
        ctr_avg = features.get('ctr_avg', [None])[0]
        bounce_rate_avg = features.get('bounce_rate_avg', [None])[0]
        event_count = features.get('event_count', [None])[0]
        
        if ctr_avg is not None and bounce_rate_avg is not None and event_count is not None:
            print("✅ Retrieved non-null feature values")
            print(f"  CTR Average: {ctr_avg}")
            print(f"  Bounce Rate Average: {bounce_rate_avg}")
            print(f"  Event Count: {event_count}")
        else:
            print("⚠️  Retrieved null values (no data in online store yet)")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to retrieve online features: {e}", file=sys.stderr)
        return False


def test_multiple_entities(store):
    """Test feature retrieval for multiple entities."""
    try:
        # Test with multiple entities
        entity_rows = [
            {"geo": "US", "platform": "ios"},
            {"geo": "DE", "platform": "web"},
            {"geo": "JP", "platform": "android"}
        ]
        
        features = store.get_online_features(
            features=[
                "traffic_5m_by_geo_platform:ctr_avg",
                "traffic_5m_by_geo_platform:bounce_rate_avg",
                "traffic_5m_by_geo_platform:event_count"
            ],
            entity_rows=entity_rows
        ).to_dict()
        
        print("✅ Successfully retrieved features for multiple entities")
        print(f"Number of entities: {len(entity_rows)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to retrieve features for multiple entities: {e}", file=sys.stderr)
        return False


def main():
    """Main function to run Feast tests."""
    print("Testing Adnomaly Phase 3 Feast Integration")
    print("=" * 50)
    
    # Test basic connection
    store = test_feast_connection()
    if not store:
        sys.exit(1)
    
    # Test online features
    print("\nTesting online feature retrieval...")
    test_online_features(store)
    
    # Test multiple entities
    print("\nTesting multiple entity retrieval...")
    test_multiple_entities(store)
    
    print("\nFeast integration test completed!")


if __name__ == '__main__':
    main()
