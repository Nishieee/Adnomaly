from feast import FileSource, FeatureView, Field
from feast.types import Float32, Int64
from datetime import timedelta
from entities import geo, platform

traffic_5m_source = FileSource(
    name="traffic_5m_offline",
    path="data/offline_store/features/traffic_5m_by_geo_platform/*",
    timestamp_field="window_end",
)

traffic_5m_by_geo_platform = FeatureView(
    name="traffic_5m_by_geo_platform",
    entities=[geo, platform],
    ttl=timedelta(days=3),
    schema=[
        Field(name="ctr_avg", dtype=Float32),
        Field(name="bounce_rate_avg", dtype=Float32),
        Field(name="event_count", dtype=Int64),
    ],
    online=True,
    source=traffic_5m_source,
)
