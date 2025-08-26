#!/usr/bin/env python3
"""
PyFlink job for Adnomaly Phase 2 - Basic Processing
Consumes clickstream events and produces 5-minute sliding window aggregates.
"""

import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Tuple

# Conditional imports for testing without Flink runtime
try:
    from pyflink.common import Types, Time
    from pyflink.datastream import DataStream, StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaRecordDeserializationSchema, KafkaSink, KafkaRecordSerializationSchema
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.window import SlidingEventTimeWindows
    from pyflink.datastream.functions import AggregateFunction
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False


class ClickstreamEvent:
    """Represents a clickstream event with required fields for processing."""
    
    def __init__(self, timestamp: datetime, geo: str, platform: str, ctr: float, bounce_rate: float):
        self.timestamp = timestamp
        self.geo = geo
        self.platform = platform
        self.ctr = ctr
        self.bounce_rate = bounce_rate


class WindowAggregate:
    """Represents aggregated metrics for a window."""
    
    def __init__(self):
        self.ctr_sum = 0.0
        self.bounce_rate_sum = 0.0
        self.count = 0
    
    def add(self, event: ClickstreamEvent):
        self.ctr_sum += event.ctr
        self.bounce_rate_sum += event.bounce_rate
        self.count += 1
    
    def get_result(self) -> Tuple[float, float, int]:
        if self.count == 0:
            return 0.0, 0.0, 0
        return self.ctr_sum / self.count, self.bounce_rate_sum / self.count, self.count


if FLINK_AVAILABLE:
    class ClickstreamAggregateFunction(AggregateFunction):
        """Custom aggregate function for clickstream events."""
        
        def create_accumulator(self) -> WindowAggregate:
            return WindowAggregate()
        
        def add(self, value: ClickstreamEvent, accumulator: WindowAggregate) -> WindowAggregate:
            accumulator.add(value)
            return accumulator
        
        def get_result(self, accumulator: WindowAggregate) -> Tuple[float, float, int]:
            return accumulator.get_result()
        
        def merge(self, a: WindowAggregate, b: WindowAggregate) -> WindowAggregate:
            a.ctr_sum += b.ctr_sum
            a.bounce_rate_sum += b.bounce_rate_sum
            a.count += b.count
            return a


def parse_clickstream_event(json_str: str) -> ClickstreamEvent:
    """Parse a JSON clickstream event into a ClickstreamEvent object."""
    try:
        data = json.loads(json_str)
        
        # Extract and validate required fields
        timestamp_str = data.get('timestamp')
        if not timestamp_str or not timestamp_str.endswith('Z'):
            return None
        
        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        
        geo = data.get('geo')
        if not geo or len(geo) != 2 or not geo.isupper():
            return None
        
        platform = data.get('platform')
        if platform not in ['web', 'ios', 'android']:
            return None
        
        ctr = data.get('CTR')
        if not isinstance(ctr, (int, float)) or ctr < 0 or ctr > 1:
            return None
        
        bounce_rate = data.get('bounce_rate')
        if not isinstance(bounce_rate, (int, float)) or bounce_rate < 0 or bounce_rate > 1:
            return None
        
        return ClickstreamEvent(timestamp, geo, platform, float(ctr), float(bounce_rate))
        
    except (json.JSONDecodeError, ValueError, TypeError):
        return None


def format_window_result(window_data: Tuple[Tuple[str, str], Tuple[float, float, int], int, int]) -> str:
    """Format window result as JSON string."""
    key, (ctr_avg, bounce_rate_avg, event_count), window_start, window_end = window_data
    
    geo, platform = key
    
    # Convert timestamps to ISO8601 UTC
    start_time = datetime.fromtimestamp(window_start / 1000, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
    end_time = datetime.fromtimestamp(window_end / 1000, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
    
    result = {
        "window_start": start_time,
        "window_end": end_time,
        "geo": geo,
        "platform": platform,
        "ctr_avg": round(ctr_avg, 4),
        "bounce_rate_avg": round(bounce_rate_avg, 3),
        "event_count": event_count
    }
    
    return json.dumps(result)


# Flink-specific functions only if Flink is available
if FLINK_AVAILABLE:
    def create_kafka_source() -> KafkaSource:
        """Create Kafka source for clickstream topic."""
        return KafkaSource.builder() \
            .set_bootstrap_servers("kafka:9092") \
            .set_topics("clickstream") \
            .set_group_id("flink-clickstream-consumer") \
            .set_starting_offsets("latest") \
            .set_value_only_deserializer(
                KafkaRecordDeserializationSchema.of(SimpleStringSchema())
            ) \
            .build()


    def create_kafka_sink() -> KafkaSink:
        """Create Kafka sink for features topic."""
        return KafkaSink.builder() \
            .set_bootstrap_servers("kafka:9092") \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic("features")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ) \
            .build()


    def process_stream(env: StreamExecutionEnvironment) -> DataStream:
        """Process the clickstream data stream."""
        
        # Create source
        source = create_kafka_source()
        stream = env.from_source(source, Types.STRING(), "clickstream-source")
        
        # Parse events and filter invalid ones
        parsed_stream = stream \
            .map(lambda x: parse_clickstream_event(x), output_type=Types.STRING()) \
            .filter(lambda x: x is not None)
        
        # Assign timestamps and watermarks
        timestamped_stream = parsed_stream \
            .assign_timestamps_and_watermarks(
                lambda event, context: event.timestamp.timestamp() * 1000,
                lambda event, context: event.timestamp.timestamp() * 1000 - 30000  # 30s bounded out-of-orderness
            )
        
        # Key by geo and platform, apply sliding window
        windowed_stream = timestamped_stream \
            .key_by(lambda event: (event.geo, event.platform)) \
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) \
            .aggregate(ClickstreamAggregateFunction())
        
        # Convert to output format
        output_stream = windowed_stream \
            .map(lambda x: format_window_result(x), output_type=Types.STRING())
        
        return output_stream


    def main():
        """Main function to set up and execute the Flink job."""
        
        # Set up execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(2)
        
        # Set up checkpointing
        env.enable_checkpointing(60000)  # Checkpoint every minute
        
        # Process the stream
        output_stream = process_stream(env)
        
        # Create sink and write to Kafka
        sink = create_kafka_sink()
        output_stream.sink_to(sink)
        
        # Execute the job
        env.execute("adnomaly-phase2")


    if __name__ == '__main__':
        main()
