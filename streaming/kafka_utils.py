import os
import logging
from typing import Optional, Tuple
from confluent_kafka import Consumer, Producer, KafkaError

logger = logging.getLogger(__name__)


def build_consumer(group_id: str, bootstrap_servers: str) -> Consumer:
    """Build a Kafka consumer with standard configuration."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start from earliest to process all messages
        'enable.auto.commit': False,  # Disable auto commit
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 2000,
        'max.poll.interval.ms': 300000,  # 5 minutes
        'enable.partition.eof': False,  # Don't treat EOF as error
        'allow.auto.create.topics': True
    }
    
    consumer = Consumer(config)
    logger.info(f"Created consumer with group_id: {group_id}")
    return consumer


def build_producer(bootstrap_servers: str) -> Producer:
    """Build a Kafka producer with standard configuration."""
    config = {
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all',
        'retries': 3,
        'delivery.timeout.ms': 30000,
        'request.timeout.ms': 5000,
        'linger.ms': 10,
        'batch.size': 16384
    }
    
    producer = Producer(config)
    logger.info("Created producer")
    return producer


def safe_poll(consumer: Consumer, timeout: float = 1.0) -> Tuple[Optional[object], Optional[KafkaError]]:
    """
    Safely poll for messages from Kafka consumer.
    
    Returns:
        Tuple of (message, error) where one will be None
    """
    try:
        message = consumer.poll(timeout=timeout)
        
        if message is None:
            return None, None
        
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, not an error
                return None, None
            else:
                # Actual error
                logger.error(f"Kafka error: {message.error()}")
                return None, message.error()
        
        return message, None
        
    except Exception as e:
        logger.error(f"Error polling Kafka: {e}")
        return None, KafkaError(KafkaError._UNKNOWN, str(e))


def commit_sync(consumer: Consumer, message) -> bool:
    """Commit the message offset synchronously after successful processing."""
    try:
        consumer.commit(message)
        return True
    except Exception as e:
        logger.error(f"Error committing offset: {e}")
        return False


def delivery_report(err, msg):
    """Delivery report callback for producer."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def get_committed_offsets(consumer: Consumer, topic: str) -> dict:
    """Get committed offsets for each partition of a topic."""
    try:
        metadata = consumer.list_topics(topic=topic)
        if topic not in metadata.topics:
            return {}
        
        topic_metadata = metadata.topics[topic]
        committed_offsets = {}
        
        for partition_id in topic_metadata.partitions:
            committed = consumer.committed([(topic, partition_id)])
            committed_offsets[partition_id] = committed[0].offset if committed[0].offset >= 0 else 0
        
        return committed_offsets
    except Exception as e:
        logger.error(f"Error getting committed offsets: {e}")
        return {}


def get_highwater_offsets(consumer: Consumer, topic: str) -> dict:
    """Get highwater offsets for each partition of a topic."""
    try:
        metadata = consumer.list_topics(topic=topic)
        if topic not in metadata.topics:
            return {}
        
        topic_metadata = metadata.topics[topic]
        highwater_offsets = {}
        
        for partition_id in topic_metadata.partitions:
            highwater = consumer.get_watermark_offsets((topic, partition_id))[1]
            highwater_offsets[partition_id] = highwater
        
        return highwater_offsets
    except Exception as e:
        logger.error(f"Error getting highwater offsets: {e}")
        return {}


def get_partition_lag(consumer: Consumer, topic: str) -> dict:
    """Get lag for each partition of a topic."""
    try:
        committed = get_committed_offsets(consumer, topic)
        highwater = get_highwater_offsets(consumer, topic)
        
        partition_lags = {}
        for partition_id in committed:
            if partition_id in highwater:
                lag = max(0, highwater[partition_id] - committed[partition_id])
                partition_lags[partition_id] = lag
        
        return partition_lags
    except Exception as e:
        logger.error(f"Error getting partition lag: {e}")
        return {}
