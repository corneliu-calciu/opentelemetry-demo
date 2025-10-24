#!/usr/bin/env python3
"""
Kafka Topic and Lag Monitor
Lists all Kafka topics and shows consumer lag per group.
Requires: pip install confluent-kafka
"""

from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException, Consumer, TopicPartition
import sys


def get_admin_client(bootstrap_servers: str) -> AdminClient:
    """Create an AdminClient connection to Kafka."""
    return AdminClient({'bootstrap.servers': bootstrap_servers})


def list_topics(admin: AdminClient):
    """List all Kafka topics."""
    metadata = admin.list_topics(timeout=10)
    print("\n🧩 Topics:")
    for topic in metadata.topics.values():
        print(f"  • {topic.topic}")


def get_consumer_lag(bootstrap_servers: str, group_id: str, admin: AdminClient):
    """Calculate consumer lag for a specific consumer group."""
    try:
        # Create a consumer to query offsets
        consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'enable.auto.commit': False
        })
        
        # Get the committed offsets for this group
        metadata = admin.list_topics(timeout=10)
        all_topics = list(metadata.topics.keys())
        
        lag_info = {}
        
        for topic in all_topics:
            if topic.startswith('__'):  # Skip internal topics
                continue
                
            # Get partition metadata
            topic_metadata = metadata.topics.get(topic)
            if not topic_metadata:
                continue
            
            for partition_id in topic_metadata.partitions.keys():
                tp = TopicPartition(topic, partition_id)
                
                # Get committed offset
                committed = consumer.committed([tp], timeout=5.0)
                if not committed or committed[0].offset < 0:
                    continue  # No committed offset for this partition
                
                committed_offset = committed[0].offset
                
                # Get high water mark (latest offset)
                low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
                
                # Calculate lag
                lag = high - committed_offset
                
                if topic not in lag_info:
                    lag_info[topic] = []
                
                lag_info[topic].append({
                    'partition': partition_id,
                    'committed': committed_offset,
                    'high_watermark': high,
                    'lag': lag
                })
        
        consumer.close()
        return lag_info
        
    except Exception as e:
        print(f"    ⚠️ Error calculating lag: {e}")
        return {}


def describe_consumer_groups(bootstrap_servers: str, admin: AdminClient):
    """Describe consumer groups and compute lag."""
    print("\n📊 Consumer Groups and Lag:")
    try:
        # list_consumer_groups() returns a ListConsumerGroupsResult
        result = admin.list_consumer_groups()
        groups = result.result()
        
        if not groups.valid:
            print("  (No valid consumer groups found.)")
            return
        
        group_ids = [g.group_id for g in groups.valid]
        if not group_ids:
            print("  (No active consumer groups found.)")
            return
        
        print(f"  Found {len(group_ids)} consumer group(s):\n")
        
        for group_id in group_ids:
            print(f"  Group: {group_id}")
            
            lag_info = get_consumer_lag(bootstrap_servers, group_id, admin)
            
            if not lag_info:
                print("    (No active subscriptions)")
                continue
            
            total_lag = 0
            for topic, partitions in sorted(lag_info.items()):
                for part_info in sorted(partitions, key=lambda x: x['partition']):
                    total_lag += part_info['lag']
                    print(f"    Topic: {topic:<20} "
                          f"Partition: {part_info['partition']:<3} "
                          f"Committed: {part_info['committed']:<8} "
                          f"Latest: {part_info['high_watermark']:<8} "
                          f"Lag: {part_info['lag']}")
            
            print(f"    {'─' * 70}")
            print(f"    Total Lag: {total_lag}\n")
            
    except Exception as e:
        print(f"  ⚠️ Error listing consumer groups: {e}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python kafka_lag_monitor.py <bootstrap_servers>")
        print("Example: python kafka_lag_monitor.py localhost:9092")
        sys.exit(1)

    bootstrap_servers = sys.argv[1]
    admin = get_admin_client(bootstrap_servers)

    list_topics(admin)
    describe_consumer_groups(bootstrap_servers, admin)


if __name__ == "__main__":
    main()

