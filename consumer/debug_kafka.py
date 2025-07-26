from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import json

def check_kafka_topics():
    """Check what topics exist in Kafka"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092']
        )
        
        topics = admin_client.list_topics()
        print("📋 Available Kafka topics:")
        for topic in sorted(topics):
            print(f"  - {topic}")
            
        return list(topics)
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        return []

def check_debezium_status():
    """Check Debezium connector status"""
    import requests
    try:
        response = requests.get('http://localhost:8083/connectors')
        connectors = response.json()
        print(f"\n🔌 Debezium connectors: {connectors}")
        
        for connector in connectors:
            status_response = requests.get(f'http://localhost:8083/connectors/{connector}/status')
            status = status_response.json()
            print(f"\n📊 {connector} status:")
            print(f"  State: {status.get('connector', {}).get('state')}")
            print(f"  Tasks: {len(status.get('tasks', []))}")
            
    except Exception as e:
        print(f"❌ Error checking Debezium: {e}")

def peek_messages(topic, max_messages=5):
    """Peek at messages in a topic"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        print(f"\n👀 Peeking at {topic} (max {max_messages} messages):")
        count = 0
        for message in consumer:
            if count >= max_messages:
                break
            print(f"  Message {count + 1}: {message.value[:200]}...")
            count += 1
            
        if count == 0:
            print("  No messages found")
            
        consumer.close()
        
    except Exception as e:
        print(f"❌ Error peeking at {topic}: {e}")

if __name__ == "__main__":
    print("🔍 Kafka Debug Information")
    print("=" * 50)
    
    topics = check_kafka_topics()
    check_debezium_status()
    
    # Check specific Debezium topics
    debezium_topics = [t for t in topics if 'dbserver1' in t]
    for topic in debezium_topics:
        peek_messages(topic)
