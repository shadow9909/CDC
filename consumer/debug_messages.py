from kafka import KafkaConsumer
import json

def inspect_raw_messages():
    """Look at raw messages to see what we're actually receiving"""
    consumer = KafkaConsumer(
        'dbserver1.public.users',
        'dbserver1.public.orders',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: x  # Keep raw bytes
    )
    
    print("🔍 Inspecting raw messages:")
    message_count = 0
    
    for message in consumer:
        message_count += 1
        print(f"\n--- Message {message_count} ---")
        print(f"Topic: {message.topic}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Value type: {type(message.value)}")
        
        if message.value is None:
            print("Value: None (tombstone message)")
        else:
            try:
                # Try to decode as string
                decoded = message.value.decode('utf-8')
                print(f"Value (raw): {decoded[:500]}...")
                
                # Try to parse as JSON
                parsed = json.loads(decoded)
                print(f"Parsed JSON keys: {list(parsed.keys())}")
                if 'payload' in parsed:
                    payload = parsed['payload']
                    print(f"Payload keys: {list(payload.keys()) if payload else 'None'}")
                    if payload and 'op' in payload:
                        print(f"Operation: {payload['op']}")
                        
            except Exception as e:
                print(f"Could not decode: {e}")
                print(f"Raw bytes: {message.value[:100]}...")
        
        if message_count >= 10:  # Limit to first 10 messages
            break
    
    consumer.close()
    print(f"\nTotal messages inspected: {message_count}")

if __name__ == "__main__":
    inspect_raw_messages()
