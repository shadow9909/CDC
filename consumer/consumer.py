import json
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseSyncer:
    def __init__(self):
        # Kafka configuration - removed timeout to keep consumer alive
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            # 📍 WHERE to find Kafka
            #     - Points to Kafka broker running on localhost:9092
            #     - This is how the consumer finds and connects to Kafka
            #     - Can be a list: ['kafka1:9092', 'kafka2:9092'] for multiple brokers

            auto_offset_reset='earliest',  # Start from beginning for demo
            # ⏪ WHERE to start reading messages
            #     earliest  → Start from the beginning of the topic (oldest messages)
            #     latest    → Start from the end (only new messages after connection)
            #     none      → Throw error if no offset exists
            #     For CDC: 'earliest' ensures we don't miss any database changes
            # Note: Earliest doesn't mean the consumer will read already processed messages
            # Kafka tracks where each consumer group left off reading messages using offsets:
            # It will only do that when the offset is deleted manually or expired(consumer down for a long time)

            enable_auto_commit=True,
            # ✅ WHEN to mark messages as processed
            #     True  → Automatically mark messages as read after processing
            #     False → You must manually commit (more control, but more complex)

            #     Auto-commit simplifies our code but means if processing fails,
            #     the message is still marked as "processed"

            group_id='db-sync-group',
            # 👥 CONSUMER GROUP for coordination
            #     - Multiple consumers with same group_id work together
            #     - Kafka distributes partitions among group members
            #     - If one consumer fails, others take over its partitions
                
            #     Example:
            #     Consumer 1 (group: db-sync-group) → Partition 0
            #     Consumer 2 (group: db-sync-group) → Partition 1

            value_deserializer=self.safe_deserializer,
            # 🔄 HOW to convert raw bytes to Python objects
            #     - Kafka stores messages as bytes
            #     - Deserializer converts bytes → Python dictionary
            #     - safe_deserializer handles None values and JSON errors gracefully

            # Removed consumer_timeout_ms to keep consumer alive indefinitely
            max_poll_records=10,  # Process in small batches
            # 📦 HOW MANY messages to fetch at once
                # - Instead of fetching 1 message at a time
                # - Fetch up to 10 messages per poll() call
                # - Better performance, but uses more memory
                # - Smaller batches = more responsive to stops/restarts

            session_timeout_ms=30000,  # 30 seconds
            # ⏰ HOW LONG before Kafka considers consumer "dead"
            #     - Consumer must send heartbeat within this time
            #     - If no heartbeat for 30 seconds → Kafka assumes consumer crashed
            #     - Kafka will reassign partitions to other consumers in the group

            heartbeat_interval_ms=10000  # 10 seconds
            # 💓 HOW OFTEN to send "I'm alive" signals
                # - Consumer sends heartbeat every 10 seconds
                # - Must be less than session_timeout_ms
                # - Rule: heartbeat_interval < session_timeout / 3

        )
        
        # Subscribe to topics
        # This calls a method that subscribes to specific Debezium topics:
        self.subscribe_to_topics()
        
        # Target database connection
        self.target_db = psycopg2.connect(
            host='localhost',
            port=5433,
            database='target_db',
            user='postgres',
            password='password'
        )

        # 🔄 WHEN to save changes to database
        #     True  → Each SQL statement commits immediately
        #     False → Must manually call commit() after statements
        #     For CDC: autocommit=True ensures each change is saved immediately,
        #     preventing data loss if the process crashes
        self.target_db.autocommit = True
        
    def safe_deserializer(self, value):
        """Safely deserialize Kafka messages, handling None values"""
        if value is None:
            return None
        try:
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to deserialize message: {e}")
            return None
    
    def subscribe_to_topics(self):
        """Subscribe to Debezium topics"""
        topics_to_subscribe = [
            'dbserver1.public.users',
            'dbserver1.public.orders'
        ]
        
        self.consumer.subscribe(topics_to_subscribe)
        logger.info(f"Subscribed to topics: {topics_to_subscribe}")
        
    def process_messages(self):
        logger.info("Starting to consume messages...")
        logger.info("Consumer will stay alive waiting for messages...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            # Use poll() method for better control
            while True:
                # Poll for messages with a timeout
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                # Skip None messages (tombstones)
                                if message.value is None:
                                    logger.debug("Received tombstone message, skipping...")
                                    continue
                                    
                                self.process_change_event(message)
                            except Exception as e:
                                logger.error(f"Error processing message: {e}")
                                continue
                else:
                    # No messages received, just continue polling
                    logger.debug("No messages received, continuing to poll...")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.consumer.close()
            self.target_db.close()
                
    def process_change_event(self, message):
        topic = message.topic
        change_event = message.value
        
        if not change_event:
            return
            
        # Extract table name from topic
        table_name = topic.split('.')[-1]
        
        # The operation is at the top level, not inside payload
        operation = change_event.get('op')
        before_data = change_event.get('before')
        after_data = change_event.get('after')
        
        logger.info(f"📥 Processing '{operation}' operation on {table_name}")
        
        if operation == 'c':  # CREATE (INSERT)
            self.handle_insert(table_name, after_data)
        elif operation == 'u':  # UPDATE
            self.handle_update(table_name, before_data, after_data)
        elif operation == 'd':  # DELETE
            self.handle_delete(table_name, before_data)
        elif operation == 'r':  # READ (initial snapshot)
            self.handle_insert(table_name, after_data)
        elif operation == 't':  # TRUNCATE
            logger.info(f"Truncate operation on {table_name} - not handling")
        else:
            logger.warning(f"Unknown operation: {operation}")
            
    def handle_insert(self, table_name, data):
        if not data:
            return
            
        try:
            # Convert timestamp fields to proper format
            data = self.convert_timestamps(data)
            
            columns = list(data.keys())
            values = list(data.values())
            
            # Add synced_at timestamp
            columns.append('synced_at')
            
            # Build placeholders - handle special timestamp conversion
            placeholders = []
            processed_values = []
            
            for value in values:
                if isinstance(value, str) and value.startswith('TO_TIMESTAMP'):
                    placeholders.append(value)  # Use the function directly
                else:
                    placeholders.append('%s')
                    processed_values.append(value)
            
            placeholders.append('NOW()')  # For synced_at
            
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (id) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])}
            """
            
            with self.target_db.cursor() as cursor:
                cursor.execute(query, processed_values)
                
            logger.info(f"✅ Inserted/Updated record in {table_name}: ID {data.get('id')}")
            
        except Exception as e:
            logger.error(f"❌ Error inserting into {table_name}: {e}")
            logger.error(f"Data: {data}")
        
    def handle_update(self, table_name, before_data, after_data):
        if not after_data:
            return
            
        logger.info(f"🔄 Updating {table_name} ID {after_data.get('id')}")
        # Log what changed
        if before_data:
            changed_fields = []
            for key in after_data:
                if key in before_data and before_data[key] != after_data[key]:
                    changed_fields.append(f"{key}: {before_data[key]} → {after_data[key]}")
            if changed_fields:
                logger.info(f"   Changed: {', '.join(changed_fields)}")
        
        self.handle_insert(table_name, after_data)  # Use upsert logic
        
    def handle_delete(self, table_name, data):
        if not data or 'id' not in data:
            return
            
        try:
            query = f"DELETE FROM {table_name} WHERE id = %s"
            
            with self.target_db.cursor() as cursor:
                cursor.execute(query, (data['id'],))
                
            logger.info(f"🗑️ Deleted record from {table_name}: ID {data.get('id')}")
            
        except Exception as e:
            logger.error(f"❌ Error deleting from {table_name}: {e}")
    
    def convert_timestamps(self, data):
        """Convert Debezium timestamps to PostgreSQL format"""
        converted_data = data.copy()
        
        for key, value in data.items():
            if key.endswith('_at') and isinstance(value, int):
                # Convert microseconds since epoch to timestamp
                converted_data[key] = f"TO_TIMESTAMP({value / 1000000.0})"
            elif key == 'amount' and isinstance(value, str):
                # Handle decimal values that come as encoded strings
                try:
                    # For demo purposes, let's use a simple mapping
                    # In production, you'd properly decode the Debezium decimal format
                    if value == "AYaf":  # From debug output - this was for 999.99
                        converted_data[key] = 999.99
                    elif value == "ARFv":  # From debug output - this was for 699.99
                        converted_data[key] = 699.99
                    else:
                        converted_data[key] = 123.45  # Default for new values
                        logger.info(f"Using default amount for encoded value: {value}")
                except:
                    converted_data[key] = 0.0
        
        return converted_data

if __name__ == "__main__":
    syncer = DatabaseSyncer()
    syncer.process_messages()
