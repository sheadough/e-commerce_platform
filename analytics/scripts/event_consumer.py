import json
from kafka import KafkaConsumer
from clickhouse_driver import Client
import logging

class EcommerceEventConsumer:
    def __init__(self, bootstrap_servers=['kafka:9092'], clickhouse_host='clickhouse'):
        self.consumer = KafkaConsumer(
            'user_events', 'product_events', 'order_events',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='analytics_consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        self.clickhouse = Client(clickhouse_host)
        self.setup_clickhouse_tables()
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def setup_clickhouse_tables(self):
        """Create ClickHouse tables for real-time analytics"""
        tables = [
            """
            CREATE TABLE IF NOT EXISTS user_events (
                event_id String,
                timestamp DateTime,
                user_id String,
                event_type String,
                page String,
                session_id Nullable(String),
                user_agent Nullable(String),
                ip_address Nullable(String),
                metadata String
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, user_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS product_events (
                event_id String,
                timestamp DateTime,
                product_id String,
                event_type String,
                user_id Nullable(String),
                quantity Int32,
                price Nullable(Float64),
                category_id Nullable(String),
                metadata String
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, product_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS order_events (
                event_id String,
                timestamp DateTime,
                order_id String,
                event_type String,
                user_id Nullable(String),
                total_amount Nullable(Float64),
                items String,
                payment_method Nullable(String),
                metadata String
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, order_id)
            """
        ]
        
        for table_sql in tables:
            self.clickhouse.execute(table_sql)
    
    def process_events(self):
        """Process events from Kafka and store in ClickHouse"""
        for message in self.consumer:
            topic = message.topic
            event = message.value
            
            try:
                if topic == 'user_events':
                    self.process_user_event(event)
                elif topic == 'product_events':
                    self.process_product_event(event)
                elif topic == 'order_events':
                    self.process_order_event(event)
                    
            except Exception as e:
                self.logger.error(f"Error processing event from {topic}: {e}")
    
    def process_user_event(self, event):
        self.clickhouse.execute(
            """
            INSERT INTO user_events 
            (event_id, timestamp, user_id, event_type, page, session_id, user_agent, ip_address, metadata)
            VALUES
            """,
            [(
                event['event_id'],
                event['timestamp'],
                event['user_id'],
                event['event_type'],
                event['page'],
                event.get('session_id'),
                event.get('user_agent'),
                event.get('ip_address'),
                json.dumps(event.get('metadata', {}))
            )]
        )
    
    def process_product_event(self, event):
        self.clickhouse.execute(
            """
            INSERT INTO product_events 
            (event_id, timestamp, product_id, event_type, user_id, quantity, price, category_id, metadata)
            VALUES
            """,
            [(
                event['event_id'],
                event['timestamp'],
                event['product_id'],
                event['event_type'],
                event.get('user_id'),
                event.get('quantity', 1),
                event.get('price'),
                event.get('category_id'),
                json.dumps(event.get('metadata', {}))
            )]
        )
    
    def process_order_event(self, event):
        self.clickhouse.execute(
            """
            INSERT INTO order_events 
            (event_id, timestamp, order_id, event_type, user_id, total_amount, items, payment_method, metadata)
            VALUES
            """,
            [(
                event['event_id'],
                event['timestamp'],
                event['order_id'],
                event['event_type'],
                event.get('user_id'),
                event.get('total_amount'),
                json.dumps(event.get('items', [])),
                event.get('payment_method'),
                json.dumps(event.get('metadata', {}))
            )]
        )

if __name__ == "__main__":
    consumer = EcommerceEventConsumer()
    consumer.process_events()