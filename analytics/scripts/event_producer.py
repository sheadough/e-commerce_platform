import json
import time
from kafka import KafkaProducer
from datetime import datetime
import uuid

class EcommerceEventProducer:
    def __init__(self, bootstrap_servers=['kafka:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v else None
        )
    
    def send_user_event(self, user_id: str, event_type: str, page: str, **kwargs):
        """Send user behavior events"""
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'event_type': event_type,
            'page': page,
            'session_id': kwargs.get('session_id'),
            'user_agent': kwargs.get('user_agent'),
            'ip_address': kwargs.get('ip_address'),
            'metadata': kwargs.get('metadata', {})
        }
        
        self.producer.send('user_events', key=user_id, value=event)
    
    def send_product_event(self, product_id: str, event_type: str, **kwargs):
        """Send product-related events"""
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'product_id': product_id,
            'event_type': event_type,  # 'view', 'add_to_cart', 'purchase', 'remove_from_cart'
            'user_id': kwargs.get('user_id'),
            'quantity': kwargs.get('quantity', 1),
            'price': kwargs.get('price'),
            'category_id': kwargs.get('category_id'),
            'metadata': kwargs.get('metadata', {})
        }
        
        self.producer.send('product_events', key=product_id, value=event)
    
    def send_order_event(self, order_id: str, event_type: str, **kwargs):
        """Send order-related events"""
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'event_type': event_type,  # 'created', 'paid', 'shipped', 'delivered', 'cancelled'
            'user_id': kwargs.get('user_id'),
            'total_amount': kwargs.get('total_amount'),
            'items': kwargs.get('items', []),
            'payment_method': kwargs.get('payment_method'),
            'metadata': kwargs.get('metadata', {})
        }
        
        self.producer.send('order_events', key=order_id, value=event)

# Integration example for your Express.js backend
# Add this to your existing controllers