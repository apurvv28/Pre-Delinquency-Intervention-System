import json
from src.cache.redis_client import stream_publish

def publish_transaction(customer_id: str, payload: dict) -> bool:
    """Publish transaction event to Redis Stream."""
    try:
        msg_id = stream_publish(payload)
        if msg_id:
            print(f"Published to stream: {customer_id} | id={msg_id}")
            return True
        return False
    except Exception as e:
        print(f"Publish error: {e}")
        return False