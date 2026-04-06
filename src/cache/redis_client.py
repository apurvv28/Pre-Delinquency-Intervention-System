import os
import json
from upstash_redis import Redis
from dotenv import load_dotenv

load_dotenv()

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)

CACHE_TTL      = 60 * 30  # 30 minutes
STREAM_KEY     = os.getenv("REDIS_STREAM_KEY",     "pie:transactions")
CONSUMER_GROUP = os.getenv("REDIS_CONSUMER_GROUP", "pie-consumers")
CONSUMER_NAME  = os.getenv("REDIS_CONSUMER_NAME",  "pie-worker-1")

# ── Cache helpers ────────────────────────────────────────────────

def get_cached_score(customer_id: str):
    try:
        data = redis.get(f"risk:{customer_id}")
        if data:
            return json.loads(data) if isinstance(data, str) else data
        return None
    except Exception as e:
        print(f"Redis GET error: {e}")
        return None

def set_cached_score(customer_id: str, score_data: dict):
    try:
        redis.setex(
            f"risk:{customer_id}",
            CACHE_TTL,
            json.dumps(score_data)
        )
    except Exception as e:
        print(f"Redis SET error: {e}")

def delete_cached_score(customer_id: str):
    try:
        redis.delete(f"risk:{customer_id}")
    except Exception as e:
        print(f"Redis DELETE error: {e}")

# ── Stream helpers (replaces Kafka) ─────────────────────────────

def stream_publish(payload: dict) -> str:
    """Publish a message to the Redis stream. Returns message ID."""
    try:
        # xadd expects flat string fields — serialize nested dict as JSON
        msg_id = redis.execute(["XADD", STREAM_KEY, "*", "data", json.dumps(payload)])
        print(f"Stream published: {msg_id}")
        return msg_id
    except Exception as e:
        print(f"Stream publish error: {e}")
        return ""

def stream_create_group():
    """Create consumer group — safe to call multiple times."""
    try:
        redis.execute(["XGROUP", "CREATE", STREAM_KEY, CONSUMER_GROUP, "0", "MKSTREAM"])
        print(f"Consumer group '{CONSUMER_GROUP}' created")
    except Exception as e:
        # BUSYGROUP error means group already exists — that's fine
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists")
        else:
            print(f"Group create error: {e}")

def stream_read(count: int = 10) -> list:
    """Read pending messages from stream as this consumer."""
    try:
        messages = redis.execute([
            "XREADGROUP", "GROUP", CONSUMER_GROUP, CONSUMER_NAME,
            "COUNT", str(count),
            "STREAMS", STREAM_KEY, ">"
        ])
        return messages or []
    except Exception as e:
        print(f"Stream read error: {e}")
        return []

def stream_ack(msg_id: str):
    """Acknowledge a processed message."""
    try:
        redis.execute(["XACK", STREAM_KEY, CONSUMER_GROUP, msg_id])
    except Exception as e:
        print(f"Stream ack error: {e}")