import json
import os
import time
from typing import Any

from dotenv import load_dotenv
from upstash_redis import Redis

load_dotenv()

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN"),
)

_redis_available = True
_redis_error_logged = False
_local_kv_store: dict[str, Any] = {}


def _log_redis_down_once(exc: Exception):
    global _redis_error_logged
    if not _redis_error_logged:
        print(f"Redis unavailable. Falling back to local in-memory cache: {exc}")
        _redis_error_logged = True


def _mark_redis_unavailable(exc: Exception):
    global _redis_available
    _redis_available = False
    _log_redis_down_once(exc)


def _redis_call(callable_fn, fallback):
    if not _redis_available:
        return fallback()

    try:
        return callable_fn()
    except Exception as exc:
        _mark_redis_unavailable(exc)
        return fallback()

CACHE_TTL = 60 * 30
STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "pie:transactions")
CONSUMER_GROUP = os.getenv("REDIS_CONSUMER_GROUP", "pie-prediction-engine")
CONSUMER_NAME = os.getenv("REDIS_CONSUMER_NAME", "pie-worker-1")

SEED_FLAG_KEY = "pie:seed:complete"
PROFILE_LIST_KEY = "pie:customers:list"


def _loads(value: Any):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def get_cached_score(customer_id: str):
    key = f"risk:{customer_id}"
    return _redis_call(lambda: _loads(redis.get(key)), lambda: _local_kv_store.get(key))


def set_cached_score(customer_id: str, score_data: dict):
    key = f"risk:{customer_id}"
    _local_kv_store[key] = score_data
    _redis_call(lambda: redis.setex(key, CACHE_TTL, json.dumps(score_data)), lambda: None)


def get_customer_transactions(customer_id: str):
    key = f"customer:{customer_id}:transactions"
    return _redis_call(lambda: _loads(redis.get(key)) or [], lambda: _local_kv_store.get(key, []))


def set_customer_transactions(customer_id: str, transactions: list[dict]):
    key = f"customer:{customer_id}:transactions"
    value = transactions[-300:]
    _local_kv_store[key] = value
    _redis_call(lambda: redis.set(key, json.dumps(value)), lambda: None)


def append_customer_transaction(customer_id: str, transaction: dict):
    history = get_customer_transactions(customer_id)
    history.append(transaction)
    set_customer_transactions(customer_id, history)
    return history


def get_customer_profile(customer_id: str):
    key = f"customer:{customer_id}:profile"
    return _redis_call(lambda: _loads(redis.get(key)), lambda: _local_kv_store.get(key))


def set_customer_profile(profile: dict):
    key = f"customer:{profile['customer_id']}:profile"
    _local_kv_store[key] = profile
    _redis_call(lambda: redis.set(key, json.dumps(profile)), lambda: None)


def set_customer_profile_list(profiles: list[dict]):
    _local_kv_store[PROFILE_LIST_KEY] = profiles
    _redis_call(lambda: redis.set(PROFILE_LIST_KEY, json.dumps(profiles)), lambda: None)


def get_customer_profile_list():
    return _redis_call(lambda: _loads(redis.get(PROFILE_LIST_KEY)) or [], lambda: _local_kv_store.get(PROFILE_LIST_KEY, []))


def is_seed_complete() -> bool:
    return bool(_redis_call(lambda: redis.get(SEED_FLAG_KEY), lambda: _local_kv_store.get(SEED_FLAG_KEY)))


def mark_seed_complete():
    _local_kv_store[SEED_FLAG_KEY] = "1"
    _redis_call(lambda: redis.set(SEED_FLAG_KEY, "1"), lambda: None)


def get_cached_transaction_count(customer_id: str) -> int:
    return len(get_customer_transactions(customer_id))


def stream_publish(payload: dict, *, maxlen: int | None = None) -> str:
    local_id = f"local-{int(time.time() * 1000)}"
    if maxlen and maxlen > 0:
        return _redis_call(
            lambda: redis.execute(["XADD", STREAM_KEY, "MAXLEN", "~", str(maxlen), "*", "data", json.dumps(payload)]),
            lambda: local_id,
        )
    return _redis_call(
        lambda: redis.execute(["XADD", STREAM_KEY, "*", "data", json.dumps(payload)]),
        lambda: local_id,
    )


def publish_live_score(message: dict):
    payload = json.dumps(message)
    _redis_call(lambda: redis.publish("pie:live_scores", payload), lambda: None)


def stream_create_group():
    global _redis_available
    if not _redis_available:
        return

    try:
        redis.execute(["XGROUP", "CREATE", STREAM_KEY, CONSUMER_GROUP, "0", "MKSTREAM"])
        print(f"Consumer group '{CONSUMER_GROUP}' created")
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists")
            return
        _mark_redis_unavailable(exc)


def ping_redis() -> bool:
    return bool(_redis_call(lambda: redis.ping(), lambda: False))


def set_hash_fields(key: str, fields: dict[str, Any]) -> None:
    encoded_fields = {name: (json.dumps(value) if isinstance(value, (dict, list)) else str(value)) for name, value in fields.items()}

    def _remote_call():
        command = ["HSET", key]
        for field_name, field_value in encoded_fields.items():
            command.extend([field_name, field_value])
        redis.execute(command)

    _local_kv_store[key] = dict(encoded_fields)
    _redis_call(_remote_call, lambda: None)


def get_hash_fields(key: str) -> dict[str, Any]:
    def _fallback():
        return _local_kv_store.get(key, {})

    payload = _redis_call(lambda: redis.execute(["HGETALL", key]), _fallback)
    if not payload:
        return {}
    if isinstance(payload, dict):
        return payload

    result: dict[str, Any] = {}
    if isinstance(payload, list):
        it = iter(payload)
        for item_key, item_value in zip(it, it):
            result[str(item_key)] = item_value
    return result


def append_stream_metric(metric_payload: dict, *, key: str = "pie:stream:metrics", maxlen: int = 10000) -> str:
    local_id = f"local-{int(time.time() * 1000)}"
    return _redis_call(
        lambda: redis.execute(["XADD", key, "MAXLEN", "~", str(maxlen), "*", "data", json.dumps(metric_payload)]),
        lambda: local_id,
    )


def _parse_stream_event_list(result: list) -> list[tuple[str, dict]]:
    """Parse raw XREAD/XREADGROUP result into (event_id, event_data) tuples."""
    events: list[tuple[str, dict]] = []
    if not (isinstance(result, list) and result):
        return events
    stream_data = result[0]
    if not (isinstance(stream_data, (list, tuple)) and len(stream_data) > 1):
        return events
    event_list = stream_data[1]
    for event_item in (event_list or []):
        if not (isinstance(event_item, (list, tuple)) and len(event_item) >= 2):
            continue
        event_id = str(event_item[0])
        fields = event_item[1]
        event_data: dict = {}
        if isinstance(fields, list):
            it = iter(fields)
            for field_name, field_value in zip(it, it):
                # Upstash REST returns strings (not bytes) — no .decode() needed
                key = str(field_name)
                val = str(field_value) if not isinstance(field_value, str) else field_value
                try:
                    event_data[key] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    event_data[key] = val
        elif isinstance(fields, dict):
            event_data = fields
        events.append((event_id, event_data))
    return events


def stream_read(stream_key: str, start_id: str = "0", count: int = 10, block_ms: int = 1000) -> list[tuple[str, dict]]:
    """
    Read from a Redis stream using plain XREAD (no consumer group).
    Prefer stream_read_group() for production consumers.
    """
    def _remote_call():
        result = redis.execute(["XREAD", "COUNT", str(count), "BLOCK", str(block_ms), "STREAMS", stream_key, start_id])
        return _parse_stream_event_list(result or [])
    return _redis_call(_remote_call, lambda: [])


def stream_read_group(
    stream_key: str,
    group: str,
    consumer: str,
    count: int = 20,
    block_ms: int = 2000,
    start_id: str = ">",
) -> list[tuple[str, dict]]:
    """
    Read from a Redis stream using XREADGROUP semantics.
    
    start_id=">" delivers only new (undelivered) messages.
    start_id="0" re-delivers pending (delivered but not ACKed) messages.
    """
    def _remote_call():
        result = redis.execute([
            "XREADGROUP", "GROUP", group, consumer,
            "COUNT", str(count),
            "BLOCK", str(block_ms),
            "STREAMS", stream_key, start_id,
        ])
        return _parse_stream_event_list(result or [])
    return _redis_call(_remote_call, lambda: [])


def stream_ack(stream_key: str, group: str, *event_ids: str) -> int:
    """Acknowledge one or more events in a consumer group (XACK)."""
    if not event_ids:
        return 0
    def _remote_call():
        return int(redis.execute(["XACK", stream_key, group, *event_ids]) or 0)
    return _redis_call(_remote_call, lambda: 0)


def get_stream_length(stream_key: str | None = None) -> int:
    """Return the total number of entries in the stream (XLEN)."""
    key = stream_key or STREAM_KEY
    return int(_redis_call(lambda: redis.execute(["XLEN", key]) or 0, lambda: 0))


# ------------------------------------------------------------------
# Per-customer stream-event counter (avoids COUNT(*) on every event)
# ------------------------------------------------------------------
_CUSTOMER_STREAM_COUNTER_KEY = "pie:customer:stream_count"


def increment_customer_stream_count(customer_id: str) -> int:
    """Atomically increment and return the streamed-event count for a customer."""
    field = customer_id
    local_key = f"stream_count:{customer_id}"

    def _remote_call() -> int:
        return int(redis.execute(["HINCRBY", _CUSTOMER_STREAM_COUNTER_KEY, field, "1"]) or 1)

    def _fallback() -> int:
        current = int(_local_kv_store.get(local_key, 0))
        current += 1
        _local_kv_store[local_key] = current
        return current

    return _redis_call(_remote_call, _fallback)


def get_customer_stream_count(customer_id: str) -> int:
    """Return the current streamed-event count for a customer."""
    field = customer_id
    local_key = f"stream_count:{customer_id}"

    def _remote_call() -> int:
        return int(redis.execute(["HGET", _CUSTOMER_STREAM_COUNTER_KEY, field]) or 0)

    return _redis_call(_remote_call, lambda: int(_local_kv_store.get(local_key, 0)))


def get_stream_pending_count(stream_key: str | None = None, group: str | None = None) -> int:
    """Return the number of delivered-but-not-ACKed messages for the consumer group."""
    key = stream_key or STREAM_KEY
    group_name = group or CONSUMER_GROUP
    payload = _redis_call(lambda: redis.execute(["XPENDING", key, group_name]), lambda: None)
    if not payload:
        return 0
    if isinstance(payload, list) and payload:
        try:
            return int(payload[0])
        except Exception:
            return 0
    if isinstance(payload, dict):
        try:
            return int(payload.get("pending", 0))
        except Exception:
            return 0
    return 0
