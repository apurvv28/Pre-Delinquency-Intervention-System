import json
import time
import uuid
import sys
import os
import datetime

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from src.cache.redis_client import (
    stream_create_group,
    stream_read,
    stream_ack,
    set_cached_score,
    redis
)
from src.api.predict import predict_risk
from src.api.database import SessionLocal, RiskScore, Intervention
from src.api.intervention import execute_intervention

def get_intervention_type(bucket: str) -> str:
    mapping = {
        "LOW_RISK":      "MONITOR",
        "HIGH_RISK":     "SMS_REMINDER",
        "CRITICAL":      "PAYMENT_PLAN",
        "VERY_CRITICAL": "HUMAN_ESCALATION"
    }
    return mapping.get(bucket, "UNKNOWN")

def process_message(msg_id: str, fields: list):
    """Process a single stream message."""
    try:
        # Convert list of ['key1', 'val1', 'key2', 'val2'] to dict
        it = iter(fields)
        fields_dict = dict(zip(it, it))
        
        payload = json.loads(fields_dict.get("data", "{}"))
        customer_id = payload.get("customer_id")
        features    = payload.get("features", {})

        if not customer_id:
            print(f"Skipping message {msg_id} - no customer_id")
            stream_ack(msg_id)
            return

        print(f"Processing: {customer_id}")

        # The prediction is already done before streaming, so we just extract it from the payload
        risk_score = payload.get("risk_score")
        risk_bucket = payload.get("risk_bucket", "UNKNOWN")
        intervention_rec = payload.get("intervention_recommended", "")

        # Fallback in case old messages are still in stream without prediction
        if risk_score is None:
            prediction = predict_risk(features)
            risk_score = prediction["risk_score"]
            risk_bucket = prediction["risk_bucket"]
            intervention_rec = prediction.get("intervention_recommended", "")

        result = {
            "customer_id": customer_id,
            "risk_score": risk_score,
            "risk_bucket": risk_bucket,
            "intervention_recommended": intervention_rec
        }

        # Update cache
        set_cached_score(customer_id, result)
        print(f"Cached: {customer_id} | score={result['risk_score']} | bucket={result['risk_bucket']}")

        # Write to SQLite
        try:
            db = SessionLocal()
            score_row = RiskScore(
                id=str(uuid.uuid4()),
                customer_id=customer_id,
                risk_score=result["risk_score"],
                risk_bucket=result["risk_bucket"]
            )
            db.add(score_row)

            intervention_row = Intervention(
                id=str(uuid.uuid4()),
                customer_id=customer_id,
                risk_bucket=result["risk_bucket"],
                intervention_type=get_intervention_type(result["risk_bucket"]),
                message=result.get("intervention_recommended", ""),
                status="PENDING"
            )
            db.add(intervention_row)
            db.commit()

            # Publish to websocket channel
            live_message = {
                "customer_id": customer_id,
                "risk_score": result["risk_score"],
                "risk_bucket": result["risk_bucket"],
                "created_at": datetime.datetime.utcnow().isoformat(),
                "intervention_recommended": result.get("intervention_recommended", "")
            }
            redis.publish("pie:live_scores", json.dumps(live_message))
            
            # Fire off our intervention engine routine for streamed messages
            execute_intervention(customer_id, result["risk_bucket"], features)

        except Exception as db_e:
            print(f"DB insert error in consumer: {db_e}")
            db.rollback()
        finally:
            db.close()

        # Acknowledge the message since it's fully processed
        stream_ack(msg_id)

    except Exception as e:
        print(f"Message processing error [{msg_id}]: {e}")

def start_consumer(poll_interval: float = 2.0):
    """Start consuming messages from Redis Stream."""
    print("Initializing Redis Stream consumer...")
    stream_create_group()
    print(f"Listening on stream: pie:transactions")
    print(f"Consumer group: pie-consumers")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            messages = stream_read(count=10)

            if not messages:
                time.sleep(poll_interval)
                continue

            for stream_name, stream_messages in messages:
                for msg_id, fields in stream_messages:
                    process_message(msg_id, fields)

        except KeyboardInterrupt:
            print("\nConsumer stopped.")
            break
        except Exception as e:
            print(f"Consumer loop error: {e}")
            time.sleep(poll_interval)

if __name__ == "__main__":
    start_consumer()
