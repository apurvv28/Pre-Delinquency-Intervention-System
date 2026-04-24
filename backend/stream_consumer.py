"""
PIE Stream Consumer
-------------------
Reads from the pie:transactions Redis stream via XREADGROUP so that:
  • Each event is delivered to exactly one consumer in the group.
  • Events are ACKed (XACK) only after successful DB persistence.
  • On restart the consumer re-claims its pending (delivered-but-not-ACKed)
    messages before moving to new ones, preventing data loss.
  • The per-customer stream-event counter lives in Redis (HINCRBY), avoiding
    an expensive COUNT(*) scan on every event (BUG-19).
  • WebSocket broadcasts push real-time updates to connected clients.
"""

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone

from backend.cache import (
    append_customer_transaction,
    increment_customer_stream_count,
    set_cached_score,
    stream_ack,
    stream_read_group,
)
from backend.database import CustomerTransaction, RiskScore, SessionLocal
from backend.predict import predict_risk
from backend.timezone_util import get_ist_now
from backend.websocket_manager import manager
from backend.intervention_system import auto_escalate_critical_customer

STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "pie:transactions")
STREAM_CONSUMER_GROUP = os.getenv("REDIS_CONSUMER_GROUP", "pie-prediction-engine")
STREAM_CONSUMER_NAME = f"consumer-{os.getenv('HOSTNAME', 'default')}-{os.getpid()}"
WS_BROADCAST_MODEL_OUTPUT = os.getenv("WS_BROADCAST_MODEL_OUTPUT", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Refresh the history-enriched risk score every N streamed transactions per customer.
RISK_REFRESH_EVERY_N = int(os.getenv("RISK_REFRESH_EVERY_N", "10"))

consumer_thread: threading.Thread | None = None
consumer_running: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_async_broadcast(coro) -> None:
    # Consumer runs in a background thread; schedule websocket sends on the
    # main server loop where WebSocket connections were accepted.
    if manager.loop and manager.loop.is_running():
        asyncio.run_coroutine_threadsafe(coro, manager.loop)
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
    else:
        loop.create_task(coro)

def _merchant_category_for_event_type(event_type: str) -> str:
    return {
        "PAYMENT": "Utilities",
        "MISSED_PAYMENT": "Cash Advance",
        "PARTIAL_PAYMENT": "Online Shopping",
        "LOAN_INQUIRY": "Travel",
        "CREDIT_UTILIZATION_UPDATE": "Electronics",
        "INCOME_CREDIT": "Salary",
        "PENALTY_APPLIED": "Penalty",
        "LOAN_OPENED": "Finance",
        "LOAN_CLOSED": "Finance",
        "ADDRESS_CHANGE": "Unknown",
        "SETTLEMENT_OFFER": "Settlement",
        "LEGAL_NOTICE_SENT": "Legal",
    }.get(event_type or "", "Unknown")


def _history_context_from_transactions(rows: list[CustomerTransaction]) -> dict:
    if not rows:
        return {
            "avg_amount": 0.0,
            "avg_dpd": 0.0,
            "avg_balance": 0.0,
            "avg_risk_score": 0.0,
            "latest_balance": 0.0,
            "latest_dpd": 0.0,
            "latest_risk_score": 0.0,
            "trend": 0.0,
        }

    recent = list(reversed(rows[-20:]))
    amounts = [float(item.amount) for item in recent]
    balances = [float(item.balance_after) for item in recent]
    dpds = [float(item.days_since_last_payment) for item in recent]
    risk_scores = [float(item.risk_score) for item in recent if item.risk_score is not None]

    return {
        "avg_amount": sum(amounts) / len(amounts),
        "avg_dpd": sum(dpds) / len(dpds),
        "avg_balance": sum(balances) / len(balances),
        "avg_risk_score": (sum(risk_scores) / len(risk_scores)) if risk_scores else 0.0,
        "latest_balance": balances[-1],
        "latest_dpd": dpds[-1],
        "latest_risk_score": risk_scores[-1] if risk_scores else 0.0,
        "trend": (amounts[-1] - amounts[0]) / max(amounts[0], 1.0),
    }


def _refresh_customer_risk_score(db, customer_id: str) -> None:
    """Re-score a customer with full history context and persist to DB + Redis cache."""
    recent_rows = (
        db.query(CustomerTransaction)
        .filter(CustomerTransaction.customer_id == customer_id)
        .order_by(CustomerTransaction.transaction_time.desc())
        .limit(20)
        .all()
    )
    if not recent_rows:
        return

    latest_row = recent_rows[0]
    history_context = _history_context_from_transactions(recent_rows)
    payload = {
        "customer_id": customer_id,
        "amount": float(latest_row.amount or 0),
        "current_balance": float(latest_row.balance_after or 0),
        "days_since_last_payment": int(latest_row.days_since_last_payment or 0),
        "previous_declines_24h": int(latest_row.previous_declines_24h or 0),
        "merchant_category": latest_row.merchant_category or "Unknown",
        "is_international": "true" if latest_row.is_international else "false",
    }

    # Preserve producer-side rich features (event_type, utilization, DTI, etc.)
    # when recalculating score so refresh stays aligned with the two-model pipeline.
    try:
        raw_payload = json.loads(latest_row.raw_json or "{}")
        if isinstance(raw_payload, dict):
            for key in (
                "event_type",
                "credit_utilization",
                "debt_to_income",
                "payment_streak",
                "missed_payment_count",
                "num_active_loans",
                "monthly_income",
                "days_past_due",
            ):
                if key in raw_payload and raw_payload.get(key) is not None:
                    payload[key] = raw_payload.get(key)
    except Exception:
        pass

    prediction = predict_risk(payload, history_context=history_context)
    score = float(prediction["risk_score"])
    bucket = str(prediction["risk_bucket"])

    db.add(RiskScore(customer_id=customer_id, risk_score=score, risk_bucket=bucket))
    db.commit()

    set_cached_score(
        customer_id,
        {
            "customer_id": customer_id,
            "risk_score": score,
            "risk_bucket": bucket,
            "cached": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )

    print(
        f"[RISK_REFRESH] Customer: {customer_id} | Tx Count: {len(recent_rows)} | "
        f"Score: {score:.2f} | Bucket: {bucket}"
    )

    # Auto-escalate via email if post-refresh score is >= 80%
    if score >= 80.0:
        try:
            escalation_result = auto_escalate_critical_customer(
                customer_id=customer_id,
                risk_score=score,
                actor="stream_auto_escalation",
            )
            if escalation_result:
                print(
                    f"[AUTO_ESCALATE] Triggered for {customer_id} | "
                    f"Score: {score:.2f} | Result: {escalation_result.get('send_result', {}).get('status', 'N/A')}"
                )
        except Exception as escalation_err:
            print(f"[AUTO_ESCALATE] Error for {customer_id}: {escalation_err}")


# ---------------------------------------------------------------------------
# Event processor
# ---------------------------------------------------------------------------

def _process_transaction_event(event_id: str, event_data: dict) -> None:
    """Persist a single stream event to the database.  Always uses a finally
    block to close the DB session so that an early exception never leaks a
    connection (BUG-04 fix).
    """
    db = None
    try:
        # --- Unwrap the "data" envelope written by stream_publish() (BUG-02) ---
        if "data" in event_data:
            payload_src = event_data["data"]
            if isinstance(payload_src, str):
                try:
                    event_data = json.loads(payload_src)
                except json.JSONDecodeError:
                    print(f"[STREAM_CONSUMER] ERROR: Event {event_id} has invalid JSON in 'data' field — skipping")
                    return
            elif isinstance(payload_src, dict):
                event_data = payload_src
            else:
                event_data = {}

        # --- Determine event shape ---
        if "transaction" in event_data and isinstance(event_data.get("transaction"), dict):
            # Shape 1: manual prediction with nested transaction dict
            transaction_data = event_data["transaction"]
            customer_id = event_data.get("customer_id")
            amount = float(transaction_data.get("amount", 0) or 0)
            balance_after = float(transaction_data.get("balance_after", 0) or 0)
            days_since_last_payment = int(transaction_data.get("days_since_last_payment", 0) or 0)
            previous_declines_24h = int(transaction_data.get("previous_declines_24h", 0) or 0)
            merchant_category = str(transaction_data.get("merchant_category", "API_PREDICTION"))
            is_international = str(transaction_data.get("is_international", "false")).lower() == "true"
            tx_time_str = transaction_data.get("transaction_time")
            risk_score = float((event_data.get("prediction") or {}).get("risk_score", 0) or 0)
            risk_bucket = str((event_data.get("prediction") or {}).get("risk_bucket", "UNKNOWN"))
        else:
            # Shape 2: stream-producer event with top-level fields
            customer_id = event_data.get("customer_id")
            amount = float(event_data.get("amount", 0) or 0)
            balance_after = float(
                event_data.get("balance_after")
                or event_data.get("outstanding_balance")
                or event_data.get("current_balance", 0)
                or 0
            )
            days_since_last_payment = int(
                event_data.get("days_since_last_payment")
                or event_data.get("days_past_due", 0)
                or 0
            )
            previous_declines_24h = int(
                event_data.get("previous_declines_24h")
                or event_data.get("missed_payment_count", 0)
                or 0
            )
            merchant_category = str(
                event_data.get("merchant_category")
                or _merchant_category_for_event_type(str(event_data.get("event_type", "")))
            )
            is_international = str(event_data.get("is_international", "false")).lower() == "true"
            tx_time_str = event_data.get("timestamp")
            risk_score = float(event_data.get("risk_score", 0) or 0)
            risk_bucket = str(event_data.get("risk_bucket") or "UNKNOWN")

        if not customer_id:
            print(f"[STREAM_CONSUMER] ERROR: Event {event_id} missing customer_id — skipping")
            return

        # --- Parse timestamp ---
        try:
            tx_time = datetime.fromisoformat(str(tx_time_str).replace("Z", "+00:00")) if tx_time_str else datetime.now(timezone.utc)
        except Exception:
            tx_time = datetime.now(timezone.utc)

        db = SessionLocal()

        # --- Compute next transaction index (BUG-03 fix: guard `is not None`) ---
        max_index_row = (
            db.query(CustomerTransaction.transaction_index)
            .filter(CustomerTransaction.customer_id == customer_id)
            .order_by(CustomerTransaction.transaction_index.desc())
            .first()
        )
        next_index = (max_index_row[0] + 1) if (max_index_row and max_index_row[0] is not None) else 1

        # --- Persist transaction ---
        transaction = CustomerTransaction(
            customer_id=customer_id,
            transaction_index=next_index,
            amount=amount,
            balance_after=balance_after,
            days_since_last_payment=days_since_last_payment,
            previous_declines_24h=previous_declines_24h,
            merchant_category=merchant_category,
            is_international=is_international,
            transaction_time=tx_time,
            risk_score=risk_score,
            risk_bucket=risk_bucket,
            is_seeded=False,
            raw_json=json.dumps(event_data),
        )
        db.add(transaction)
        # Keep dashboard registry aligned with stream events by recording each
        # event's model score as an official RiskScore observation.
        db.add(RiskScore(customer_id=customer_id, risk_score=risk_score, risk_bucket=risk_bucket))
        db.commit()

        # --- Update Redis cache list ---
        append_customer_transaction(
            customer_id,
            {
                "customer_id": customer_id,
                "transaction_index": next_index,
                "amount": amount,
                "balance_after": balance_after,
                "days_since_last_payment": days_since_last_payment,
                "previous_declines_24h": previous_declines_24h,
                "merchant_category": merchant_category,
                "is_international": is_international,
                "transaction_time": tx_time.isoformat(),
                "seeded": False,
                "risk_score": risk_score,
                "risk_bucket": risk_bucket,
                "raw_json": event_data,
            },
        )

        # --- Increment Redis counter (BUG-19 fix) and conditionally refresh risk score ---
        streamed_count = increment_customer_stream_count(customer_id)
        if streamed_count % RISK_REFRESH_EVERY_N == 0:
            _refresh_customer_risk_score(db, customer_id)

        # --- Broadcast to WebSocket clients for real-time updates ---
        # Run async broadcast in a thread since we're in a sync context
        try:
            # Prepare broadcast data
            tx_broadcast = {
                "customer_id": customer_id,
                "transaction_index": next_index,
                "amount": amount,
                "balance_after": balance_after,
                "days_since_last_payment": days_since_last_payment,
                "merchant_category": merchant_category,
                "is_international": is_international,
                "transaction_time": tx_time.isoformat(),
                "risk_score": risk_score,
                "risk_bucket": risk_bucket,
            }
            
            score_update = {
                "customer_id": customer_id,
                "risk_score": risk_score,
                "risk_bucket": risk_bucket,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            # Schedule async broadcasts
            _run_async_broadcast(manager.broadcast_transaction(tx_broadcast))
            _run_async_broadcast(manager.broadcast_score_update(customer_id, risk_score, risk_bucket))
            if WS_BROADCAST_MODEL_OUTPUT:
                _run_async_broadcast(manager.broadcast_model_output("transaction_processed", {
                    "customer_id": customer_id,
                    "transaction_index": next_index,
                    "fusion_score": risk_score,
                    "fusion_bucket": risk_bucket,
                }))
        except Exception as ws_err:
            print(f"[STREAM_CONSUMER] WebSocket broadcast error: {ws_err}")

        print(
            f"[STREAM_CONSUMER] OK | Event: {event_id} | Customer: {customer_id} | "
            f"₹{amount:.2f} | {risk_bucket} | idx={next_index} | n={streamed_count}"
        )

    except Exception as exc:
        print(f"[STREAM_CONSUMER] ERROR processing event {event_id}: {exc}")
    finally:
        # BUG-04 fix: always close db if it was successfully opened
        if db is not None:
            db.close()


# ---------------------------------------------------------------------------
# Consumer loop — XREADGROUP + XACK  (BUG-01 / MISSING-01 fix)
# ---------------------------------------------------------------------------

def _consumer_loop() -> None:
    """Main consumer loop.

    On startup:
      1. First drain any pending (delivered but not ACKed) messages that this
         consumer held before a previous crash (start_id="0").
      2. Then switch to start_id=">" to deliver only new messages.

    After each successful process batch, send XACK so those message IDs leave
    the PEL (Pending Entry List) and the stream can compact them.
    """
    global consumer_running

    print(
        f"[STREAM_CONSUMER] Started | Stream: {STREAM_KEY} | "
        f"Group: {STREAM_CONSUMER_GROUP} | Consumer: {STREAM_CONSUMER_NAME}"
    )

    # Step 1: re-claim any messages pending from a previous crash
    _drain_pending_messages()

    # Step 2: continuous delivery of new messages
    while consumer_running:
        try:
            events = stream_read_group(
                stream_key=STREAM_KEY,
                group=STREAM_CONSUMER_GROUP,
                consumer=STREAM_CONSUMER_NAME,
                count=20,
                block_ms=2000,
                start_id=">",  # only undelivered messages
            )

            if not events:
                continue

            acked_ids: list[str] = []
            for event_id, event_data in events:
                try:
                    _process_transaction_event(event_id, event_data)
                    acked_ids.append(event_id)
                except Exception as exc:
                    print(f"[STREAM_CONSUMER] Skipping event {event_id} after error: {exc}")
                    # Still ACK to avoid infinite retry loops on bad messages
                    acked_ids.append(event_id)

            if acked_ids:
                n_acked = stream_ack(STREAM_KEY, STREAM_CONSUMER_GROUP, *acked_ids)
                if n_acked != len(acked_ids):
                    print(f"[STREAM_CONSUMER] WARNING: ACKed {n_acked}/{len(acked_ids)} events")

        except Exception as exc:
            print(f"[STREAM_CONSUMER] Loop error: {exc}")
            time.sleep(1)


def _drain_pending_messages() -> None:
    """Re-processes messages that were delivered to this consumer but never ACKed
    (e.g. due to a crash).  Runs once on startup with start_id='0'.
    """
    print("[STREAM_CONSUMER] Draining pending messages from previous session...")
    recovered = 0
    while consumer_running:
        try:
            events = stream_read_group(
                stream_key=STREAM_KEY,
                group=STREAM_CONSUMER_GROUP,
                consumer=STREAM_CONSUMER_NAME,
                count=50,
                block_ms=500,
                start_id="0",  # pending messages for this consumer
            )
            if not events:
                break  # no more pending messages

            acked_ids: list[str] = []
            for event_id, event_data in events:
                try:
                    _process_transaction_event(event_id, event_data)
                except Exception as exc:
                    print(f"[STREAM_CONSUMER] Pending event {event_id} error: {exc}")
                acked_ids.append(event_id)
                recovered += 1

            if acked_ids:
                stream_ack(STREAM_KEY, STREAM_CONSUMER_GROUP, *acked_ids)
        except Exception as exc:
            print(f"[STREAM_CONSUMER] Error draining pending: {exc}")
            break

    if recovered:
        print(f"[STREAM_CONSUMER] Recovered {recovered} pending messages from previous session")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def start_stream_consumer() -> None:
    global consumer_thread, consumer_running

    if consumer_running:
        print("[STREAM_CONSUMER] Consumer already running")
        return

    consumer_running = True
    consumer_thread = threading.Thread(
        target=_consumer_loop,
        daemon=True,
        name="StreamConsumer",
    )
    consumer_thread.start()
    print("[STREAM_CONSUMER] Consumer thread started")


def stop_stream_consumer() -> None:
    global consumer_running, consumer_thread

    consumer_running = False
    if consumer_thread and consumer_thread.is_alive():
        consumer_thread.join(timeout=5)
    print("[STREAM_CONSUMER] Consumer stopped")
