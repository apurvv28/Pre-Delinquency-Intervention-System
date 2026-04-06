import json
import os
import random
import threading
import time
from datetime import datetime, timezone

from sqlalchemy import func

from backend.cache import (
    append_customer_transaction,
    get_customer_profile_list,
    get_customer_transactions,
    publish_live_score,
    set_cached_score,
    set_customer_profile,
    set_customer_profile_list,
)
from backend.database import CustomerProfile, CustomerTransaction, RiskScore, SessionLocal
from backend.intervention_system import orchestrate_from_latest_scores
from backend.timezone_util import get_ist_now
from backend.predict import predict_risk
from backend.seed import build_transaction_record

STREAM_INTERVAL_MIN_SECONDS = float(os.getenv("TRANSACTION_STREAM_MIN_SECONDS", "5"))
STREAM_INTERVAL_MAX_SECONDS = float(os.getenv("TRANSACTION_STREAM_MAX_SECONDS", "10"))
INTERVENTION_TRIGGER_MIN_TRANSACTIONS = int(os.getenv("INTERVENTION_TRIGGER_MIN_TRANSACTIONS", "50"))
INTERVENTION_TRIGGER_MAX_TRANSACTIONS = int(os.getenv("INTERVENTION_TRIGGER_MAX_TRANSACTIONS", "60"))


def _clamp_stream_interval_bounds() -> tuple[float, float]:
    min_seconds = max(0.5, STREAM_INTERVAL_MIN_SECONDS)
    max_seconds = max(min_seconds, STREAM_INTERVAL_MAX_SECONDS)
    return min_seconds, max_seconds


def _next_intervention_target() -> int:
    lower = max(1, INTERVENTION_TRIGGER_MIN_TRANSACTIONS)
    upper = max(lower, INTERVENTION_TRIGGER_MAX_TRANSACTIONS)
    return random.randint(lower, upper)


def _load_profiles_for_stream() -> list[dict]:
    cached_profiles = get_customer_profile_list()
    if cached_profiles:
        return cached_profiles

    with SessionLocal() as db:
        rows = db.query(CustomerProfile).order_by(CustomerProfile.customer_id.asc()).all()

    profiles = [
        {
            "customer_id": row.customer_id,
            "name": row.name,
            "branch": row.branch,
            "loan_type": row.loan_type,
            "risk_segment": row.risk_segment,
            "monthly_income": float(row.monthly_income or 0),
            "loan_amount": float(row.loan_amount or 0),
            "occupation": row.occupation,
            "spending_culture": row.spending_culture,
            "email": row.email,
            "rm_email": row.rm_email,
            "rm_phone": row.rm_phone,
            "branch_address": row.branch_address,
            "intervention_status": row.intervention_status,
            "pre_npa": bool(row.pre_npa),
            "account_age_months": int(row.account_age_months or 0),
            "relationship_manager": row.relationship_manager,
        }
        for row in rows
    ]

    if profiles:
        set_customer_profile_list(profiles)
        for profile in profiles:
            set_customer_profile(profile)

    return profiles


def build_history_context(history: list[dict]) -> dict:
    if not history:
        return {"avg_amount": 0.0, "avg_dpd": 0.0, "avg_balance": 0.0, "trend": 0.0}

    recent = history[-20:]
    amounts = [float(item["amount"]) for item in recent]
    balances = [float(item["balance_after"]) for item in recent]
    dpds = [float(item["days_since_last_payment"]) for item in recent]
    first_amount = amounts[0]
    last_amount = amounts[-1]

    return {
        "avg_amount": sum(amounts) / len(amounts),
        "avg_dpd": sum(dpds) / len(dpds),
        "avg_balance": sum(balances) / len(balances),
        "trend": (last_amount - first_amount) / max(first_amount, 1.0),
    }


def _persist_prediction(db, customer_id: str, transaction: dict, prediction: dict):
    db.add(
        RiskScore(
            customer_id=customer_id,
            risk_score=prediction["risk_score"],
            risk_bucket=prediction["risk_bucket"],
        )
    )

    stored = db.query(CustomerTransaction).filter(
        CustomerTransaction.customer_id == customer_id,
        CustomerTransaction.transaction_index == transaction["transaction_index"],
    ).first()
    if stored:
        stored.risk_score = prediction["risk_score"]
        stored.risk_bucket = prediction["risk_bucket"]

    db.commit()

    set_cached_score(
        customer_id,
        {
            "customer_id": customer_id,
            "risk_score": prediction["risk_score"],
            "risk_bucket": prediction["risk_bucket"],
            "base_model_risk_score": prediction.get("base_model_risk_score"),
            "base_model_risk_bucket": prediction.get("base_model_risk_bucket"),
            "context_model_risk_score": prediction.get("context_model_risk_score"),
            "context_model_risk_bucket": prediction.get("context_model_risk_bucket"),
            "final_model_risk_score": prediction.get("final_model_risk_score", prediction["risk_score"]),
            "final_model_risk_bucket": prediction.get("final_model_risk_bucket", prediction["risk_bucket"]),
            "pipeline_stage": prediction.get("pipeline_stage", "ingest->lightgbm->xgboost->final"),
            "timestamp": get_ist_now().isoformat(),
        },
    )

    publish_live_score(
        {
            "customer_id": customer_id,
            "risk_score": prediction["risk_score"],
            "risk_bucket": prediction["risk_bucket"],
            "base_model_risk_score": prediction.get("base_model_risk_score"),
            "context_model_risk_score": prediction.get("context_model_risk_score"),
            "final_model_risk_score": prediction.get("final_model_risk_score", prediction["risk_score"]),
            "pipeline_stage": prediction.get("pipeline_stage", "ingest->lightgbm->xgboost->final"),
            "created_at": get_ist_now().isoformat(),
        }
    )


def stream_customer_transactions(stop_event: threading.Event | None = None):
    profiles = _load_profiles_for_stream()
    if not profiles:
        return

    min_interval, max_interval = _clamp_stream_interval_bounds()
    transactions_since_trigger = 0
    next_trigger_target = _next_intervention_target()

    while True:
        if stop_event and stop_event.is_set():
            break

        with SessionLocal() as db:
            profile = random.choice(profiles)
            previous_history = get_customer_transactions(profile["customer_id"])
            max_index = (
                db.query(func.max(CustomerTransaction.transaction_index))
                .filter(CustomerTransaction.customer_id == profile["customer_id"])
                .scalar()
                or 0
            )
            transaction_index = int(max_index) + 1
            rng_seed = time.time_ns() ^ hash(profile["customer_id"]) ^ transaction_index
            rng = random.Random(rng_seed)
            transaction = build_transaction_record(profile, transaction_index, rng, False, previous_history)
            append_customer_transaction(profile["customer_id"], transaction)

            db.add(
                CustomerTransaction(
                    customer_id=profile["customer_id"],
                    transaction_index=transaction_index,
                    amount=transaction["amount"],
                    balance_after=transaction["balance_after"],
                    days_since_last_payment=transaction["days_since_last_payment"],
                    previous_declines_24h=transaction["previous_declines_24h"],
                    merchant_category=transaction["merchant_category"],
                    is_international=transaction["is_international"],
                    transaction_time=datetime.fromisoformat(transaction["transaction_time"]),
                    risk_score=None,
                    risk_bucket=None,
                    is_seeded=False,
                    raw_json=json.dumps(transaction["raw_json"]),
                )
            )
            db.commit()

            model_payload = {**transaction["raw_json"], "customer_id": profile["customer_id"]}
            prediction = predict_risk(model_payload, history_context=build_history_context(previous_history))
            _persist_prediction(db, profile["customer_id"], transaction, prediction)

            transactions_since_trigger += 1
            if transactions_since_trigger >= next_trigger_target:
                result = orchestrate_from_latest_scores(db, actor="stream-auto-trigger")
                print(
                    "Intervention orchestrator auto-triggered after",
                    transactions_since_trigger,
                    "transactions ->",
                    result,
                )
                transactions_since_trigger = 0
                next_trigger_target = _next_intervention_target()

        time.sleep(random.uniform(min_interval, max_interval))

