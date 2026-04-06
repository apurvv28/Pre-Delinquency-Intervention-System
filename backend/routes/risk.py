import json
import os
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.cache import (
    append_customer_transaction,
    get_cached_score,
    get_customer_profile,
    get_customer_profile_list,
    get_customer_transactions,
    publish_live_score,
    set_cached_score,
    stream_publish,
)
from backend.database import CustomerProfile, CustomerTransaction, RiskScore, get_db
from backend.predict import predict_risk

router = APIRouter(prefix="/api/v1", tags=["risk"])
MAX_EXPLANATION_WORDS = 500


def _truncate_words(text: str, max_words: int = MAX_EXPLANATION_WORDS) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).rstrip() + "..."


def _history_context_from_transactions(rows: list[dict]) -> dict:
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
    recent = rows[-20:]
    amounts = [float(item["amount"]) for item in recent]
    balances = [float(item["balance_after"]) for item in recent]
    dpds = [float(item["days_since_last_payment"]) for item in recent]
    risk_scores = [float(item["risk_score"]) for item in recent if item.get("risk_score") is not None]
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

def _spending_summary(rows: list[CustomerTransaction]) -> tuple[list[str], float]:
    if not rows:
        return [], 0.0

    spend_by_reason: dict[str, float] = {}
    risk_scores: list[float] = []
    for row in rows:
        reason = row.merchant_category or "Unknown"
        spend_by_reason[reason] = spend_by_reason.get(reason, 0.0) + float(row.amount or 0)
        if row.risk_score is not None:
            risk_scores.append(float(row.risk_score))

    top_reasons = [item[0] for item in sorted(spend_by_reason.items(), key=lambda item: item[1], reverse=True)[:3]]
    avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
    return top_reasons, round(avg_risk, 2)


@router.post("/predict")
async def predict_endpoint(payload: dict, db: Session = Depends(get_db)):
    customer_id = payload.get("customer_id")
    features = dict(payload.get("features", {}))

    # Allow both nested and flat payloads from clients.
    if not features:
        features = {
            key: value
            for key, value in payload.items()
            if key not in {"customer_id", "features", "bypass_cache"}
        }

    if "merchant_category" not in features and payload.get("transaction_reason"):
        features["merchant_category"] = payload.get("transaction_reason")

    cached = get_cached_score(customer_id)
    if cached and not payload.get("bypass_cache", False):
        cached["cached"] = True
        cached["timestamp"] = datetime.now(timezone.utc).isoformat()
        return cached

    recent_history = get_customer_transactions(customer_id)
    max_index = (
        db.query(func.max(CustomerTransaction.transaction_index))
        .filter(CustomerTransaction.customer_id == customer_id)
        .scalar()
        or 0
    )
    next_transaction_index = int(max_index) + 1
    history_context = _history_context_from_transactions(recent_history)

    # Backfill model-required features with customer history context when absent.
    features["current_balance"] = float(features.get("current_balance") or history_context["latest_balance"] or history_context["avg_balance"] or 0)
    features["days_since_last_payment"] = int(features.get("days_since_last_payment") or history_context["latest_dpd"] or history_context["avg_dpd"] or 0)
    features["previous_declines_24h"] = int(features.get("previous_declines_24h") or 0)
    features["risk_score"] = float(features.get("risk_score") or history_context["latest_risk_score"] or history_context["avg_risk_score"] or 0)
    features["customer_id"] = customer_id

    prediction = predict_risk(features, history_context=history_context)

    db.add(RiskScore(customer_id=customer_id, risk_score=prediction["risk_score"], risk_bucket=prediction["risk_bucket"]))
    db.add(
        CustomerTransaction(
            customer_id=customer_id,
            transaction_index=next_transaction_index,
            amount=float(features.get("amount", 0) or 0),
            balance_after=float(features.get("current_balance", 0) or 0),
            days_since_last_payment=int(features.get("days_since_last_payment", 0) or 0),
            previous_declines_24h=int(features.get("previous_declines_24h", 0) or 0),
            merchant_category=str(features.get("merchant_category", "Unknown")),
            is_international=str(features.get("is_international", "false")).lower() == "true",
            transaction_time=datetime.now(timezone.utc),
            risk_score=prediction["risk_score"],
            risk_bucket=prediction["risk_bucket"],
            is_seeded=False,
            raw_json=json.dumps(features),
        )
    )
    db.commit()

    response = {
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
        "model_probability": prediction.get("probability"),
        "cached": False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    set_cached_score(customer_id, response)

    transaction_payload = {
        "customer_id": customer_id,
        "transaction_index": next_transaction_index,
        "amount": float(features.get("amount", 0) or 0),
        "balance_after": float(features.get("current_balance", 0) or 0),
        "days_since_last_payment": int(features.get("days_since_last_payment", 0) or 0),
        "previous_declines_24h": int(features.get("previous_declines_24h", 0) or 0),
        "merchant_category": str(features.get("merchant_category", "Unknown")),
        "is_international": str(features.get("is_international", "false")).lower() == "true",
        "transaction_time": datetime.now(timezone.utc).isoformat(),
        "seeded": False,
        "risk_score": prediction["risk_score"],
        "base_model_risk_score": prediction.get("base_model_risk_score"),
        "context_model_risk_score": prediction.get("context_model_risk_score"),
        "final_model_risk_score": prediction.get("final_model_risk_score", prediction["risk_score"]),
        "raw_json": features,
    }
    append_customer_transaction(customer_id, transaction_payload)
    stream_publish({"type": "manual_prediction", "customer_id": customer_id, "transaction": transaction_payload, "prediction": response})
    publish_live_score(response)
    return response


@router.get("/score/{customer_id}")
async def get_score(customer_id: str, db: Session = Depends(get_db)):
    cached = get_cached_score(customer_id)
    if cached:
        cached["cached"] = True
        cached["timestamp"] = datetime.now(timezone.utc).isoformat()
        return cached

    row = db.query(RiskScore).filter(RiskScore.customer_id == customer_id).order_by(RiskScore.created_at.desc()).first()
    if not row:
        raise HTTPException(status_code=404, detail="Customer not found")

    return {
        "customer_id": customer_id,
        "risk_score": row.risk_score,
        "risk_bucket": row.risk_bucket,
        "cached": False,
        "timestamp": str(row.created_at),
    }


@router.get("/history/{customer_id}")
async def get_history(customer_id: str, db: Session = Depends(get_db)):
    rows = db.query(RiskScore).filter(RiskScore.customer_id == customer_id).order_by(RiskScore.created_at.desc()).limit(20).all()
    return {
        "customer_id": customer_id,
        "history": [{"risk_score": row.risk_score, "risk_bucket": row.risk_bucket, "created_at": str(row.created_at)} for row in rows],
    }


@router.get("/all-scores")
async def get_all_scores(db: Session = Depends(get_db)):
    recent_rows = db.query(RiskScore).order_by(RiskScore.created_at.desc(), RiskScore.id.desc()).limit(5000).all()

    unique_rows: list[RiskScore] = []
    seen_customers: set[str] = set()
    for row in recent_rows:
        if row.customer_id in seen_customers:
            continue
        seen_customers.add(row.customer_id)
        unique_rows.append(row)
        if len(unique_rows) >= 500:
            break

    rows = sorted(unique_rows, key=lambda item: float(item.risk_score), reverse=True)
    return {
        "total": len(rows),
        "scores": [
            {"customer_id": row.customer_id, "risk_score": row.risk_score, "risk_bucket": row.risk_bucket, "created_at": str(row.created_at)}
            for row in rows
        ],
    }


@router.get("/explain/{customer_id}")
async def explain_risk(customer_id: str, db: Session = Depends(get_db)):
    rows = db.query(RiskScore).filter(RiskScore.customer_id == customer_id).order_by(RiskScore.created_at.desc()).limit(5).all()
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found or no score available")

    row = rows[0]
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY missing in server env configuration")

    history_rows = rows[::-1]
    history_str = ", ".join([f"{history_row.risk_score:.2f} ({history_row.risk_bucket})" for history_row in history_rows])
    
    prompt = (
        f"Act as a financial risk analyst analyzing the pre-delinquency pipeline.\n\n"
        f"## Pre-Delinquency Analysis: {customer_id}\n\n"
        f"Current Risk Score: **{row.risk_score:.2f}** | Classification: **{row.risk_bucket}**\n\n"
        f"Recent score trend: {history_str}.\n\n"
        f"Give a compact explanation in 3 to 5 bullet points only.\n"
        f"Focus on: the trend pattern, why the customer may be pre-delinquent, and the likely driving factors.\n"
        f"Use simple markdown bullets, keep each bullet short, and do not write a long paragraph.\n"
        f"End with a one-line bottom line if needed. Keep the response under {min(MAX_EXPLANATION_WORDS, 180)} words."
    )

    payload = {
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5,
        "max_tokens": 600,
    }
    headers = {"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            explanation = _truncate_words(data["choices"][0]["message"]["content"], MAX_EXPLANATION_WORDS)
            return {"customer_id": customer_id, "risk_score": row.risk_score, "risk_bucket": row.risk_bucket, "explanation": explanation}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to generate explanation from Groq LLM: {exc}")


@router.get("/customers")
async def list_customers(db: Session = Depends(get_db)):
    profiles = db.query(CustomerProfile).order_by(CustomerProfile.customer_id.asc()).all()
    result = []
    for profile in profiles:
        tx_count = db.query(CustomerTransaction).filter(CustomerTransaction.customer_id == profile.customer_id).count()
        latest_tx = db.query(CustomerTransaction).filter(CustomerTransaction.customer_id == profile.customer_id).order_by(CustomerTransaction.transaction_time.desc()).first()
        latest_score = db.query(RiskScore).filter(RiskScore.customer_id == profile.customer_id).order_by(RiskScore.created_at.desc()).first()
        tx_rows = db.query(CustomerTransaction).filter(CustomerTransaction.customer_id == profile.customer_id).order_by(CustomerTransaction.transaction_time.desc()).limit(120).all()
        top_reasons, avg_spend_risk = _spending_summary(tx_rows)
        result.append(
            {
                "customer_id": profile.customer_id,
                "name": profile.name,
                "branch": profile.branch,
                "loan_type": profile.loan_type,
                "risk_segment": profile.risk_segment,
                "monthly_income": profile.monthly_income,
                "loan_amount": profile.loan_amount,
                "occupation": profile.occupation,
                "spending_culture": profile.spending_culture,
                "intervention_status": profile.intervention_status,
                "pre_npa": profile.pre_npa,
                "account_age_months": profile.account_age_months,
                "relationship_manager": profile.relationship_manager,
                "top_spending_reasons": top_reasons,
                "avg_spend_risk_score": avg_spend_risk,
                "transaction_count": tx_count,
                "latest_amount": latest_tx.amount if latest_tx else None,
                "latest_transaction_time": str(latest_tx.transaction_time) if latest_tx else None,
                "latest_risk_score": latest_score.risk_score if latest_score else None,
                "latest_risk_bucket": latest_score.risk_bucket if latest_score else None,
            }
        )
    return {"total": len(result), "customers": result}


@router.get("/customers/{customer_id}/transactions")
async def customer_transactions(customer_id: str, db: Session = Depends(get_db)):
    profile = db.get(CustomerProfile, customer_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Customer not found")

    rows = db.query(CustomerTransaction).filter(CustomerTransaction.customer_id == customer_id).order_by(CustomerTransaction.transaction_time.asc()).all()
    return {
        "customer_id": customer_id,
        "profile": {
            "customer_id": profile.customer_id,
            "name": profile.name,
            "branch": profile.branch,
            "loan_type": profile.loan_type,
            "risk_segment": profile.risk_segment,
            "monthly_income": profile.monthly_income,
            "loan_amount": profile.loan_amount,
            "occupation": profile.occupation,
            "spending_culture": profile.spending_culture,
            "intervention_status": profile.intervention_status,
            "pre_npa": profile.pre_npa,
            "account_age_months": profile.account_age_months,
            "relationship_manager": profile.relationship_manager,
        },
        "transactions": [
            {
                "transaction_index": row.transaction_index,
                "amount": row.amount,
                "balance_after": row.balance_after,
                "days_since_last_payment": row.days_since_last_payment,
                "previous_declines_24h": row.previous_declines_24h,
                "merchant_category": row.merchant_category,
                "is_international": row.is_international,
                "transaction_time": row.transaction_time.isoformat(),
                "risk_score": row.risk_score,
                "risk_bucket": row.risk_bucket,
                "is_seeded": row.is_seeded,
            }
            for row in rows
        ],
    }
