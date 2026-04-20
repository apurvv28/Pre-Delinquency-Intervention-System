import json
import os
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import and_, func
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
    row = db.query(RiskScore).filter(RiskScore.customer_id == customer_id).order_by(RiskScore.created_at.desc()).first()
    if not row:
        cached = get_cached_score(customer_id)
        if cached:
            cached["cached"] = True
            cached["timestamp"] = datetime.now(timezone.utc).isoformat()
            return cached
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
    ranked_scores = (
        db.query(
            RiskScore.id.label("id"),
            RiskScore.customer_id.label("customer_id"),
            RiskScore.risk_score.label("risk_score"),
            RiskScore.risk_bucket.label("risk_bucket"),
            RiskScore.created_at.label("created_at"),
            func.row_number()
            .over(
                partition_by=RiskScore.customer_id,
                order_by=(RiskScore.created_at.desc(), RiskScore.id.desc()),
            )
            .label("rn"),
        )
        .subquery()
    )

    rows = (
        db.query(
            ranked_scores.c.customer_id,
            ranked_scores.c.risk_score,
            ranked_scores.c.risk_bucket,
            ranked_scores.c.created_at,
        )
        .filter(ranked_scores.c.rn == 1)
        .order_by(ranked_scores.c.risk_score.desc())
        .limit(500)
        .all()
    )
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
    
    # Check Redis cache first.
    cache_key = f"explain_cache:explain:{customer_id}:{row.risk_score:.1f}"
    cached_explanation = None
    try:
        cached_explanation = redis.get(cache_key)
    except Exception:
        cached_explanation = None

    if isinstance(cached_explanation, bytes):
        cached_explanation = cached_explanation.decode("utf-8", errors="ignore")
    if isinstance(cached_explanation, str) and cached_explanation.strip():
        return {
            "customer_id": customer_id,
            "risk_score": row.risk_score,
            "risk_bucket": row.risk_bucket,
            "explanation": cached_explanation,
        }
    
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

    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                explanation = _truncate_words(data["choices"][0]["message"]["content"], MAX_EXPLANATION_WORDS)
                
                # Cache the explanation for 1 hour.
                try:
                    redis.setex(cache_key, 3600, explanation)
                except Exception:
                    pass  # Cache failure is non-critical
                
                return {"customer_id": customer_id, "risk_score": row.risk_score, "risk_bucket": row.risk_bucket, "explanation": explanation}
        except httpx.TimeoutException:
            if attempt < max_retries - 1:
                import asyncio
                await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                continue
            raise HTTPException(status_code=504, detail="Groq API timeout after retries")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429 and attempt < max_retries - 1:  # Rate limit
                import asyncio
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(status_code=502, detail=f"Groq API error: {exc.response.status_code}")
        except Exception as exc:
            if attempt < max_retries - 1:
                import asyncio
                await asyncio.sleep(2 ** attempt)
                continue
            raise HTTPException(status_code=502, detail=f"Failed to generate explanation from Groq LLM: {str(exc)}")


@router.get("/customers")
async def list_customers(db: Session = Depends(get_db)):
    ranked_scores = (
        db.query(
            RiskScore.customer_id.label("customer_id"),
            RiskScore.risk_score.label("latest_risk_score"),
            RiskScore.risk_bucket.label("latest_risk_bucket"),
            func.row_number()
            .over(
                partition_by=RiskScore.customer_id,
                order_by=(RiskScore.created_at.desc(), RiskScore.id.desc()),
            )
            .label("rn"),
        )
        .subquery()
    )

    latest_scores = (
        db.query(
            ranked_scores.c.customer_id,
            ranked_scores.c.latest_risk_score,
            ranked_scores.c.latest_risk_bucket,
        )
        .filter(ranked_scores.c.rn == 1)
        .subquery()
    )

    tx_stats = (
        db.query(
            CustomerTransaction.customer_id.label("customer_id"),
            func.count(CustomerTransaction.id).label("transaction_count"),
            func.max(CustomerTransaction.transaction_time).label("latest_transaction_time"),
            func.avg(func.coalesce(CustomerTransaction.risk_score, 0.0)).label("avg_spend_risk_score"),
            func.max(CustomerTransaction.transaction_index).label("latest_tx_index"),
        )
        .group_by(CustomerTransaction.customer_id)
        .subquery()
    )

    latest_tx_amount = (
        db.query(
            CustomerTransaction.customer_id.label("customer_id"),
            CustomerTransaction.amount.label("latest_amount"),
        )
        .join(
            tx_stats,
            and_(
                CustomerTransaction.customer_id == tx_stats.c.customer_id,
                CustomerTransaction.transaction_index == tx_stats.c.latest_tx_index,
            ),
        )
        .subquery()
    )

    rows = (
        db.query(
            CustomerProfile.customer_id,
            CustomerProfile.name,
            CustomerProfile.branch,
            CustomerProfile.loan_type,
            CustomerProfile.risk_segment,
            CustomerProfile.monthly_income,
            CustomerProfile.loan_amount,
            CustomerProfile.occupation,
            CustomerProfile.spending_culture,
            CustomerProfile.intervention_status,
            CustomerProfile.pre_npa,
            CustomerProfile.account_age_months,
            CustomerProfile.relationship_manager,
            latest_scores.c.latest_risk_score,
            latest_scores.c.latest_risk_bucket,
            tx_stats.c.transaction_count,
            tx_stats.c.latest_transaction_time,
            tx_stats.c.avg_spend_risk_score,
            latest_tx_amount.c.latest_amount,
        )
        .outerjoin(latest_scores, latest_scores.c.customer_id == CustomerProfile.customer_id)
        .outerjoin(tx_stats, tx_stats.c.customer_id == CustomerProfile.customer_id)
        .outerjoin(latest_tx_amount, latest_tx_amount.c.customer_id == CustomerProfile.customer_id)
        .order_by(CustomerProfile.customer_id.asc())
        .all()
    )

    result = []
    for row in rows:
        result.append(
            {
                "customer_id": row.customer_id,
                "name": row.name,
                "branch": row.branch,
                "loan_type": row.loan_type,
                "risk_segment": row.risk_segment,
                "monthly_income": row.monthly_income,
                "loan_amount": row.loan_amount,
                "occupation": row.occupation,
                "spending_culture": row.spending_culture,
                "intervention_status": row.intervention_status,
                "pre_npa": row.pre_npa,
                "account_age_months": row.account_age_months,
                "relationship_manager": row.relationship_manager,
                # Keep this endpoint lightweight for fast customer search/registry rendering.
                "top_spending_reasons": [],
                "avg_spend_risk_score": round(float(row.avg_spend_risk_score), 2) if row.avg_spend_risk_score is not None else 0.0,
                "transaction_count": int(row.transaction_count) if row.transaction_count is not None else 0,
                "latest_amount": float(row.latest_amount) if row.latest_amount is not None else None,
                "latest_transaction_time": str(row.latest_transaction_time) if row.latest_transaction_time is not None else None,
                "latest_risk_score": float(row.latest_risk_score) if row.latest_risk_score is not None else None,
                "latest_risk_bucket": row.latest_risk_bucket,
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
