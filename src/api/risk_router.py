import os
import uuid
import json
import httpx
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from src.api.models import TransactionInput, RiskScoreResponse
from src.api.predict import predict_risk
from src.api.database import get_db, RiskScore, Intervention, Transaction
from src.cache.redis_client import get_cached_score, set_cached_score, redis
from src.kafka.producer import publish_transaction
from src.api.intervention import execute_intervention

load_dotenv()
router = APIRouter()

def get_intervention_type(bucket: str) -> str:
    mapping = {
        "LOW_RISK":      "MONITOR",
        "HIGH_RISK":     "SMS_REMINDER",
        "CRITICAL":      "PAYMENT_PLAN",
        "VERY_CRITICAL": "HUMAN_ESCALATION"
    }
    return mapping.get(bucket, "UNKNOWN")

@router.post("/predict", response_model=RiskScoreResponse)
async def predict_endpoint(payload: TransactionInput, db: Session = Depends(get_db)):
    customer_id = payload.customer_id

    # 1 — Check Redis cache
    if not payload.bypass_cache:
        cached = get_cached_score(customer_id)
        if cached:
            cached["cached"] = True
            cached["timestamp"] = datetime.utcnow().isoformat()
            return cached

    # 2 — Run prediction
    result = predict_risk(payload.features)

    # 3 — Save risk score to SQLite
    try:
        score_row = RiskScore(
            id=str(uuid.uuid4()),
            customer_id=customer_id,
            risk_score=result["risk_score"],
            risk_bucket=result["risk_bucket"]
        )
        db.add(score_row)

        # Save intervention log
        intervention_row = Intervention(
            id=str(uuid.uuid4()),
            customer_id=customer_id,
            risk_bucket=result["risk_bucket"],
            intervention_type=get_intervention_type(result["risk_bucket"]),
            message=result["intervention_recommended"],
            status="PENDING"
        )
        db.add(intervention_row)
        
        # Save transaction audit log
        audit_row = Transaction(
            id=str(uuid.uuid4()),
            customer_id=customer_id,
            transaction_data=json.dumps(payload.features)
        )
        db.add(audit_row)
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"DB insert error: {e}")

    # Fire off our new intervention engine routines
    execute_intervention(customer_id, result["risk_bucket"], payload.features)

    # 4 — Cache in Redis
    score_data = {
        "customer_id":              customer_id,
        "risk_score":               result["risk_score"],
        "risk_bucket":              result["risk_bucket"],
        "intervention_recommended": result["intervention_recommended"]
    }
    set_cached_score(customer_id, score_data)

    # 5 — Publish to Kafka
    publish_transaction(customer_id, {
        "customer_id": customer_id,
        "features":    payload.features,
        "risk_score":  result["risk_score"],
        "risk_bucket": result["risk_bucket"]
    })
    # Publish to websocket channel
    live_message = {
        "customer_id": customer_id,
        "risk_score": result["risk_score"],
        "risk_bucket": result["risk_bucket"],
        "created_at": datetime.utcnow().isoformat(),
        "intervention_recommended": result["intervention_recommended"]
    }
    redis.publish("pie:live_scores", json.dumps(live_message))
    return RiskScoreResponse(
        customer_id=customer_id,
        risk_score=result["risk_score"],
        risk_bucket=result["risk_bucket"],
        intervention_recommended=result["intervention_recommended"],
        cached=False,
        timestamp=datetime.utcnow().isoformat()
    )

@router.get("/score/{customer_id}", response_model=RiskScoreResponse)
async def get_score(customer_id: str, db: Session = Depends(get_db)):
    # Try cache first
    cached = get_cached_score(customer_id)
    if cached:
        cached["cached"] = True
        cached["timestamp"] = datetime.utcnow().isoformat()
        return cached

    # Fall back to SQLite
    row = (db.query(RiskScore)
           .filter(RiskScore.customer_id == customer_id)
           .order_by(RiskScore.created_at.desc())
           .first())

    if not row:
        raise HTTPException(status_code=404, detail="Customer not found")

    return RiskScoreResponse(
        customer_id=customer_id,
        risk_score=row.risk_score,
        risk_bucket=row.risk_bucket,
        intervention_recommended="Use /predict for fresh recommendation",
        cached=False,
        timestamp=str(row.created_at)
    )

@router.get("/history/{customer_id}")
async def get_history(customer_id: str, db: Session = Depends(get_db)):
    rows = (db.query(RiskScore)
            .filter(RiskScore.customer_id == customer_id)
            .order_by(RiskScore.created_at.desc())
            .limit(10)
            .all())

    return {
        "customer_id": customer_id,
        "history": [
            {
                "risk_score":  r.risk_score,
                "risk_bucket": r.risk_bucket,
                "created_at":  str(r.created_at)
            } for r in rows
        ]
    }

@router.get("/interventions/{customer_id}")
async def get_interventions(customer_id: str, db: Session = Depends(get_db)):
    rows = (db.query(Intervention)
            .filter(Intervention.customer_id == customer_id)
            .order_by(Intervention.created_at.desc())
            .all())

    return {
        "customer_id": customer_id,
        "interventions": [
            {
                "intervention_type": r.intervention_type,
                "risk_bucket":       r.risk_bucket,
                "message":           r.message,
                "status":            r.status,
                "created_at":        str(r.created_at)
            } for r in rows
        ]
    }

@router.get("/all-scores")
async def get_all_scores(db: Session = Depends(get_db)):
    rows = (db.query(RiskScore)
            .order_by(RiskScore.created_at.desc())
            .limit(500)
            .all())

    return {
        "total": len(rows),
        "scores": [
            {
                "customer_id": r.customer_id,
                "risk_score":  r.risk_score,
                "risk_bucket": r.risk_bucket,
                "created_at":  str(r.created_at)
            } for r in rows
        ]
    }

@router.get("/explain/{customer_id}")
async def explain_risk(customer_id: str, db: Session = Depends(get_db)):
    # Fetch recent risk scores/history to give context
    rows = (db.query(RiskScore)
            .filter(RiskScore.customer_id == customer_id)
            .order_by(RiskScore.created_at.desc())
            .limit(5)
            .all())

    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found or no score available")

    # Get the latest score
    row = rows[0]

    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY missing in server env configuration")

    history_str = ", ".join([f"{r.risk_score:.2f} ({r.risk_bucket})" for r in rows[::-1]])
    
    prompt = (f"Act as a financial risk analyst analyzing the pre-delinquency pipeline. "
              f"The customer {customer_id} currently has a calculated risk score of {row.risk_score:.2f} "
              f"and is classified as '{row.risk_bucket}' by our ML layer. "
              f"Their recent score trend (oldest to newest) is: {history_str}. "
              f"Explain briefly why this sequence and classification might lead to pre-default, "
              f"and provide an intuition on the possible driving factors behind this high risk. "
              f"Please ensure your explanation is complete, concise, and does not cut off.")
              
    payload = {
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5,
        "max_tokens": 600
    }
    
    headers = {
        "Authorization": f"Bearer {groq_api_key}",
        "Content-Type": "application/json"
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=10.0
            )
            resp.raise_for_status()
            data = resp.json()
            explanation = data["choices"][0]["message"]["content"]
            
            return {
                "customer_id": customer_id,
                "risk_score": row.risk_score,
                "risk_bucket": row.risk_bucket,
                "explanation": explanation
            }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to generate explanation from Groq LLM: {str(e)}")
