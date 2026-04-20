from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import delete
from sqlalchemy.orm import Session

from backend.cache import redis
from backend.database import CustomerTransaction, RiskScore, get_db
from backend.timezone_util import get_ist_now
from backend.drift_retrain_pipeline import (
    DRIFT_RETRAIN_THRESHOLD,
    DRIFT_WARN_THRESHOLD,
    activate_model,
    get_retrain_config,
    latest_drift_status,
    list_model_versions,
    ping_colab_health,
    record_prediction_distribution,
    refresh_retraining_status,
    run_drift_check_and_optionally_trigger,
    trigger_colab_retraining,
    update_retrain_config,
)

router = APIRouter(tags=["drift-retraining"])


def _actor_from_request(request: Request) -> str:
    actor = request.headers.get("x-pie-actor")
    if actor:
        return actor.strip()
    return "system"


@router.post("/api/drift/check")
async def run_drift_check(request: Request, db: Session = Depends(get_db)):
    actor = _actor_from_request(request)
    return run_drift_check_and_optionally_trigger(db, trigger_mode="manual", actor=actor)


@router.get("/api/drift/status")
async def get_drift_status(db: Session = Depends(get_db)):
    latest = latest_drift_status(db)
    next_check = (get_ist_now() + timedelta(hours=24)).isoformat()
    latest["nextScheduledCheck"] = next_check
    latest["thresholds"] = {
        "warning": DRIFT_WARN_THRESHOLD,
        "retrain": DRIFT_RETRAIN_THRESHOLD,
    }
    return latest


@router.post("/api/retrain/trigger")
async def trigger_retrain(request: Request, payload: dict | None = None, db: Session = Depends(get_db)):
    actor = _actor_from_request(request)
    requested_model = str((payload or {}).get("model", "base")).strip().lower()
    if requested_model not in {"base", "lightgbm"}:
        raise HTTPException(
            status_code=400,
            detail="Only base model retraining is supported.",
        )
    current_drift = latest_drift_status(db)
    drift_score = None
    if current_drift.get("available"):
        drift_score = float(current_drift.get("compositeDriftScore", 0.0))

    result = trigger_colab_retraining(
        db,
        trigger_type=str((payload or {}).get("triggerType", "manual_admin")),
        triggered_by=actor,
        drift_score=drift_score,
    )
    return result


@router.get("/api/retrain/status")
async def retrain_status(job_id: str | None = None, db: Session = Depends(get_db)):
    return refresh_retraining_status(db, job_id=job_id)


@router.get("/api/retrain/logs/stream")
async def retrain_logs_stream(job_id: str | None = None, db: Session = Depends(get_db)):
    status_payload = refresh_retraining_status(db, job_id=job_id)
    if not status_payload.get("available"):
        raise HTTPException(status_code=404, detail="No retraining job found")

    active_job_id = status_payload.get("jobId")
    config = get_retrain_config()
    base_url = str(config.get("colab_ngrok_url") or "").strip().rstrip("/")
    if not base_url:
        raise HTTPException(status_code=400, detail="Colab URL not configured")

    async def event_stream():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", f"{base_url}/logs/{active_job_id}") as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line:
                        yield f"{line}\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/api/model/activate")
async def activate_model_endpoint(request: Request, payload: dict, db: Session = Depends(get_db)):
    actor = _actor_from_request(request)
    version = str(payload.get("version") or "").strip()
    drive_file_id = str(payload.get("drive_file_id") or "").strip()
    approver_primary = str(payload.get("approver_primary") or "").strip()
    approver_secondary = str(payload.get("approver_secondary") or "").strip()
    if not version:
        raise HTTPException(status_code=400, detail="version is required")
    if not drive_file_id:
        raise HTTPException(status_code=400, detail="drive_file_id is required")
    if not approver_primary or not approver_secondary:
        raise HTTPException(status_code=400, detail="approver_primary and approver_secondary are required")
    if approver_primary == approver_secondary:
        raise HTTPException(status_code=400, detail="approvers must be distinct")

    return activate_model(
        db,
        actor=actor,
        approver_primary=approver_primary,
        approver_secondary=approver_secondary,
        version=version,
        drive_file_id=drive_file_id,
        features_file_id=payload.get("features_file_id"),
        threshold_file_id=payload.get("threshold_file_id"),
        preprocessor_file_id=payload.get("preprocessor_file_id"),
    )


@router.get("/api/model/history")
async def model_history(db: Session = Depends(get_db)):
    return {"models": list_model_versions(db)}


@router.get("/api/model/prediction-distribution")
async def prediction_distribution(days: int = 7, db: Session = Depends(get_db)):
    return record_prediction_distribution(db, days=max(1, min(days, 30)))


@router.get("/api/retrain/config")
async def get_retrain_runtime_config():
    config = get_retrain_config()
    return {
        **config,
        "colabConnection": ping_colab_health(),
    }


@router.put("/api/retrain/config")
async def update_retrain_runtime_config(payload: dict):
    updated = update_retrain_config(
        colab_ngrok_url=payload.get("colab_ngrok_url"),
        bank_threshold=payload.get("bank_threshold"),
    )
    return {
        **updated,
        "colabConnection": ping_colab_health(),
    }


@router.post("/api/stream/restart")
async def restart_stream(request: Request, db: Session = Depends(get_db)):
    """
    Restart the transaction stream from 0 transactions.
    This endpoint:
    1. Deletes all CustomerTransaction records from the database
    2. Deletes all RiskScore records from the database
    3. Clears the Redis stream
    4. Deletes and recreates the consumer group
    
    After this, the stream producer/consumer will restart fresh,
    flowing transactions through the ML models (LightGBM + XGBoost fusion).
    """
    actor = _actor_from_request(request)
    
    try:
        # Delete all transactions
        db.execute(delete(CustomerTransaction))
        db.commit()
        tx_count = 0
        print(f"[STREAM_RESTART] Cleared all CustomerTransaction records")
        
        # Delete all risk scores
        db.execute(delete(RiskScore))
        db.commit()
        print(f"[STREAM_RESTART] Cleared all RiskScore records")
        
        # Clear Redis stream
        stream_key = "pie:transactions"
        consumer_group = "pie-prediction-engine"
        
        try:
            # Delete consumer group (this will clear pending messages)
            redis.execute("XGROUP", "DESTROY", stream_key, consumer_group, skip_error=True)
            print(f"[STREAM_RESTART] Deleted consumer group {consumer_group}")
        except Exception as e:
            print(f"[STREAM_RESTART] Note: Consumer group deletion: {e}")
        
        try:
            # Delete the entire stream
            redis.delete(stream_key)
            print(f"[STREAM_RESTART] Cleared Redis stream {stream_key}")
        except Exception as e:
            print(f"[STREAM_RESTART] Note: Stream cleanup: {e}")
        
        # Clear risk cache keys
        try:
            # Scan and delete all risk: prefixed keys
            redis.execute("EVAL", """
                local keys = redis.call('keys', 'risk:*')
                for i, key in ipairs(keys) do
                    redis.call('del', key)
                end
                return #keys
            """, 0)
            print(f"[STREAM_RESTART] Cleared Redis risk cache")
        except Exception as e:
            print(f"[STREAM_RESTART] Note: Risk cache cleanup: {e}")
        
        return {
            "status": "success",
            "message": "Stream restarted successfully",
            "cleared": {
                "transactions": "all",
                "risk_scores": "all",
                "redis_stream": stream_key,
                "consumer_group": consumer_group,
                "cache": "risk:*"
            },
            "actor": actor,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "next_steps": "Streamed transactions will now flow through LightGBM + XGBoost fusion model and appear in customer registry."
        }
        
    except Exception as e:
        print(f"[STREAM_RESTART] ERROR: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to restart stream: {str(e)}"
        )

