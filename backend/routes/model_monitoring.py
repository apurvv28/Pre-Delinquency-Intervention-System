from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.contextual_xgb import get_contextual_model_status, get_contextual_model_monitoring
from backend.database import get_db
from backend.monitoring import build_monitoring_report
from backend.retraining import retrain_model

router = APIRouter(prefix="/api/v1/model-monitoring", tags=["model-monitoring"])


@router.get("")
async def get_model_monitoring(db: Session = Depends(get_db)):
    return build_monitoring_report(db)


@router.get("/both")
async def get_both_models_monitoring(db: Session = Depends(get_db)):
    """Get monitoring data for both base model (LightGBM) and contextual model (XGBoost)."""
    base_model_monitoring = build_monitoring_report(db)
    contextual_model_monitoring = get_contextual_model_monitoring(db)
    
    return {
        "baseModel": {
            "name": "LightGBM Base Model",
            "type": "lightgbm",
            **base_model_monitoring,
        },
        "contextualModel": {
            "name": "XGBoost Contextual Model",
            "type": "xgboost",
            **contextual_model_monitoring,
        },
    }


@router.post("/retrain")
async def retrain_now(db: Session = Depends(get_db)):
    before = build_monitoring_report(db)
    try:
        result = retrain_model(
            trigger_reason="manual",
            drift_score=float(before["drift"]["driftScore"]),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Manual retraining failed: {exc}") from exc

    after = build_monitoring_report(db)
    return {
        "status": "retrained",
        "monitoringBefore": before,
        "monitoring": after,
        "result": result,
    }


@router.get("/contextual")
async def contextual_status():
    return get_contextual_model_status()


@router.get("/contextual/monitoring")
async def contextual_monitoring(db: Session = Depends(get_db)):
    """Get comprehensive monitoring report for contextual model."""
    return get_contextual_model_monitoring(db)


@router.post("/contextual/retrain")
async def retrain_contextual_model(db: Session = Depends(get_db)):
    raise HTTPException(
        status_code=403,
        detail="Retraining is restricted to the base LightGBM model only.",
    )