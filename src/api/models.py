from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class TransactionInput(BaseModel):
    customer_id: str
    features: Dict[str, Any]  # raw feature dict from transaction
    bypass_cache: bool = False

class RiskScoreResponse(BaseModel):
    customer_id: str
    risk_score: float
    risk_bucket: str
    intervention_recommended: str
    cached: bool = False
    timestamp: str

class InterventionResponse(BaseModel):
    customer_id: str
    risk_bucket: str
    intervention_type: str
    message: str
    status: str

class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    redis_connected: bool
    kafka_connected: bool
    supabase_connected: bool