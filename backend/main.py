import os
import threading

from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.cache import ping_redis, stream_create_group
from backend.database import SessionLocal, init_db
from backend.drift_retrain_pipeline import ensure_model_registry_bootstrap, run_drift_check_and_optionally_trigger
from backend.intervention_system import start_intervention_scheduler, stop_intervention_scheduler
from backend.routes.auth import router as auth_router
from backend.routes.drift_retraining import router as drift_retraining_router
from backend.routes.interventions import router as intervention_router
from backend.routes.model_monitoring import router as model_monitoring_router
from backend.routes.risk import router as risk_router
from backend.seed import seed_backend_data
from backend.simulator import stream_customer_transactions
from backend.stream_producer import start_advanced_stream_producer, stop_advanced_stream_producer
from backend.stream_consumer import start_stream_consumer, stop_stream_consumer
from backend.retraining import ensure_baseline_history
from backend.websocket_manager import manager

load_dotenv()

app = FastAPI(
    title="PIE — Pre-Delinquency Intelligence Engine",
    description="Real-time behavioural risk scoring API",
    version="2.0.0",
)

frontend_origin = os.getenv("FRONTEND_URL", "http://localhost:5173")
extra_origins = [origin.strip() for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_origin, *extra_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(risk_router)
app.include_router(auth_router)
app.include_router(model_monitoring_router)
app.include_router(drift_retraining_router)
app.include_router(intervention_router)

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")


@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time transaction and score updates."""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive, receive any client messages if needed
            data = await websocket.receive_text()
            # Echo back or handle client commands (optional)
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as e:
        print(f"[WS] Error: {e}")
        await manager.disconnect(websocket)




def _scheduled_drift_check() -> None:
    with SessionLocal() as db:
        run_drift_check_and_optionally_trigger(db, trigger_mode="scheduled", actor="scheduler")


@app.on_event("startup")
async def startup_event():
    init_db()
    stream_create_group()

    skip_seed = os.getenv("SKIP_SEED_ON_STARTUP", "false").strip().lower() in {"1", "true", "yes", "on"}
    skip_stream = os.getenv("DISABLE_TRANSACTION_STREAM", "false").strip().lower() in {"1", "true", "yes", "on"}
    skip_retraining_scheduler = os.getenv("DISABLE_RETRAINING_SCHEDULER", "false").strip().lower() in {"1", "true", "yes", "on"}
    preseed_transactions = os.getenv("PRESEED_TRANSACTIONS_ON_STARTUP", "false").strip().lower() in {"1", "true", "yes", "on"}
    use_advanced_stream_producer = os.getenv("USE_ADVANCED_STREAM_PRODUCER", "true").strip().lower() in {"1", "true", "yes", "on"}

    if not skip_seed:
        seed_backend_data(seed_transactions=preseed_transactions)

    with SessionLocal() as db:
        ensure_model_registry_bootstrap(db)

    ensure_baseline_history()

    if not skip_stream:
        if use_advanced_stream_producer:
            start_advanced_stream_producer()
        else:
            threading.Thread(target=stream_customer_transactions, daemon=True).start()
        
        # Start consumer to process streamed transactions
        start_stream_consumer()

    if not skip_retraining_scheduler:
        scheduler.add_job(
            _scheduled_drift_check,
            trigger="cron",
            hour=2,
            minute=0,
            id="pie_drift_daily_check",
            replace_existing=True,
        )
        scheduler.start()

    start_intervention_scheduler()


@app.on_event("shutdown")
async def shutdown_event():
    stop_advanced_stream_producer()
    stop_stream_consumer()
    if scheduler.running:
        scheduler.shutdown(wait=False)
    stop_intervention_scheduler()


@app.get("/")
async def health():
    redis_ok = ping_redis()

    return {
        "status": "healthy",
        "model_loaded": True,
        "redis_connected": redis_ok,
        "kafka_connected": redis_ok,
        "supabase_connected": False,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
