from datetime import datetime
import csv
import io

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.intervention_system import (
    approve_intervention,
    get_history,
    list_queue,
    orchestrate_from_latest_scores,
    preview_intervention,
    reject_intervention,
    send_manual_intervention_email,
    send_test_email,
)

router = APIRouter(prefix="/api/v1/interventions", tags=["interventions"])


@router.post("/orchestrate")
async def run_orchestrator(payload: dict | None = None, db: Session = Depends(get_db)):
    actor = str((payload or {}).get("actor") or "admin")
    return orchestrate_from_latest_scores(db, actor=actor)


@router.get("/queue")
async def get_queue(status: str | None = None, db: Session = Depends(get_db)):
    return {"items": list_queue(db, status=status)}


@router.get("/history")
async def intervention_history(engine_tier: int | None = None, status: str | None = None, db: Session = Depends(get_db)):
    return {"items": get_history(db, engine_tier=engine_tier, status=status)}


@router.get("/history/export.csv")
async def intervention_history_export(engine_tier: int | None = None, status: str | None = None, db: Session = Depends(get_db)):
    rows = get_history(db, engine_tier=engine_tier, status=status)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "id",
        "customer_id",
        "engine_tier",
        "tier_label",
        "risk_score",
        "status",
        "delivery_status",
        "approved_by",
        "maker_id",
        "checker_id",
        "created_at",
        "sent_at",
        "retry_count",
        "case_file_path",
        "subject",
    ])
    for row in rows:
        writer.writerow([
            row.get("id"),
            row.get("customer_id"),
            row.get("engine_tier"),
            row.get("tier_label"),
            row.get("risk_score"),
            row.get("status"),
            row.get("delivery_status"),
            row.get("approved_by"),
            row.get("maker_id"),
            row.get("checker_id"),
            row.get("created_at"),
            row.get("sent_at"),
            row.get("retry_count"),
            row.get("case_file_path"),
            row.get("subject"),
        ])

    content = buffer.getvalue()
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=intervention_history.csv"},
    )


@router.get("/{intervention_id}/preview")
async def intervention_preview(intervention_id: str, db: Session = Depends(get_db)):
    try:
        return preview_intervention(db, intervention_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{intervention_id}/approve")
async def intervention_approve(intervention_id: str, payload: dict, db: Session = Depends(get_db)):
    admin_id = str(payload.get("admin_id") or "admin")
    checker_id = payload.get("checker_id")
    schedule_at_raw = payload.get("schedule_at")
    comment = payload.get("comment")

    schedule_at = None
    if schedule_at_raw:
        schedule_at = datetime.fromisoformat(str(schedule_at_raw).replace("Z", "+00:00"))

    try:
        return approve_intervention(
            db,
            intervention_id=intervention_id,
            admin_id=admin_id,
            checker_id=checker_id,
            schedule_at=schedule_at,
            comment=comment,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{intervention_id}/reject")
async def intervention_reject(intervention_id: str, payload: dict, db: Session = Depends(get_db)):
    admin_id = str(payload.get("admin_id") or "admin")
    reason = str(payload.get("reason") or "Rejected by admin")
    try:
        return reject_intervention(db, intervention_id=intervention_id, admin_id=admin_id, reason=reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{intervention_id}/test-email")
async def intervention_test_email(intervention_id: str, payload: dict, db: Session = Depends(get_db)):
    admin_id = str(payload.get("admin_id") or "admin")
    admin_email = str(payload.get("admin_email") or "")
    if not admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")

    try:
        return send_test_email(db, intervention_id=intervention_id, admin_email=admin_email, admin_id=admin_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/bulk-approve")
async def intervention_bulk_approve(payload: dict, db: Session = Depends(get_db)):
    ids = payload.get("ids") or []
    admin_id = str(payload.get("admin_id") or "admin")
    if not isinstance(ids, list) or not ids:
        raise HTTPException(status_code=400, detail="ids must be a non-empty list")

    outcomes = []
    for intervention_id in ids:
        try:
            result = approve_intervention(db, intervention_id=str(intervention_id), admin_id=admin_id)
            outcomes.append({"id": intervention_id, "ok": True, "result": result})
        except ValueError as exc:
            outcomes.append({"id": intervention_id, "ok": False, "error": str(exc)})
    return {"items": outcomes}


@router.post("/manual-send")
async def manual_send_email(payload: dict):
    """Admin-triggered manual email to a specific customer.

    Body:
        customer_id (str): required
        admin_id (str): optional, defaults to 'admin'
        engine_tier (int): optional, auto-determined from latest risk score
    """
    customer_id = str(payload.get("customer_id") or "").strip()
    if not customer_id:
        raise HTTPException(status_code=400, detail="customer_id is required")

    admin_id = str(payload.get("admin_id") or "admin")
    engine_tier = payload.get("engine_tier")
    if engine_tier is not None:
        engine_tier = int(engine_tier)

    try:
        result = send_manual_intervention_email(
            customer_id=customer_id,
            admin_id=admin_id,
            engine_tier=engine_tier,
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
