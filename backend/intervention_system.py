import hashlib
import json
import os
import smtplib
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Literal

from apscheduler.schedulers.background import BackgroundScheduler
from jinja2 import Template
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.database import (
    CustomerProfile,
    CustomerTransaction,
    InterventionAuditLog,
    InterventionQueue,
    RiskScore,
    SessionLocal,
)
from backend.timezone_util import get_ist_now

EngineTier = Literal[1, 2, 3]

INTERVENTION_SCHEDULER = BackgroundScheduler(timezone="Asia/Kolkata")
ENGINE_SLA_HOURS = {1: 48, 2: 24, 3: 6}
TERMINAL_STATUSES = {"SENT", "FAILED", "REJECTED", "CANCELLED"}
ACTIVE_STATUSES = {"PENDING", "AWAITING_CHECKER", "APPROVED", "QUEUED"}
CASE_FILES_DIR = Path(__file__).resolve().parent / "data" / "case_files"

ADVISORY_TEMPLATE = Template(
    """
    <html><body style="font-family:Arial,sans-serif;background:#fff;color:#0f172a;">
      <div style="border-bottom:6px solid #003366;padding:14px 0;margin-bottom:14px;"><h2 style="margin:0;">{{ bank_name }} Customer Wellness</h2></div>
      <p>Dear {{ customer_name }},</p>
      <p>Your loan account <b>{{ loan_id }}</b> is in good standing. We noticed early stress indicators and want to help you stay on track.</p>
      <p><b>EMI:</b> {{ emi_amount }} | <b>Due Date:</b> {{ due_date }} | <b>On-time Streak:</b> {{ payment_streak }} months</p>
      <p><b>Financial Tips</b></p><ul>{% for tip in tips_list %}<li>{{ tip }}</li>{% endfor %}</ul>
      <p>Book a 15-minute wellness call: <a href="{{ booking_link }}">{{ booking_link }}</a></p>
      <p style="font-size:12px;color:#475569;">This is a proactive outreach from {{ bank_name }}.</p>
    </body></html>
    """
)

ALERT_TEMPLATE = Template(
    """
    <html><body style="font-family:Georgia,Arial,sans-serif;background:#fff;color:#0f172a;">
      <div style="background:#f59e0b;color:#111827;padding:12px;font-weight:bold;">Important: Action Required</div>
      <p>Dear {{ customer_name }},</p>
      <p>Your loan account <b>{{ loan_id }}</b> requires immediate attention.</p>
      <p><b>Risk Indicators</b></p><ul>{% for indicator in risk_indicators %}<li>{{ indicator }}</li>{% endfor %}</ul>
      <p><b>Resolution Options</b></p><ol>{% for option in resolution_options %}<li>{{ option }}</li>{% endfor %}</ol>
      <p>Assigned RM: {{ rm_name }} | {{ rm_email }} | {{ rm_phone }}</p>
      <p>Please respond by <b>{{ deadline_date }}</b>.</p>
      <p style="font-size:12px;color:#475569;">{{ legal_disclaimer }}</p>
    </body></html>
    """
)

LEGAL_TEMPLATE = Template(
    """
    <html><body style="font-family:Georgia,Arial,sans-serif;background:#fff;color:#111827;">
      <div style="background:#7f1d1d;color:#fff;padding:12px;font-weight:bold;">Final Notice - Immediate Action Required</div>
      <p>Dear {{ customer_name }},</p>
      <p>This follows prior interventions dated {{ engine1_date }} and {{ engine2_date }} for account <b>{{ loan_id }}</b>.</p>
      <table style="border-collapse:collapse;width:100%;">
        <tr><td>Principal</td><td>{{ principal_outstanding }}</td></tr>
        <tr><td>Interest</td><td>{{ accrued_interest }}</td></tr>
        <tr><td>Penalty</td><td>{{ penalty_charges }}</td></tr>
        <tr><td><b>Total Due</b></td><td><b>{{ total_due }}</b></td></tr>
      </table>
      <p>Final deadline: <b>{{ deadline_date }}</b> | Case File: {{ case_file_id }}</p>
      <p>Legal Contact: {{ legal_officer_name }} | {{ legal_email }} | {{ legal_phone }}</p>
      <p style="font-size:12px;color:#475569;">{{ legal_footer }}</p>
    </body></html>
    """
)


def _utcnow() -> datetime:
    return get_ist_now()


def _tier_from_score(score: float) -> tuple[str, int]:
    if score <= 40:
        return "LOW_RISK", 0
    if score <= 64:
        return "HIGH_RISK", 1
    if score <= 79:
        return "CRITICAL", 2
    return "VERY_CRITICAL", 3


def _customer_email(customer: CustomerProfile) -> str:
    forced_email = os.getenv("INTERVENTION_FORCE_TO_EMAIL", "").strip()
    if forced_email:
        return forced_email
    return customer.email or f"{customer.customer_id.lower()}@examplebank.local"


def _latest_audit_hash(db: Session) -> str | None:
    row = db.query(InterventionAuditLog).order_by(InterventionAuditLog.created_at.desc()).first()
    return row.event_hash if row else None


def _append_audit(db: Session, *, intervention_id: str | None, customer_id: str, action: str, actor: str, details: dict) -> None:
    prev_hash = _latest_audit_hash(db)
    payload = json.dumps(
        {
            "intervention_id": intervention_id,
            "customer_id": customer_id,
            "action": action,
            "actor": actor,
            "details": details,
            "prev_hash": prev_hash,
            "ts": _utcnow().isoformat(),
            "nonce": str(uuid.uuid4()),
        },
        sort_keys=True,
    )
    db.add(
        InterventionAuditLog(
            intervention_id=intervention_id,
            customer_id=customer_id,
            action=action,
            actor=actor,
            details_json=json.dumps(details),
            prev_hash=prev_hash,
            event_hash=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        )
    )


def _tip_bank(dti: float, util: float, dpd: int) -> list[str]:
    tips: list[str] = []
    if dti >= 0.5:
        tips.append("Consider debt consolidation to reduce monthly pressure.")
    if util >= 0.8:
        tips.append("Request a credit limit review to improve utilization.")
    if dpd > 0:
        tips.append("Enable standing instructions to avoid missed dues.")
    if not tips:
        tips = [
            "Track due dates weekly to preserve payment discipline.",
            "Build one EMI as emergency buffer.",
            "Contact your advisor early for temporary stress.",
        ]
    return tips[:3]


def _risk_indicators(score: float, dpd: int, dti: float, util: float) -> list[str]:
    indicators: list[str] = []
    if dpd > 0:
        indicators.append(f"Observed payment delay of {dpd} days.")
    if dti >= 0.5:
        indicators.append("Debt-to-income profile has increased.")
    if util > 0.8:
        indicators.append("Credit utilization is above 80%.")
    if score >= 65:
        indicators.append("Behavioral delinquency risk has increased.")
    return indicators


def _smtp_cfg() -> dict:
    smtp_user = os.getenv("SMTP_USER", "") or os.getenv("SMTP_USERNAME", "")
    smtp_host = os.getenv("SMTP_HOST", "") or os.getenv("SMTP_SERVER", "")
    smtp_from = os.getenv("SMTP_FROM_EMAIL", "") or smtp_user or "noreply@examplebank.local"
    return {
        "host": smtp_host,
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": smtp_user,
        "password": os.getenv("SMTP_PASSWORD", ""),
        "from_name": os.getenv("SMTP_FROM_NAME", "PIE Intervention Desk"),
        "from_email": smtp_from,
        "compliance_bcc": os.getenv("COMPLIANCE_BCC", "compliance@bank.local"),
    }


def _send_email(*, to_email: str, subject: str, html: str, bcc: list[str] | None = None) -> None:
    cfg = _smtp_cfg()
    if not cfg["host"]:
        raise RuntimeError("SMTP_HOST not configured")

    forced_email = os.getenv("INTERVENTION_FORCE_TO_EMAIL", "").strip()
    if forced_email:
        to_email = forced_email

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{cfg['from_name']} <{cfg['from_email']}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html"))

    recipients = [to_email, *(bcc or [])]

    if cfg["port"] == 465:
        with smtplib.SMTP_SSL(cfg["host"], cfg["port"]) as server:
            if cfg["user"]:
                server.login(cfg["user"], cfg["password"])
            server.sendmail(cfg["from_email"], recipients, msg.as_string())
    else:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
            server.starttls()
            if cfg["user"]:
                server.login(cfg["user"], cfg["password"])
            server.sendmail(cfg["from_email"], recipients, msg.as_string())


def _render(engine_tier: int, variables: dict) -> str:
    if engine_tier == 1:
        return ADVISORY_TEMPLATE.render(**variables)
    if engine_tier == 2:
        return ALERT_TEMPLATE.render(**variables)
    return LEGAL_TEMPLATE.render(**variables)


def _subject(engine_tier: int, loan_id: str) -> str:
    if engine_tier == 1:
        return f"A Quick Note on Your Loan Account {loan_id}"
    if engine_tier == 2:
        return f"Important: Action Required on Your Loan Account {loan_id}"
    return f"Final Notice - Loan Account {loan_id} | Immediate Action Required"


def _build_variables(db: Session, customer: CustomerProfile, score: float, engine_tier: int, case_file_id: str | None) -> dict:
    loan_id = customer.customer_id
    emi_amount = round(max(2000.0, (customer.loan_amount or 100000) / 24), 2)
    due_date = (_utcnow() + timedelta(days=7)).date().isoformat()
    payment_streak = max(1, int((customer.account_age_months or 12) / 6))

    tx = (
        db.query(CustomerTransaction)
        .filter(CustomerTransaction.customer_id == customer.customer_id)
        .order_by(CustomerTransaction.transaction_time.desc())
        .limit(12)
        .all()
    )
    dpd = int(tx[0].days_since_last_payment) if tx else 0
    util = min(1.0, (score / 100.0) + 0.15)
    dti = min(0.95, (emi_amount / max((customer.monthly_income or 1.0), 1.0)) + (score / 250.0))

    return {
        "bank_name": os.getenv("BANK_NAME", "PIE Bank"),
        "customer_name": customer.name,
        "loan_id": loan_id,
        "emi_amount": f"INR {emi_amount:,.2f}",
        "due_date": due_date,
        "payment_streak": payment_streak,
        "tips_list": _tip_bank(dti=dti, util=util, dpd=dpd),
        "booking_link": os.getenv("BOOKING_LINK", "https://bank.example.com/advisor"),
        "risk_indicators": _risk_indicators(score, dpd, dti, util),
        "resolution_options": [
            "EMI Restructuring",
            "Moratorium Request",
            "Partial Payment Plan",
            "Visit Branch",
        ],
        "rm_name": customer.relationship_manager,
        "rm_email": customer.rm_email or "rm@bank.local",
        "rm_phone": customer.rm_phone or "+91-00000-00000",
        "deadline_date": (_utcnow() + timedelta(days=7 if engine_tier == 2 else 14)).date().isoformat(),
        "legal_disclaimer": os.getenv("ALERT_LEGAL_DISCLAIMER", "Official bank notice."),
        "engine1_date": (_utcnow() - timedelta(days=16)).date().isoformat(),
        "engine2_date": (_utcnow() - timedelta(days=8)).date().isoformat(),
        "principal_outstanding": f"INR {(customer.loan_amount or 0.0) * 0.2:,.2f}",
        "accrued_interest": f"INR {(customer.loan_amount or 0.0) * 0.015:,.2f}",
        "penalty_charges": f"INR {(customer.loan_amount or 0.0) * 0.005:,.2f}",
        "total_due": f"INR {(customer.loan_amount or 0.0) * 0.22:,.2f}",
        "legal_officer_name": os.getenv("LEGAL_OFFICER_NAME", "Collections Legal Desk"),
        "legal_email": os.getenv("LEGAL_EMAIL", "legal@bank.local"),
        "legal_phone": os.getenv("LEGAL_PHONE", "+91-11111-11111"),
        "legal_footer": os.getenv("LEGAL_FOOTER", "As per applicable banking regulations."),
        "case_file_id": case_file_id or "N/A",
    }


def _generate_case_file(db: Session, customer: CustomerProfile, queue_row: InterventionQueue, actor: str) -> tuple[str, str]:
    CASE_FILES_DIR.mkdir(parents=True, exist_ok=True)
    case_id = f"PIE-CASE-{_utcnow().strftime('%Y%m%d')}-{customer.customer_id}"
    case_path = CASE_FILES_DIR / f"{case_id}.pdf"

    tx_count = (
        db.query(func.count(CustomerTransaction.id))
        .filter(CustomerTransaction.customer_id == customer.customer_id)
        .scalar()
        or 0
    )

    pdf = canvas.Canvas(str(case_path), pagesize=A4)
    y = 800
    pdf.setFont("Helvetica-Bold", 13)
    pdf.drawString(40, y, "PIE Legal Escalation Case File")
    y -= 24
    pdf.setFont("Helvetica", 10)
    for line in [
        f"Case File: {case_id}",
        f"Customer: {customer.customer_id} | {customer.name}",
        f"Loan Type: {customer.loan_type}",
        f"Risk Score: {queue_row.risk_score}",
        f"Transaction Records: {tx_count}",
        f"Generated by: {actor}",
        f"Generated at: {_utcnow().isoformat()}",
    ]:
        pdf.drawString(40, y, line)
        y -= 18
    pdf.showPage()
    pdf.save()
    return case_id, str(case_path)


def _no_duplicate_recent(db: Session, customer_id: str, engine_tier: int) -> bool:
    since = _utcnow() - timedelta(days=30)
    row = (
        db.query(InterventionQueue)
        .filter(
            InterventionQueue.customer_id == customer_id,
            InterventionQueue.engine_tier == engine_tier,
            InterventionQueue.sent_at.isnot(None),
            InterventionQueue.sent_at >= since,
        )
        .first()
    )
    return row is None


def _create_queue_item(db: Session, customer: CustomerProfile, score: float, engine_tier: int, actor: str) -> InterventionQueue | None:
    tier_label, _ = _tier_from_score(score)

    if engine_tier == 0:
        _append_audit(
            db,
            intervention_id=None,
            customer_id=customer.customer_id,
            action="PASSIVE_MONITORING",
            actor=actor,
            details={"score": score, "tier": tier_label},
        )
        return None

    if not _no_duplicate_recent(db, customer.customer_id, engine_tier):
        return None

    existing_active = (
        db.query(InterventionQueue)
        .filter(
            InterventionQueue.customer_id == customer.customer_id,
            InterventionQueue.status.in_(tuple(ACTIVE_STATUSES)),
            InterventionQueue.engine_tier >= engine_tier,
        )
        .first()
    )
    if existing_active:
        return None

    status = "PENDING" if engine_tier in {1, 2} else "AWAITING_CHECKER"
    row = InterventionQueue(
        customer_id=customer.customer_id,
        risk_score=score,
        tier_label=tier_label,
        engine_tier=engine_tier,
        status=status,
        delivery_status="QUEUED",
        maker_id=actor if engine_tier == 3 else None,
        rm_escalation_flag=engine_tier >= 2,
        collections_flag=engine_tier == 3,
        response_due_at=_utcnow() + timedelta(hours=ENGINE_SLA_HOURS[engine_tier]),
    )
    db.add(row)
    db.flush()

    _append_audit(
        db,
        intervention_id=row.id,
        customer_id=row.customer_id,
        action="QUEUE_CREATED",
        actor=actor,
        details={"engine_tier": engine_tier, "score": score, "status": status},
    )
    return row


def orchestrate_from_latest_scores(db: Session, actor: str = "orchestrator") -> dict:
    ordered_scores = (
        db.query(RiskScore)
        .order_by(RiskScore.customer_id.asc(), RiskScore.created_at.desc(), RiskScore.id.desc())
        .all()
    )

    latest_scores_map: dict[str, RiskScore] = {}
    for score_row in ordered_scores:
        if score_row.customer_id in latest_scores_map:
            continue
        latest_scores_map[score_row.customer_id] = score_row

    latest_scores = list(latest_scores_map.values())

    created = 0
    passive = 0
    for row in latest_scores:
        customer = db.get(CustomerProfile, row.customer_id)
        if not customer:
            continue
        _, engine_tier = _tier_from_score(float(row.risk_score))
        queue_item = _create_queue_item(db, customer, float(row.risk_score), engine_tier, actor)
        if queue_item is None:
            if engine_tier == 0:
                passive += 1
            continue
        created += 1

    db.commit()
    return {"scanned": len(latest_scores), "created": created, "passive": passive}


def list_queue(db: Session, status: str | None = None) -> list[dict]:
    q = db.query(InterventionQueue).order_by(InterventionQueue.created_at.desc())
    if status:
        q = q.filter(InterventionQueue.status == status.upper())

    result = []
    for row in q.limit(500).all():
        customer = db.get(CustomerProfile, row.customer_id)
        result.append(
            {
                "id": row.id,
                "customer_id": row.customer_id,
                "customer_name": customer.name if customer else row.customer_id,
                "engine_tier": row.engine_tier,
                "tier_label": row.tier_label,
                "risk_score": row.risk_score,
                "status": row.status,
                "delivery_status": row.delivery_status,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "sent_at": row.sent_at.isoformat() if row.sent_at else None,
                "response_due_at": row.response_due_at.isoformat() if row.response_due_at else None,
                "rm_escalation_flag": row.rm_escalation_flag,
                "collections_flag": row.collections_flag,
            }
        )
    return result


def preview_intervention(db: Session, intervention_id: str) -> dict:
    row = db.get(InterventionQueue, intervention_id)
    if not row:
        raise ValueError("Intervention not found")
    customer = db.get(CustomerProfile, row.customer_id)
    if not customer:
        raise ValueError("Customer not found")

    case_file_id = row.case_file_id
    if row.engine_tier == 3 and not case_file_id:
        case_file_id = f"PIE-CASE-{_utcnow().strftime('%Y%m%d')}-{customer.customer_id}"

    variables = _build_variables(db, customer, row.risk_score, row.engine_tier, case_file_id)
    subject = _subject(row.engine_tier, variables["loan_id"])
    html = _render(row.engine_tier, variables)

    return {
        "intervention_id": row.id,
        "customer_id": row.customer_id,
        "engine_tier": row.engine_tier,
        "subject": subject,
        "variables": variables,
        "html": html,
        "requires_dual_approval": row.engine_tier == 3,
        "case_file_id": case_file_id,
    }


def _send_with_retry(to_email: str, subject: str, html: str, bcc: list[str] | None) -> tuple[bool, str | None, int]:
    delays = [0, 2, 4]
    last_error = None
    for attempt, delay in enumerate(delays, start=1):
        if delay:
            time.sleep(delay)
        try:
            _send_email(to_email=to_email, subject=subject, html=html, bcc=bcc)
            return True, None, attempt - 1
        except Exception as exc:
            last_error = str(exc)
    return False, last_error, len(delays)


def _send_job(intervention_id: str, actor: str) -> None:
    with SessionLocal() as db:
        row = db.get(InterventionQueue, intervention_id)
        if not row or row.status in TERMINAL_STATUSES:
            return
        _execute_send(db, row, actor=actor, schedule_at=None)


def _execute_send(db: Session, row: InterventionQueue, actor: str, schedule_at: datetime | None) -> dict:
    customer = db.get(CustomerProfile, row.customer_id)
    if not customer:
        raise ValueError("Customer not found")

    if row.engine_tier == 2 and not customer.rm_email:
        customer.rm_email = os.getenv("DEFAULT_RM_EMAIL", "rm@bank.local")

    if schedule_at and schedule_at > _utcnow():
        row.status = "QUEUED"
        row.scheduled_at = schedule_at
        db.commit()
        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=row.customer_id,
            action="SCHEDULED",
            actor=actor,
            details={"run_at": schedule_at.isoformat()},
        )
        db.commit()
        INTERVENTION_SCHEDULER.add_job(
            _send_job,
            "date",
            run_date=schedule_at,
            args=[row.id, actor],
            id=f"intervention-send-{row.id}",
            replace_existing=True,
        )
        return {"status": "QUEUED", "scheduled_at": schedule_at.isoformat()}

    if row.engine_tier == 3 and not row.case_file_path:
        case_id, case_path = _generate_case_file(db, customer, row, actor)
        row.case_file_id = case_id
        row.case_file_path = case_path

    variables = _build_variables(db, customer, row.risk_score, row.engine_tier, row.case_file_id)
    subject = _subject(row.engine_tier, variables["loan_id"])
    html = _render(row.engine_tier, variables)

    bcc = None
    if row.engine_tier == 3:
        compliance = _smtp_cfg().get("compliance_bcc")
        bcc = [compliance] if compliance else None

    ok, err, retries = _send_with_retry(_customer_email(customer), subject, html, bcc)

    row.email_subject = subject
    row.email_html = html
    row.template_variables_json = json.dumps(variables)
    row.retry_count = retries
    row.sent_at = _utcnow()

    if ok:
        row.status = "SENT"
        row.delivery_status = "SENT"
        customer.intervention_status = {1: "ADVISORY_SENT", 2: "CRITICAL_ALERT_SENT", 3: "LEGAL_ESCALATION_SENT"}[row.engine_tier]
        if row.engine_tier == 3:
            customer.pre_npa = True
        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=row.customer_id,
            action="SENT",
            actor=actor,
            details={"subject": subject, "engine_tier": row.engine_tier},
        )
    else:
        row.status = "FAILED"
        row.delivery_status = "FAILED"
        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=row.customer_id,
            action="SEND_FAILED",
            actor=actor,
            details={"error": err, "engine_tier": row.engine_tier},
        )

    db.commit()
    return {"status": row.status, "delivery_status": row.delivery_status, "retry_count": retries, "error": err}


def approve_intervention(
    db: Session,
    intervention_id: str,
    admin_id: str,
    checker_id: str | None = None,
    schedule_at: datetime | None = None,
    comment: str | None = None,
) -> dict:
    row = db.get(InterventionQueue, intervention_id)
    if not row:
        raise ValueError("Intervention not found")

    if row.engine_tier == 3:
        if not row.maker_id:
            row.maker_id = admin_id
            row.status = "AWAITING_CHECKER"
            _append_audit(
                db,
                intervention_id=row.id,
                customer_id=row.customer_id,
                action="MAKER_SUBMITTED",
                actor=admin_id,
                details={"comment": comment or ""},
            )
            db.commit()
            return {"status": "AWAITING_CHECKER"}

        if not checker_id:
            raise ValueError("checker_id required for Engine 3 approval")
        if checker_id == row.maker_id:
            raise ValueError("Maker and checker must be different")

        row.checker_id = checker_id
        row.approved_by = checker_id
        row.approved_at = _utcnow()
        row.approval_comment = comment
        row.status = "APPROVED"
        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=row.customer_id,
            action="CHECKER_APPROVED",
            actor=checker_id,
            details={"comment": comment or ""},
        )
        db.commit()
        return _execute_send(db, row, actor=checker_id, schedule_at=schedule_at)

    row.approved_by = admin_id
    row.approved_at = _utcnow()
    row.approval_comment = comment
    row.status = "APPROVED"
    _append_audit(
        db,
        intervention_id=row.id,
        customer_id=row.customer_id,
        action="APPROVED",
        actor=admin_id,
        details={"comment": comment or ""},
    )
    db.commit()
    return _execute_send(db, row, actor=admin_id, schedule_at=schedule_at)


def reject_intervention(db: Session, intervention_id: str, admin_id: str, reason: str) -> dict:
    row = db.get(InterventionQueue, intervention_id)
    if not row:
        raise ValueError("Intervention not found")

    row.status = "REJECTED"
    row.delivery_status = "CANCELLED"
    row.approval_comment = reason
    _append_audit(
        db,
        intervention_id=row.id,
        customer_id=row.customer_id,
        action="REJECTED",
        actor=admin_id,
        details={"reason": reason},
    )
    db.commit()
    return {"status": "REJECTED"}


def send_test_email(db: Session, intervention_id: str, admin_email: str, admin_id: str) -> dict:
    preview = preview_intervention(db, intervention_id)
    _send_email(to_email=admin_email, subject=f"[TEST] {preview['subject']}", html=preview["html"], bcc=None)
    _append_audit(
        db,
        intervention_id=intervention_id,
        customer_id=preview["customer_id"],
        action="TEST_EMAIL_SENT",
        actor=admin_id,
        details={"admin_email": admin_email},
    )
    db.commit()
    return {"status": "TEST_SENT"}


def get_history(db: Session, engine_tier: int | None = None, status: str | None = None) -> list[dict]:
    q = db.query(InterventionQueue).order_by(InterventionQueue.created_at.desc())
    if engine_tier:
        q = q.filter(InterventionQueue.engine_tier == engine_tier)
    if status:
        q = q.filter(InterventionQueue.status == status.upper())

    return [
        {
            "id": row.id,
            "customer_id": row.customer_id,
            "engine_tier": row.engine_tier,
            "tier_label": row.tier_label,
            "risk_score": row.risk_score,
            "status": row.status,
            "delivery_status": row.delivery_status,
            "approved_by": row.approved_by,
            "maker_id": row.maker_id,
            "checker_id": row.checker_id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "sent_at": row.sent_at.isoformat() if row.sent_at else None,
            "retry_count": row.retry_count,
            "case_file_path": row.case_file_path,
            "subject": row.email_subject,
        }
        for row in q.limit(1000).all()
    ]


def start_intervention_scheduler() -> None:
    if not INTERVENTION_SCHEDULER.running:
        INTERVENTION_SCHEDULER.start()

    if not INTERVENTION_SCHEDULER.get_job("intervention-orchestrator"):
        interval_min = int(os.getenv("INTERVENTION_ORCHESTRATOR_INTERVAL_MINUTES", "10"))
        INTERVENTION_SCHEDULER.add_job(
            _orchestrator_job,
            "interval",
            minutes=max(1, interval_min),
            id="intervention-orchestrator",
            replace_existing=True,
        )


def stop_intervention_scheduler() -> None:
    if INTERVENTION_SCHEDULER.running:
        INTERVENTION_SCHEDULER.shutdown(wait=False)


def auto_escalate_critical_customer(customer_id: str, risk_score: float, actor: str = "auto_escalation") -> dict | None:
    """Auto-escalate a customer whose risk exceeds 80% after a periodic risk refresh.

    Creates an intervention queue entry and immediately dispatches the email
    without requiring manual admin approval. Only fires for CRITICAL and
    VERY_CRITICAL buckets (score >= 80).
    """
    if risk_score < 80.0:
        return None

    with SessionLocal() as db:
        customer = db.get(CustomerProfile, customer_id)
        if not customer:
            print(f"[AUTO_ESCALATE] Customer {customer_id} not found - skipping")
            return None

        tier_label, engine_tier = _tier_from_score(risk_score)
        if engine_tier < 2:
            # Only escalate for CRITICAL (tier 2) and VERY_CRITICAL (tier 3)
            return None

        # Check for recent duplicate to avoid spamming the same customer
        if not _no_duplicate_recent(db, customer_id, engine_tier):
            print(f"[AUTO_ESCALATE] {customer_id} already escalated recently (tier {engine_tier})")
            return None

        # Create intervention queue item
        status = "APPROVED"  # auto-approved for immediate dispatch
        row = InterventionQueue(
            customer_id=customer_id,
            risk_score=risk_score,
            tier_label=tier_label,
            engine_tier=engine_tier,
            status=status,
            delivery_status="QUEUED",
            approved_by=actor,
            approved_at=_utcnow(),
            maker_id=actor,
            rm_escalation_flag=True,
            collections_flag=engine_tier == 3,
            response_due_at=_utcnow() + timedelta(hours=ENGINE_SLA_HOURS[engine_tier]),
        )
        db.add(row)
        db.flush()

        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=customer_id,
            action="AUTO_ESCALATION_CREATED",
            actor=actor,
            details={"score": risk_score, "tier": tier_label, "engine_tier": engine_tier},
        )
        db.commit()

        # Immediately dispatch the email
        try:
            result = _execute_send(db, row, actor=actor, schedule_at=None)
            print(
                f"[AUTO_ESCALATE] {customer_id} | Score: {risk_score:.2f} | "
                f"Tier: {tier_label} | Engine: {engine_tier} | Status: {result.get('status')}"
            )
            return {
                "customer_id": customer_id,
                "risk_score": risk_score,
                "tier": tier_label,
                "engine_tier": engine_tier,
                "intervention_id": row.id,
                "send_result": result,
            }
        except Exception as exc:
            print(f"[AUTO_ESCALATE] Email send failed for {customer_id}: {exc}")
            return {
                "customer_id": customer_id,
                "risk_score": risk_score,
                "error": str(exc),
            }


def send_manual_intervention_email(
    customer_id: str,
    admin_id: str = "admin",
    engine_tier: int | None = None,
) -> dict:
    """Admin-triggered manual email to a specific customer.

    If no engine_tier is provided, it is determined from the customer's latest
    risk score in the database.
    """
    with SessionLocal() as db:
        customer = db.get(CustomerProfile, customer_id)
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        # Get latest risk score
        latest_score_row = (
            db.query(RiskScore)
            .filter(RiskScore.customer_id == customer_id)
            .order_by(RiskScore.created_at.desc())
            .first()
        )
        score = float(latest_score_row.risk_score) if latest_score_row else 50.0

        if engine_tier is None:
            _, engine_tier = _tier_from_score(score)
            engine_tier = max(1, engine_tier)  # at least tier 1 (advisory)

        tier_label, _ = _tier_from_score(score)

        row = InterventionQueue(
            customer_id=customer_id,
            risk_score=score,
            tier_label=tier_label,
            engine_tier=engine_tier,
            status="APPROVED",
            delivery_status="QUEUED",
            approved_by=admin_id,
            approved_at=_utcnow(),
            maker_id=admin_id,
            rm_escalation_flag=engine_tier >= 2,
            collections_flag=engine_tier == 3,
            response_due_at=_utcnow() + timedelta(hours=ENGINE_SLA_HOURS.get(engine_tier, 48)),
        )
        db.add(row)
        db.flush()

        _append_audit(
            db,
            intervention_id=row.id,
            customer_id=customer_id,
            action="MANUAL_SEND_CREATED",
            actor=admin_id,
            details={"score": score, "tier": tier_label, "engine_tier": engine_tier},
        )
        db.commit()

        result = _execute_send(db, row, actor=admin_id, schedule_at=None)
        return {
            "customer_id": customer_id,
            "risk_score": score,
            "engine_tier": engine_tier,
            "intervention_id": row.id,
            "send_result": result,
        }


def _orchestrator_job() -> None:
    with SessionLocal() as db:
        orchestrate_from_latest_scores(db, actor="orchestrator")
