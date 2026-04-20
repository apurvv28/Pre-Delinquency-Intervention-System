import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np
import pandas as pd
from scipy.spatial.distance import jensenshannon
from scipy.stats import ks_2samp
from sqlalchemy.orm import Session

from backend.database import CustomerTransaction, DriftLog, ModelAuditLog, ModelRegistry, RetrainJob
from backend.predict import activate_model_artifacts
from backend.timezone_util import get_ist_now

BACKEND_DIR = Path(__file__).resolve().parent
DATA_DIR = BACKEND_DIR / "data"
MODELS_DIR = BACKEND_DIR / "models"
BASELINE_PATH = DATA_DIR / "drift_baseline.json"
RETRAIN_CONFIG_PATH = DATA_DIR / "retrain_config.json"

DRIFT_WARN_THRESHOLD = float(os.getenv("PIE_DRIFT_WARN_THRESHOLD", "0.5"))
DRIFT_RETRAIN_THRESHOLD = float(os.getenv("PIE_DRIFT_RETRAIN_THRESHOLD", "0.8"))


def _safe_read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def _safe_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def get_retrain_config() -> dict:
    return {
        "colab_ngrok_url": "",
        "bank_threshold": 0.3,
        "scheduled_hour": 2,
        **_safe_read_json(RETRAIN_CONFIG_PATH, {}),
    }


def update_retrain_config(*, colab_ngrok_url: str | None = None, bank_threshold: float | None = None) -> dict:
    config = get_retrain_config()
    if colab_ngrok_url is not None:
        config["colab_ngrok_url"] = colab_ngrok_url.strip().rstrip("/")
    if bank_threshold is not None:
        config["bank_threshold"] = float(bank_threshold)
    _safe_write_json(RETRAIN_CONFIG_PATH, config)
    return config


def _feature_frame(rows: list[CustomerTransaction]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(
        [
            {
                "amount": float(row.amount or 0),
                "balance_after": float(row.balance_after or 0),
                "days_since_last_payment": float(row.days_since_last_payment or 0),
                "previous_declines_24h": float(row.previous_declines_24h or 0),
                "risk_score": float(row.risk_score or 0),
            }
            for row in rows
        ]
    )
    return frame.replace([np.inf, -np.inf], np.nan)


def _compute_psi(expected: pd.Series, actual: pd.Series, buckets: int = 10) -> float:
    expected = expected.replace([np.inf, -np.inf], np.nan).dropna()
    actual = actual.replace([np.inf, -np.inf], np.nan).dropna()
    if expected.empty or actual.empty:
        return 0.0

    boundaries = np.unique(np.quantile(expected, np.linspace(0.0, 1.0, buckets + 1)))
    if len(boundaries) < 3:
        return 0.0

    e_bins = pd.cut(expected, bins=boundaries, include_lowest=True, duplicates="drop")
    a_bins = pd.cut(actual, bins=boundaries, include_lowest=True, duplicates="drop")

    e_pct = e_bins.value_counts(normalize=True).sort_index().clip(lower=1e-6)
    a_pct = a_bins.value_counts(normalize=True).reindex(e_pct.index, fill_value=0.0).clip(lower=1e-6)

    return float(((a_pct - e_pct) * np.log(a_pct / e_pct)).sum())


def _data_quality_score(frame: pd.DataFrame) -> tuple[float, dict]:
    if frame.empty:
        return 1.0, {"nullRate": 0.0, "outOfRangeRate": 0.0}

    total_values = frame.shape[0] * frame.shape[1]
    null_rate = float(frame.isna().sum().sum() / max(total_values, 1))

    out_of_range_count = 0
    row_count = max(len(frame), 1)

    out_of_range_count += int((frame["amount"].fillna(0) < 0).sum())
    out_of_range_count += int((frame["balance_after"].fillna(0) < -1).sum())
    out_of_range_count += int((frame["days_since_last_payment"].fillna(0) < 0).sum())
    out_of_range_count += int((frame["days_since_last_payment"].fillna(0) > 365).sum())
    out_of_range_count += int((frame["previous_declines_24h"].fillna(0) < 0).sum())
    out_of_range_count += int((frame["risk_score"].fillna(0) < 0).sum())
    out_of_range_count += int((frame["risk_score"].fillna(0) > 100).sum())

    out_of_range_rate = float(out_of_range_count / (row_count * 7))
    score = max(0.0, min(1.0, 1.0 - (0.7 * null_rate + 0.3 * out_of_range_rate)))

    return score, {
        "nullRate": round(null_rate, 4),
        "outOfRangeRate": round(out_of_range_rate, 4),
    }


def _normalize_psi(value: float) -> float:
    # PSI above 0.25 is generally considered significant drift.
    return max(0.0, min(1.0, value / 0.25))


def _prediction_js_divergence(baseline_scores: list[float], recent_scores: list[float]) -> float:
    if not baseline_scores or not recent_scores:
        return 0.0

    bins = np.linspace(0.0, 100.0, 11)
    b_hist, _ = np.histogram(np.array(baseline_scores), bins=bins)
    r_hist, _ = np.histogram(np.array(recent_scores), bins=bins)

    b_prob = (b_hist + 1e-6) / np.sum(b_hist + 1e-6)
    r_prob = (r_hist + 1e-6) / np.sum(r_hist + 1e-6)

    return float(jensenshannon(b_prob, r_prob) ** 2)


def _load_or_create_baseline(rows: list[CustomerTransaction]) -> dict:
    payload = _safe_read_json(BASELINE_PATH, {})
    if payload:
        return payload

    frame = _feature_frame(rows)
    if frame.empty:
        return {"createdAt": get_ist_now().isoformat(), "features": {}, "predictionScores": []}

    features = {
        column: frame[column].dropna().tolist()
        for column in frame.columns
    }
    baseline = {
        "createdAt": get_ist_now().isoformat(),
        "features": features,
        "predictionScores": frame["risk_score"].fillna(0).tolist(),
    }
    _safe_write_json(BASELINE_PATH, baseline)
    return baseline


def refresh_baseline_from_recent(rows: list[CustomerTransaction]) -> dict:
    frame = _feature_frame(rows)
    features = {
        column: frame[column].fillna(0).tolist()
        for column in frame.columns
    }
    baseline = {
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "features": features,
        "predictionScores": frame["risk_score"].fillna(0).tolist(),
    }
    _safe_write_json(BASELINE_PATH, baseline)
    return baseline


def compute_drift_report(db: Session) -> dict:
    rows = (
        db.query(CustomerTransaction)
        .order_by(CustomerTransaction.transaction_time.asc())
        .limit(6000)
        .all()
    )

    if len(rows) < 100:
        return {
            "compositeDriftScore": 0.0,
            "stability": "stable",
            "psiScore": 0.0,
            "ksScore": 0.0,
            "jsScore": 0.0,
            "dataQualityScore": 1.0,
            "featureBreakdown": [],
            "qualityBreakdown": {"nullRate": 0.0, "outOfRangeRate": 0.0},
            "baselineWindow": 0,
            "recentWindow": len(rows),
            "checkedAt": get_ist_now().isoformat(),
            "nextAction": "log_stable",
        }

    recent_window = min(1500, max(250, len(rows) // 2))
    recent_rows = rows[-recent_window:]

    baseline_payload = _load_or_create_baseline(rows[:-recent_window] or rows[:recent_window])
    baseline_features = baseline_payload.get("features", {})
    baseline_scores = baseline_payload.get("predictionScores", [])

    recent_frame = _feature_frame(recent_rows)
    quality_score, quality_breakdown = _data_quality_score(recent_frame)

    feature_breakdown: list[dict] = []
    psi_values: list[float] = []
    ks_values: list[float] = []

    for feature in ["amount", "balance_after", "days_since_last_payment", "previous_declines_24h", "risk_score"]:
        baseline_series = pd.Series(baseline_features.get(feature, []), dtype=float)
        recent_series = recent_frame[feature].fillna(0) if feature in recent_frame else pd.Series([], dtype=float)

        psi_raw = _compute_psi(baseline_series, recent_series)
        ks_raw = float(ks_2samp(baseline_series, recent_series).statistic) if not baseline_series.empty and not recent_series.empty else 0.0

        psi_values.append(_normalize_psi(psi_raw))
        ks_values.append(ks_raw)

        feature_breakdown.append(
            {
                "feature": feature,
                "psi": round(float(psi_raw), 4),
                "psiNormalized": round(_normalize_psi(psi_raw), 4),
                "ks": round(ks_raw, 4),
            }
        )

    js_score = _prediction_js_divergence(
        [float(v) for v in baseline_scores],
        recent_frame["risk_score"].fillna(0).tolist(),
    )

    psi_score = float(np.mean(psi_values)) if psi_values else 0.0
    ks_score = float(np.mean(ks_values)) if ks_values else 0.0
    data_quality_drift = 1.0 - quality_score

    composite = (0.40 * psi_score) + (0.25 * ks_score) + (0.25 * js_score) + (0.10 * data_quality_drift)
    composite = max(0.0, min(1.0, composite))

    if composite > DRIFT_RETRAIN_THRESHOLD:
        stability = "critical"
        next_action = "trigger_retraining"
    elif composite > DRIFT_WARN_THRESHOLD:
        stability = "warning"
        next_action = "raise_warning"
    else:
        stability = "stable"
        next_action = "log_stable"

    return {
        "compositeDriftScore": round(float(composite), 4),
        "stability": stability,
        "psiScore": round(float(psi_score), 4),
        "ksScore": round(float(ks_score), 4),
        "jsScore": round(float(js_score), 4),
        "dataQualityScore": round(float(quality_score), 4),
        "featureBreakdown": feature_breakdown,
        "qualityBreakdown": quality_breakdown,
        "baselineWindow": len(baseline_features.get("amount", [])),
        "recentWindow": len(recent_rows),
        "checkedAt": get_ist_now().isoformat(),
        "nextAction": next_action,
    }


def persist_drift_report(db: Session, report: dict, *, trigger_mode: str, triggered_retraining: bool = False) -> DriftLog:
    row = DriftLog(
        composite_score=float(report["compositeDriftScore"]),
        psi_score=float(report["psiScore"]),
        ks_score=float(report["ksScore"]),
        js_score=float(report["jsScore"]),
        data_quality_score=float(report["dataQualityScore"]),
        stability_label=str(report["stability"]),
        feature_breakdown_json=json.dumps(
            {
                "features": report.get("featureBreakdown", []),
                "quality": report.get("qualityBreakdown", {}),
            }
        ),
        triggered_retraining=bool(triggered_retraining),
        trigger_mode=trigger_mode,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def latest_drift_status(db: Session) -> dict:
    row = db.query(DriftLog).order_by(DriftLog.checked_at.desc()).first()
    if not row:
        return {
            "available": False,
            "status": "stable",
            "compositeDriftScore": 0.0,
            "checkedAt": None,
            "nextScheduledCheck": None,
        }

    breakdown = json.loads(row.feature_breakdown_json or "{}")
    return {
        "available": True,
        "status": row.stability_label,
        "compositeDriftScore": round(float(row.composite_score), 4),
        "psiScore": round(float(row.psi_score), 4),
        "ksScore": round(float(row.ks_score), 4),
        "jsScore": round(float(row.js_score), 4),
        "dataQualityScore": round(float(row.data_quality_score), 4),
        "featureBreakdown": breakdown.get("features", []),
        "qualityBreakdown": breakdown.get("quality", {}),
        "checkedAt": row.checked_at.isoformat() if row.checked_at else None,
        "triggeredRetraining": bool(row.triggered_retraining),
        "triggerMode": row.trigger_mode,
    }


def _colab_base_url() -> str:
    return get_retrain_config().get("colab_ngrok_url", "").rstrip("/")


def ping_colab_health() -> dict:
    base = _colab_base_url()
    if not base:
        return {"connected": False, "reason": "missing_colab_url"}
    try:
        with httpx.Client(timeout=8.0) as client:
            response = client.get(f"{base}/health")
            response.raise_for_status()
            payload = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        return {"connected": True, "url": base, "health": payload}
    except Exception as exc:
        return {"connected": False, "url": base, "reason": str(exc)}


def _audit(db: Session, *, action: str, actor: str, details: dict) -> None:
    db.add(
        ModelAuditLog(
            action=action,
            actor=actor,
            details_json=json.dumps(details),
        )
    )
    db.commit()


def trigger_colab_retraining(
    db: Session,
    *,
    trigger_type: str,
    triggered_by: str,
    drift_score: float | None,
) -> dict:
    health = ping_colab_health()
    if not health.get("connected"):
        raise RuntimeError("Colab endpoint unavailable. Update ngrok URL in settings and restart Colab notebook.")

    base = str(health["url"]).rstrip("/")
    payload = {
        "trigger_type": trigger_type,
        "drift_score": drift_score,
        "bank_threshold": get_retrain_config().get("bank_threshold", 0.3),
    }

    job = RetrainJob(
        status="pending",
        trigger_type=trigger_type,
        triggered_by=triggered_by,
        drift_score=drift_score,
        colab_url=base,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        with httpx.Client(timeout=20.0) as client:
            response = client.post(f"{base}/retrain", json={**payload, "job_id": job.job_id})
            response.raise_for_status()
            result = response.json()

        remote_status = str(result.get("status", "running")).strip().lower()
        if remote_status in {"failed", "error"}:
            remote_error = str(
                result.get("error")
                or result.get("message")
                or result.get("detail")
                or "Colab retraining failed"
            )
            job.status = "failed"
            job.response_json = json.dumps(result)
            job.completed_at = get_ist_now()
            db.commit()
            raise RuntimeError(f"Colab retraining failed: {remote_error}")

        job.status = remote_status or "running"
        job.response_json = json.dumps(result)
        job.updated_at = get_ist_now()
        db.commit()
        db.refresh(job)

        _audit(
            db,
            action="retrain_triggered",
            actor=triggered_by,
            details={
                "job_id": job.job_id,
                "trigger_type": trigger_type,
                "drift_score": drift_score,
                "colab_url": base,
            },
        )

        return {
            "jobId": job.job_id,
            "status": job.status,
            "colab": result,
        }
    except Exception as exc:
        job.status = "failed"
        job.response_json = json.dumps(
            {
                "status": "failed",
                "error": str(exc),
            }
        )
        job.completed_at = get_ist_now()
        db.commit()
        raise


def refresh_retraining_status(db: Session, *, job_id: str | None = None) -> dict:
    query = db.query(RetrainJob)
    if job_id:
        job = query.filter(RetrainJob.job_id == job_id).first()
    else:
        job = query.order_by(RetrainJob.created_at.desc()).first()

    if not job:
        return {"available": False, "status": "idle"}

    payload = json.loads(job.response_json) if job.response_json else {}

    if job.status in {"pending", "running"} and job.colab_url:
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(f"{job.colab_url.rstrip('/')}/status/{job.job_id}")
                response.raise_for_status()
                status_payload = response.json()
            job.status = str(status_payload.get("status", job.status))
            payload = status_payload
            job.response_json = json.dumps(status_payload)
            if job.status in {"done", "failed"}:
                job.completed_at = get_ist_now()
            db.commit()
            db.refresh(job)
        except Exception:
            pass

    return {
        "available": True,
        "jobId": job.job_id,
        "status": job.status,
        "triggerType": job.trigger_type,
        "triggeredBy": job.triggered_by,
        "driftScore": job.drift_score,
        "createdAt": job.created_at.isoformat() if job.created_at else None,
        "updatedAt": job.updated_at.isoformat() if job.updated_at else None,
        "completedAt": job.completed_at.isoformat() if job.completed_at else None,
        "response": payload,
    }


def _download_drive_file(file_id: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    with httpx.Client(timeout=60.0) as client:
        response = client.get(url)
        response.raise_for_status()
        destination.write_bytes(response.content)
    return destination


def list_model_versions(db: Session) -> list[dict]:
    rows = db.query(ModelRegistry).order_by(ModelRegistry.trained_at.desc(), ModelRegistry.version.desc()).all()
    result = []
    for row in rows:
        result.append(
            {
                "version": row.version,
                "trainedAt": row.trained_at.isoformat() if row.trained_at else None,
                "aucRoc": row.auc_roc,
                "gini": row.gini,
                "ksStat": row.ks_stat,
                "precision": row.precision,
                "recall": row.recall,
                "f1": row.f1,
                "driftScoreAtTrigger": row.drift_score_at_trigger,
                "driveFileId": row.drive_file_id,
                "status": row.status,
                "modelPath": row.model_path,
                "preprocessorPath": row.preprocessor_path,
                "metadata": json.loads(row.metadata_json) if row.metadata_json else {},
            }
        )
    return result


def ensure_model_registry_bootstrap(db: Session) -> None:
    production = db.query(ModelRegistry).filter(ModelRegistry.status == "production").first()
    if production:
        return

    version = "v1"
    existing = db.query(ModelRegistry).filter(ModelRegistry.version == version).first()
    if existing:
        existing.status = "production"
        db.commit()
        return

    row = ModelRegistry(
        version=version,
        auc_roc=0.0,
        gini=0.0,
        ks_stat=0.0,
        precision=0.0,
        recall=0.0,
        f1=0.0,
        status="production",
        metadata_json=json.dumps({"bootstrapped": True}),
    )
    db.add(row)
    db.commit()


def _next_model_version(db: Session) -> str:
    latest = db.query(ModelRegistry).order_by(ModelRegistry.trained_at.desc()).first()
    if not latest:
        return "v1"
    raw = str(latest.version).lower().replace("v", "")
    try:
        return f"v{int(raw) + 1}"
    except ValueError:
        return f"v{int(get_ist_now().timestamp())}"


def register_candidate_model(
    db: Session,
    *,
    metrics: dict,
    drift_score_at_trigger: float | None,
    drive_file_id: str,
    metadata: dict | None = None,
) -> dict:
    version = _next_model_version(db)
    candidate = ModelRegistry(
        version=version,
        auc_roc=float(metrics.get("auc_roc", 0.0)),
        gini=float(metrics.get("gini", 0.0)),
        ks_stat=float(metrics.get("ks_stat", 0.0)),
        precision=float(metrics.get("precision", 0.0)),
        recall=float(metrics.get("recall", 0.0)),
        f1=float(metrics.get("f1", 0.0)),
        drift_score_at_trigger=drift_score_at_trigger,
        drive_file_id=drive_file_id,
        status="candidate",
        metadata_json=json.dumps(metadata or {}),
    )
    db.add(candidate)
    db.commit()
    db.refresh(candidate)
    return {"version": candidate.version}


def activate_model(
    db: Session,
    *,
    actor: str,
    approver_primary: str,
    approver_secondary: str,
    version: str,
    drive_file_id: str,
    features_file_id: str | None = None,
    threshold_file_id: str | None = None,
    preprocessor_file_id: str | None = None,
) -> dict:
    target = db.query(ModelRegistry).filter(ModelRegistry.version == version).first()
    if not target:
        target = ModelRegistry(
            version=version,
            auc_roc=0.0,
            gini=0.0,
            ks_stat=0.0,
            status="candidate",
            drive_file_id=drive_file_id,
        )
        db.add(target)
        db.commit()
        db.refresh(target)

    model_path = MODELS_DIR / f"lightgbm_pie_{version}.pkl"
    _download_drive_file(drive_file_id, model_path)

    features_path = None
    threshold_path = None
    if features_file_id:
        features_path = MODELS_DIR / f"lightgbm_pie_{version}_features.pkl"
        _download_drive_file(features_file_id, features_path)
    if threshold_file_id:
        threshold_path = MODELS_DIR / f"lightgbm_pie_{version}_threshold.pkl"
        _download_drive_file(threshold_file_id, threshold_path)

    activated = activate_model_artifacts(
        model_path=str(model_path),
        features_path=str(features_path) if features_path else None,
        threshold_path=str(threshold_path) if threshold_path else None,
    )

    db.query(ModelRegistry).filter(ModelRegistry.status == "production").update({"status": "retired"})
    target.status = "production"
    target.model_path = str(model_path)
    target.preprocessor_path = preprocessor_file_id
    target.drive_file_id = drive_file_id
    db.commit()

    _audit(
        db,
        action="model_activated",
        actor=actor,
        details={
            "version": version,
            "approver_primary": approver_primary,
            "approver_secondary": approver_secondary,
            "drive_file_id": drive_file_id,
            "features_file_id": features_file_id,
            "threshold_file_id": threshold_file_id,
            "activated": activated,
        },
    )

    recent_rows = db.query(CustomerTransaction).order_by(CustomerTransaction.transaction_time.desc()).limit(1500).all()
    refresh_baseline_from_recent(list(reversed(recent_rows)))

    return {
        "status": "activated",
        "version": version,
        "artifacts": activated,
    }


def record_prediction_distribution(db: Session, days: int = 7) -> dict:
    rows = (
        db.query(CustomerTransaction)
        .order_by(CustomerTransaction.transaction_time.desc())
        .limit(max(500, days * 400))
        .all()
    )
    if not rows:
        return {"bins": [], "days": days}

    frame = _feature_frame(list(reversed(rows)))
    bins = [0, 20, 40, 60, 80, 100]
    labels = ["0-20", "20-40", "40-60", "60-80", "80-100"]
    frame["bucket"] = pd.cut(frame["risk_score"].fillna(0), bins=bins, labels=labels, include_lowest=True)
    grouped = frame.groupby("bucket", observed=False).size().reindex(labels, fill_value=0)

    return {
        "days": days,
        "bins": [{"bucket": bucket, "count": int(count)} for bucket, count in grouped.items()],
    }


def run_drift_check_and_optionally_trigger(db: Session, *, trigger_mode: str, actor: str) -> dict:
    report = compute_drift_report(db)
    should_trigger = report["compositeDriftScore"] > DRIFT_RETRAIN_THRESHOLD

    persist_drift_report(
        db,
        report,
        trigger_mode=trigger_mode,
        triggered_retraining=should_trigger,
    )

    retrain_result = None
    if should_trigger:
        try:
            retrain_result = trigger_colab_retraining(
                db,
                trigger_type="auto_drift",
                triggered_by=actor,
                drift_score=float(report["compositeDriftScore"]),
            )
        except Exception as exc:
            retrain_result = {"status": "failed", "error": str(exc)}

    return {
        "drift": report,
        "retraining": retrain_result,
    }
