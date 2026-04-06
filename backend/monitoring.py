import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from backend.database import CustomerTransaction
from backend.timezone_util import get_ist_now

BACKEND_DIR = Path(__file__).resolve().parent
DATA_DIR = BACKEND_DIR / "data"
MODEL_HISTORY_PATH = DATA_DIR / "model_metrics_history.json"
DRIFT_WARNING_THRESHOLD = float(os.getenv("MODEL_DRIFT_WARNING_THRESHOLD", "0.10"))
DRIFT_RETRAIN_THRESHOLD = float(os.getenv("MODEL_DRIFT_RETRAIN_THRESHOLD", "0.20"))


def _safe_read_json(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []
    return payload if isinstance(payload, list) else []


def load_model_history() -> list[dict]:
    history = _safe_read_json(MODEL_HISTORY_PATH)
    history.sort(key=lambda item: item.get("runAt", ""))
    return history


def save_model_history(history: list[dict]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with MODEL_HISTORY_PATH.open("w", encoding="utf-8") as handle:
        json.dump(history, handle, indent=2)


def append_model_history(entry: dict) -> dict:
    history = load_model_history()
    history.append(entry)
    save_model_history(history)
    return entry


def _psi(expected: pd.Series, actual: pd.Series, buckets: int = 10) -> float:
    expected = expected.replace([np.inf, -np.inf], np.nan).dropna()
    actual = actual.replace([np.inf, -np.inf], np.nan).dropna()
    if expected.empty or actual.empty:
        return 0.0

    quantiles = np.unique(np.quantile(expected, np.linspace(0.0, 1.0, buckets + 1)))
    if len(quantiles) < 3:
        return 0.0

    expected_bins = pd.cut(expected, bins=quantiles, include_lowest=True, duplicates="drop")
    actual_bins = pd.cut(actual, bins=quantiles, include_lowest=True, duplicates="drop")

    expected_pct = expected_bins.value_counts(normalize=True).sort_index()
    actual_pct = actual_bins.value_counts(normalize=True).reindex(expected_pct.index, fill_value=0.0)

    expected_pct = expected_pct.clip(lower=1e-6)
    actual_pct = actual_pct.clip(lower=1e-6)

    return float(((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)).sum())


def _build_feature_frame(rows: list[CustomerTransaction]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(
        [
            {
                "amount": row.amount,
                "balance_after": row.balance_after,
                "days_since_last_payment": row.days_since_last_payment,
                "previous_declines_24h": row.previous_declines_24h,
                "risk_score": row.risk_score if row.risk_score is not None else 0.0,
            }
            for row in rows
        ]
    )
    return frame.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def compute_drift_report(db: Session) -> dict:
    rows = db.query(CustomerTransaction).order_by(CustomerTransaction.transaction_time.asc()).all()
    if len(rows) < 50:
        return {
            "driftScore": 0.0,
            "status": "healthy",
            "shouldRetrain": False,
            "baselineWindow": 0,
            "recentWindow": len(rows),
            "featureDrift": [],
        }

    baseline_rows = rows[: min(250, max(50, len(rows) // 3))]
    recent_rows = rows[-min(250, max(50, len(rows) // 3)) :]

    baseline_frame = _build_feature_frame(baseline_rows)
    recent_frame = _build_feature_frame(recent_rows)
    drift_rows = []
    for column in ["amount", "balance_after", "days_since_last_payment", "previous_declines_24h", "risk_score"]:
        psi_value = _psi(baseline_frame[column], recent_frame[column]) if column in baseline_frame and column in recent_frame else 0.0
        drift_rows.append({"feature": column, "psi": round(float(psi_value), 4)})

    drift_score = float(np.mean([row["psi"] for row in drift_rows])) if drift_rows else 0.0
    if drift_score >= DRIFT_RETRAIN_THRESHOLD:
        status = "retrain"
    elif drift_score >= DRIFT_WARNING_THRESHOLD:
        status = "watch"
    else:
        status = "healthy"

    return {
        "driftScore": round(drift_score, 4),
        "status": status,
        "shouldRetrain": drift_score >= DRIFT_RETRAIN_THRESHOLD,
        "baselineWindow": len(baseline_rows),
        "recentWindow": len(recent_rows),
        "featureDrift": drift_rows,
    }


def build_monitoring_report(db: Session) -> dict:
    history = load_model_history()
    drift = compute_drift_report(db)
    latest_run = history[-1] if history else None
    latest_accuracy = float(latest_run.get("accuracy", 0.0)) if latest_run else 0.0
    retrain_recommended = bool(drift["shouldRetrain"] or latest_accuracy < 0.85)

    return {
        "status": "healthy" if not retrain_recommended and drift["status"] == "healthy" else "watch",
        "shouldRetrain": retrain_recommended,
        "latestRun": latest_run,
        "accuracyTimeline": history[-12:],
        "drift": drift,
        "historyCount": len(history),
        "generatedAt": get_ist_now().isoformat(),
    }