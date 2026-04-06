import json
import os
import pickle
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from lightgbm import LGBMClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

from backend.monitoring import append_model_history, build_monitoring_report, load_model_history
from backend.timezone_util import get_ist_now

BACKEND_DIR = Path(__file__).resolve().parent
REPO_DIR = BACKEND_DIR.parent
DATASET_PATH = BACKEND_DIR / "data" / "pie_sample_cleaned.csv"
ALT_DATASET_PATH = BACKEND_DIR / "data" / "pie_sample_multirow.csv"
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(BACKEND_DIR / "models" / "pie_lightgbm_model_v2.pkl")))
FEATURES_PATH = Path(os.getenv("FEATURES_PATH", str(BACKEND_DIR / "models" / "pie_feature_columns_v2.pkl")))
THRESHOLD_PATH = Path(os.getenv("THRESHOLD_PATH", str(BACKEND_DIR / "models" / "pie_threshold_v2.pkl")))
RETRAIN_INTERVAL_SECONDS = float(os.getenv("MODEL_RETRAIN_CHECK_INTERVAL_SECONDS", "900"))
RETRAIN_COOLDOWN_HOURS = float(os.getenv("MODEL_RETRAIN_COOLDOWN_HOURS", "24"))


def _candidate_dataset_paths() -> list[Path]:
    # Prefer the supervised cleaned dataset first; the multirow file is much larger
    # and can exceed memory when loaded as a full training frame.
    candidates = [DATASET_PATH, ALT_DATASET_PATH]
    return [path for path in candidates if path.exists()]


def _pick_dataset(required_features: list[str] | None = None) -> tuple[Path, list[str]]:
    required = set(required_features or [])
    best_path: Path | None = None
    best_cols: list[str] = []
    best_overlap = -1

    for path in _candidate_dataset_paths():
        cols = pd.read_csv(path, nrows=0).columns.tolist()
        if not required:
            return path, cols
        overlap = len(required.intersection(cols))
        if overlap > best_overlap:
            best_overlap = overlap
            best_path = path
            best_cols = cols

    if best_path is None:
        raise FileNotFoundError("No training dataset found under backend/data")

    return best_path, best_cols


def _load_dataset(required_features: list[str] | None = None) -> pd.DataFrame:
    dataset_path, columns = _pick_dataset(required_features)
    if not required_features:
        return pd.read_csv(dataset_path)

    required = [feature for feature in required_features if feature in columns]
    usecols = list(dict.fromkeys([*required, "target"]))
    if "target" not in usecols:
        raise ValueError(f"Dataset missing target column: {dataset_path}")
    return pd.read_csv(dataset_path, usecols=usecols)


def _current_artifacts():
    from backend import predict as predict_module

    return predict_module.model, predict_module.feature_columns, predict_module.threshold, predict_module.refresh_artifacts


def _prepare_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    target_column = "target"
    excluded = {"customer_ID", "S_2", target_column}
    feature_columns = [column for column in df.columns if column not in excluded]

    frame = df[feature_columns].replace([float("inf"), float("-inf")], 0).fillna(0)
    target = df[target_column].astype(int)
    return frame, target, feature_columns


def _align_to_feature_schema(df: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    aligned_map = {
        column: (df[column] if column in df.columns else 0.0)
        for column in feature_columns
    }
    aligned = pd.DataFrame(aligned_map, index=df.index)
    return aligned.replace([float("inf"), float("-inf")], 0).fillna(0)


def _optimal_threshold(y_true: pd.Series, probabilities: pd.Series) -> float:
    candidate_thresholds = [index / 100 for index in range(10, 91, 2)]
    best_threshold = 0.5
    best_score = -1.0
    for threshold in candidate_thresholds:
        predictions = (probabilities >= threshold).astype(int)
        score = f1_score(y_true, predictions, zero_division=0)
        if score > best_score:
            best_score = score
            best_threshold = threshold
    return float(best_threshold)


def _save_artifacts(model, feature_columns: list[str], threshold: float) -> None:
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as handle:
        pickle.dump(model, handle)
    with FEATURES_PATH.open("wb") as handle:
        pickle.dump(feature_columns, handle)
    with THRESHOLD_PATH.open("wb") as handle:
        pickle.dump(threshold, handle)


def _evaluate_model(model, X_val: pd.DataFrame, y_val: pd.Series, threshold: float) -> dict:
    probabilities = model.predict_proba(X_val)[:, 1]
    predictions = (probabilities >= threshold).astype(int)

    metrics = {
        "accuracy": round(float(accuracy_score(y_val, predictions)), 4),
        "precision": round(float(precision_score(y_val, predictions, zero_division=0)), 4),
        "recall": round(float(recall_score(y_val, predictions, zero_division=0)), 4),
        "f1": round(float(f1_score(y_val, predictions, zero_division=0)), 4),
        "auc": round(float(roc_auc_score(y_val, probabilities)), 4),
    }
    return metrics


def _build_history_entry(
    *,
    model_version: str,
    threshold: float,
    metrics: dict,
    trigger_reason: str,
    drift_score: float,
    dataset_rows: int,
) -> dict:
    return {
        "runAt": get_ist_now().isoformat(),
        "modelVersion": model_version,
        "threshold": round(float(threshold), 4),
        "accuracy": metrics["accuracy"],
        "precision": metrics["precision"],
        "recall": metrics["recall"],
        "f1": metrics["f1"],
        "auc": metrics["auc"],
        "driftScore": round(float(drift_score), 4),
        "triggerReason": trigger_reason,
        "datasetRows": dataset_rows,
    }


def evaluate_current_model_snapshot(trigger_reason: str = "baseline") -> dict:
    model, current_feature_columns, threshold, _ = _current_artifacts()
    df = _load_dataset(required_features=current_feature_columns)
    if "target" not in df.columns:
        raise ValueError("Selected dataset does not include target column")
    X = _align_to_feature_schema(df, current_feature_columns)
    y = df["target"].astype(int)
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    metrics = _evaluate_model(model, X_val, y_val, float(threshold))
    entry = _build_history_entry(
        model_version="current",
        threshold=float(threshold),
        metrics=metrics,
        trigger_reason=trigger_reason,
        drift_score=0.0,
        dataset_rows=len(df),
    )
    history = load_model_history()
    if not history:
        append_model_history(entry)
    return entry


def retrain_model(trigger_reason: str = "manual", drift_score: float = 0.0) -> dict:
    _, current_feature_columns, _, _ = _current_artifacts()

    # Recover from legacy/degenerate artifacts that learned on tiny or leakage-prone schemas.
    reset_schema = (not current_feature_columns) or ("risk_score" in current_feature_columns) or (len(current_feature_columns) < 20)

    if reset_schema:
        df = _load_dataset(required_features=None)
        if "target" not in df.columns:
            raise ValueError("Selected dataset does not include target column")
        X, y, feature_columns = _prepare_training_frame(df)
    else:
        df = _load_dataset(required_features=current_feature_columns)
        if "target" not in df.columns:
            raise ValueError("Selected dataset does not include target column")
        X = _align_to_feature_schema(df, current_feature_columns)
        y = df["target"].astype(int)
        feature_columns = current_feature_columns

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    model = LGBMClassifier(
        n_estimators=180,
        learning_rate=0.05,
        num_leaves=31,
        subsample=0.85,
        colsample_bytree=0.85,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    probabilities = model.predict_proba(X_val)[:, 1]
    threshold = _optimal_threshold(y_val, probabilities)
    metrics = _evaluate_model(model, X_val, y_val, threshold)

    _save_artifacts(model, feature_columns, threshold)

    from backend.predict import refresh_artifacts

    refresh_artifacts()

    model_version = f"retrained-{len(load_model_history()) + 1:03d}"
    entry = _build_history_entry(
        model_version=model_version,
        threshold=threshold,
        metrics=metrics,
        trigger_reason=trigger_reason,
        drift_score=drift_score,
        dataset_rows=len(df),
    )
    append_model_history(entry)
    return entry


def ensure_baseline_history() -> dict:
    history = load_model_history()
    if history:
        return history[-1]
    try:
        return evaluate_current_model_snapshot(trigger_reason="baseline")
    except Exception as exc:
        print(f"Baseline model snapshot skipped: {exc}")
        return {}


def maybe_retrain_from_monitoring(trigger_reason: str = "auto-drift") -> dict | None:
    from backend.database import SessionLocal

    history = load_model_history()
    if history:
        latest_run_at = history[-1].get("runAt")
        if latest_run_at:
            try:
                last_run = datetime.fromisoformat(latest_run_at.replace("Z", "+00:00"))
                if get_ist_now() - last_run < timedelta(hours=RETRAIN_COOLDOWN_HOURS):
                    return None
            except ValueError:
                pass

    with SessionLocal() as db:
        report = build_monitoring_report(db)

    if not report["shouldRetrain"]:
        return None

    return retrain_model(trigger_reason=trigger_reason, drift_score=float(report["drift"]["driftScore"]))


def run_retraining_scheduler(stop_event: threading.Event | None = None) -> None:
    while True:
        if stop_event and stop_event.is_set():
            break

        try:
            maybe_retrain_from_monitoring()
        except Exception as exc:
            print(f"Retraining scheduler skipped: {exc}")

        time.sleep(RETRAIN_INTERVAL_SECONDS)