import os
import pickle
import threading
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from backend.contextual_xgb import predict_contextual_risk

load_dotenv()

BACKEND_DIR = Path(__file__).resolve().parent
REPO_DIR = BACKEND_DIR.parent
model_swap_lock = threading.RLock()


def _resolve_artifact_path(env_name: str, default_name: str) -> Path:
    raw_value = os.getenv(env_name)
    if raw_value:
        candidate = Path(raw_value)
        if not candidate.is_absolute():
            repo_candidate = REPO_DIR / candidate
            backend_candidate = BACKEND_DIR / candidate
            for option in (candidate, repo_candidate, backend_candidate):
                if option.exists():
                    return option
        elif candidate.exists():
            return candidate

    backend_candidate = BACKEND_DIR / "models" / default_name
    repo_candidate = REPO_DIR / "models" / default_name
    if backend_candidate.exists():
        return backend_candidate
    if repo_candidate.exists():
        return repo_candidate

    return backend_candidate


active_model_path = _resolve_artifact_path("MODEL_PATH", "pie_lightgbm_model_v2.pkl")
active_features_path = _resolve_artifact_path("FEATURES_PATH", "pie_feature_columns_v2.pkl")
active_threshold_path = _resolve_artifact_path("THRESHOLD_PATH", "pie_threshold_v2.pkl")


def load_artifacts(
    model_path: Path | None = None,
    features_path: Path | None = None,
    threshold_path: Path | None = None,
):
    model_path = model_path or active_model_path
    with open(model_path, "rb") as file:
        model = pickle.load(file)

    features_path = features_path or active_features_path
    with open(features_path, "rb") as file:
        feature_columns = pickle.load(file)

    threshold_path = threshold_path or active_threshold_path
    with open(threshold_path, "rb") as file:
        threshold = pickle.load(file)

    return model, feature_columns, threshold


model, feature_columns, threshold = load_artifacts()
print(f"Model loaded | Features: {len(feature_columns)} | Threshold: {threshold}")


def refresh_artifacts():
    global model, feature_columns, threshold
    with model_swap_lock:
        model, feature_columns, threshold = load_artifacts()
        return model, feature_columns, threshold


def activate_model_artifacts(
    *,
    model_path: str,
    features_path: str | None = None,
    threshold_path: str | None = None,
):
    global active_model_path, active_features_path, active_threshold_path

    next_model_path = Path(model_path)
    next_features_path = Path(features_path) if features_path else active_features_path
    next_threshold_path = Path(threshold_path) if threshold_path else active_threshold_path

    # Load candidate artifacts before swapping globals so failed loads do not affect serving.
    next_model, next_features, next_threshold = load_artifacts(
        model_path=next_model_path,
        features_path=next_features_path,
        threshold_path=next_threshold_path,
    )

    with model_swap_lock:
        active_model_path = next_model_path
        active_features_path = next_features_path
        active_threshold_path = next_threshold_path
        globals()["model"] = next_model
        globals()["feature_columns"] = next_features
        globals()["threshold"] = next_threshold

    return {
        "model_path": str(active_model_path),
        "features_path": str(active_features_path),
        "threshold_path": str(active_threshold_path),
        "feature_count": len(feature_columns),
        "threshold": float(threshold),
    }


def get_risk_bucket(score: float) -> str:
    # Conservative portfolio calibration: keep most customers in normal unless risk is materially elevated.
    if score < 40:
        return "LOW_RISK"
    if score < 60:
        return "LOW_RISK"
    if score < 75:
        return "HIGH_RISK"
    if score < 99:
        return "CRITICAL"
    return "VERY_CRITICAL"


def _income_event_safe_cap(
    first_model_score: float,
    second_model_score: float | None,
) -> float:
    """Model-driven soft cap for INCOME_CREDIT events.

    Keeps income transactions safely in LOW_RISK while preserving realistic
    score spread instead of collapsing to a single fixed value.
    """
    context_score = second_model_score if second_model_score is not None else first_model_score
    blended = (0.65 * min(first_model_score, 100.0)) + (0.35 * min(context_score, 100.0))
    normalized = min(1.0, max(0.0, blended / 100.0))
    # Map to [18, 54] with slight curvature for smoother spread in low-risk band.
    return round(18.0 + ((normalized ** 0.85) * 36.0), 2)


def _unsafe_event_context_floor(first_model_score: float) -> float:
    """Lower bound for final score on non-safe events.

    Prevents contextual stage from collapsing very high baseline risk into
    low-risk bands unless the event type is explicitly whitelisted as safe.
    """
    if first_model_score >= 95.0:
        return max(80.0, first_model_score - 15.0)
    if first_model_score >= 90.0:
        return max(75.0, first_model_score - 18.0)
    if first_model_score >= 80.0:
        return max(68.0, first_model_score - 20.0)
    if first_model_score >= 70.0:
        return max(60.0, first_model_score - 18.0)
    return 0.0


def _should_apply_context_floor(raw_payload: dict) -> bool:
    """Only apply collapse protection when explicit high-risk context exists."""
    merchant = str(raw_payload.get("merchant_category") or raw_payload.get("transaction_reason") or "").strip().lower()
    risky_merchant = merchant in {
        "crypto",
        "gambling",
        "crypto exchange",
        "jewelry",
        "wire transfer",
        "cash advance",
        "luxury goods",
    }
    is_international = str(raw_payload.get("is_international", "false")).strip().lower() in {"true", "1", "yes"}
    days_late = int(raw_payload.get("days_since_last_payment", 0) or 0)
    declines = int(raw_payload.get("previous_declines_24h", 0) or 0)
    return bool(risky_merchant or is_international or days_late >= 25 or declines >= 2)


# ---------------------------------------------------------------------------
# Event types that represent EXPECTED, legitimate payments.
# For these events a large amount is contextually normal (EMI, salary credit).
# amount_pressure must NOT be used to inflate risk features for them.
# ---------------------------------------------------------------------------
_EXPECTED_PAYMENT_EVENTS = frozenset({
    "PAYMENT",
    "PARTIAL_PAYMENT",
    "INCOME_CREDIT",
    "SETTLEMENT_OFFER",
    "LOAN_CLOSED",
})

# For INCOME_CREDIT specifically, a large credit should also zero util_ratio
# (the customer is getting richer — this is good news).
_INCOME_EVENTS = frozenset({"INCOME_CREDIT"})


def calculate_features_from_transaction(tx: dict, history_context: dict | None = None) -> dict:
    if "P_2_last" in tx:
        return tx

    amt = float(tx.get("amount", 0))
    bal = float(tx.get("current_balance", 0) or (history_context or {}).get("latest_balance", 0) or (history_context or {}).get("avg_balance", 0))
    days_late = int(tx.get("days_since_last_payment", 0) or (history_context or {}).get("latest_dpd", 0) or (history_context or {}).get("avg_dpd", 0))
    declines = int(tx.get("previous_declines_24h", 0))
    merchant = str(tx.get("merchant_category") or tx.get("transaction_reason") or "")

    # Detect whether this is an expected/routine payment event
    event_type = str(tx.get("event_type", "")).upper()
    is_expected_payment = event_type in _EXPECTED_PAYMENT_EVENTS
    is_income_event = event_type in _INCOME_EVENTS

    # For INCOME_CREDIT the payload balance is pre-credit. The model should see
    # the post-credit balance so B_2 (balance/amount ratio) reflects the ACTUAL
    # financial state after the salary is deposited, not a distressed pre-deposit view.
    if is_income_event:
        bal = bal + amt

    risky_merchant = merchant.lower() in ["crypto", "gambling", "crypto exchange", "jewelry", "wire transfer", "cash advance"]
    is_international = str(tx.get("is_international", "false")).strip().lower() in {"true", "1", "yes"}

    if history_context:
        avg_amount = float(history_context.get("avg_amount", amt) or amt or 1.0)
        avg_dpd = float(history_context.get("avg_dpd", days_late) or days_late or 0.0)
        avg_balance = float(history_context.get("avg_balance", bal) or bal or 1.0)
        trend = float(history_context.get("trend", 0.0) or 0.0)
    else:
        avg_amount = float(max(250.0, min(5000.0, (bal * 0.08) if bal > 0 else (amt * 1.2))))
        avg_dpd = float(max(0.5, days_late * 0.6))
        avg_balance = float(max(1.0, (bal * 1.08) if bal > 0 else (amt * 2.0)))
        trend = 0.0

    util_ratio = min(1.0, amt / max(bal, 1.0)) if bal > 0 else min(1.0, amt / max(avg_balance, 1.0))
    pay_to_bal = max(0.0, 100.0 / (max(bal, 1.0) + 1.0))
    raw_amount_pressure = max(0.0, (amt - avg_amount) / max(avg_amount, 1.0))
    balance_pressure = max(0.0, (avg_balance - bal) / max(avg_balance, 1.0))
    delay_pressure = max(0.0, (days_late - avg_dpd) / 30.0)
    dpd_norm = min(1.0, days_late / 60.0)
    decline_norm = min(1.0, declines / 5.0)
    merchant_risk = 1.0 if risky_merchant else 0.0
    intl_risk = 1.0 if is_international else 0.0
    # ------------------------------------------------------------------
    # BUG-09 FIX: Honour rich behavioral signals from the stream producer
    # ------------------------------------------------------------------

    # 1. credit_utilization: directly overrides the proxy util_ratio when provided
    credit_util_raw = tx.get("credit_utilization")
    if credit_util_raw is not None:
        try:
            cu = float(credit_util_raw)
            if 0.0 <= cu <= 1.0:
                util_ratio = cu
        except (TypeError, ValueError):
            pass

    # 2. debt_to_income: adds to the burden signal
    dti_boost = 0.0
    dti_raw = tx.get("debt_to_income")
    if dti_raw is not None:
        try:
            dti = float(dti_raw)
            if 0.0 <= dti <= 1.0:
                dti_boost = dti * 0.30  # up to +0.30 on the burden feature
        except (TypeError, ValueError):
            pass

    # 3. payment_streak: sustained on-time payments improve payment health
    payment_streak = int(tx.get("payment_streak", 0) or 0)
    streak_health = min(0.12, payment_streak * 0.01)

    # 4. missed_payment_count (12-month window): stronger than 24h decline counter
    missed_12m = int(tx.get("missed_payment_count", declines) or declines)
    effective_declines = max(declines, min(missed_12m, 12))
    decline_norm = min(1.0, effective_declines / 5.0)

    # 5. num_active_loans: stressed borrowers open many loans simultaneously
    num_loans = int(tx.get("num_active_loans", 1) or 1)
    loan_stress = min(0.30, max(0.0, (num_loans - 2) * 0.07))

    # -----------------------------------------------------------------------
    # CONTEXT-AWARE AMOUNT PRESSURE
    # Suppression rules:
    #   • Expected payment events → amount_pressure = 0 (the size is intentional)
    #   • Income events           → also zero util_ratio (credit is a positive signal)
    #   • All other events        → keep raw_amount_pressure as-is
    # -----------------------------------------------------------------------
    if is_expected_payment:
        amount_pressure = 0.0          # do not punish large-but-routine payments
    else:
        amount_pressure = raw_amount_pressure

    if is_income_event:
        # A salary credit increases funds — it is purely positive context.
        # Zero all pressure signals that would otherwise inflate risk features.
        util_ratio = max(0.0, util_ratio - 0.15)  # salary lowers effective utilisation
        balance_pressure = 0.0                     # a credit is NOT a balance deterioration
        delay_pressure = 0.0                       # income events don't relate to DPD

    # ------------------------------------------------------------------
    # Build continuous signals (same names, richer inputs)
    # ------------------------------------------------------------------
    payment_health = (
        0.92
        - (0.25 * min(1.0, days_late / 45.0))
        - (0.08 * decline_norm)
        - (0.22 * util_ratio)
        + streak_health  # positive signal from sustained on-time payments
    )
    burden = (
        0.03
        + (0.55 * util_ratio)
        + (0.20 * amount_pressure)
        + (0.12 * delay_pressure)
        + dti_boost  # amplified by direct DTI signal
    )
    delinquency_signal = min(
        6.0,
        (days_late / 12.0) + (effective_declines * 0.6) + (max(0.0, trend) * 0.5),
    )
    fraud_signal = (
        (0.55 if is_international else 0.0)
        + (0.65 if risky_merchant else 0.0)
        + min(0.35, amount_pressure * 0.25)
        + loan_stress  # simultaneous loan openings as fraud-adjacent signal
    )

    feats = {
        # Map transaction behavior onto core AMEX-style feature columns expected by the trained model.
        "P_2": max(0.05, min(0.98, payment_health)),
        "B_1": max(0.02, min(2.5, burden)),
        "B_2": max(0.05, min(2.5, max(bal, 1.0) / max(amt, 1.0))),
        "D_39": max(0.0, min(10.0, delinquency_signal)),
        "D_41": max(0.0, min(4.0, fraud_signal)),
        "R_1": max(0.0, min(5.0, (effective_declines * 0.9) + (amount_pressure * 1.2))),
        "S_3": max(0.05, min(2.0, 0.15 + (0.35 if risky_merchant else 0.0) + min(0.7, amount_pressure * 0.4) + min(0.5, abs(trend) * 0.2))),
        "B_3": max(0.02, min(2.5, 0.08 + (0.75 * util_ratio) + (0.25 * balance_pressure))),
        "B_4": max(0.02, min(2.5, 0.06 + (0.85 * amount_pressure) + (0.25 * util_ratio))),
        "B_5": max(0.02, min(2.5, 0.04 + (0.70 * decline_norm) + (0.55 * dpd_norm))),
        "D_42": max(0.0, min(10.0, (2.0 * dpd_norm) + (0.8 * max(0.0, trend)))),
        "D_43": max(0.0, min(10.0, (1.9 * amount_pressure) + (0.8 * balance_pressure))),
        "D_45": max(0.0, min(10.0, (1.2 * dpd_norm) + (0.9 * decline_norm))),
        "D_46": max(0.0, min(10.0, (1.3 * util_ratio) + (0.9 * amount_pressure))),
        "D_47": max(0.0, min(10.0, (1.4 * merchant_risk) + (0.8 * intl_risk) + (0.5 * amount_pressure))),
        "R_3": max(0.0, min(5.0, (0.85 * decline_norm) + (0.8 * amount_pressure))),
        "P_3": max(0.05, min(0.98, 0.90 - (0.22 * dpd_norm) - (0.14 * decline_norm) - (0.08 * util_ratio))),
        "D_51": max(0.0, min(10.0, (2.2 * balance_pressure) + (1.1 * decline_norm))),
        "S_24": max(0.0, min(5.0, 0.2 + (0.95 * amount_pressure) + (0.45 * merchant_risk))),
        "S_7": max(0.0, min(5.0, 0.2 + (0.55 * util_ratio) + (0.45 * max(0.0, trend)) + (0.25 * decline_norm))),
        # Engineered helpers for internal adjustment logic and forward compatibility.
        "util_ratio": util_ratio,
        "pay_to_bal": pay_to_bal,
        "delinq_trend_sum": max(0.0, delay_pressure + max(0.0, trend)),
        "bal_volatility": max(0.0, balance_pressure),
        "risk_composite": 0.0,
    }

    risk_composite = (amount_pressure * 0.8) + (balance_pressure * 0.7) + (delay_pressure * 0.7) + (effective_declines * 0.15)
    if is_international:
        risk_composite += 0.45
    if risky_merchant:
        risk_composite += 0.55
    risk_composite += loan_stress * 0.5

    feats["risk_composite"] = risk_composite
    return feats


def predict_risk(raw_payload: dict, history_context: dict | None = None) -> dict:
    # Stage 1: baseline risk from LightGBM on transaction-derived behavioral features.
    features = calculate_features_from_transaction(raw_payload, history_context=history_context)
    with model_swap_lock:
        input_row = {col: features.get(col, 0.0) for col in feature_columns}
        input_df = pd.DataFrame([input_row])
        input_df = input_df.fillna(0).replace([float("inf"), float("-inf")], 0)

        raw_prob = float(model.predict_proba(input_df)[0][1])
        raw_prob = min(0.995, max(0.001, raw_prob))

    # Convert model probability with smooth threshold calibration to preserve low-risk variation.
    current_threshold = float(threshold)
    if 0 < current_threshold < 1:
        if raw_prob <= current_threshold:
            calibrated_prob = (raw_prob / max(current_threshold, 1e-6)) * 0.5
        else:
            calibrated_prob = 0.5 + ((raw_prob - current_threshold) / max(1.0 - current_threshold, 1e-6)) * 0.5
    else:
        calibrated_prob = raw_prob
    prob = min(0.995, max(0.0, calibrated_prob))

    first_model_score = round(float(prob) * 100, 2)
    first_model_bucket = get_risk_bucket(first_model_score)

    # Stage 2: sequential contextual risk from XGBoost using the LightGBM score as an input feature.
    customer_id = str(raw_payload.get("customer_id") or "").strip()
    run_contextual = bool(customer_id)
    context_output = predict_contextual_risk(
        customer_id,
        first_model_score,
        history_context=history_context,
        transaction_context=raw_payload,
    ) if run_contextual else None
    second_model_score = float(context_output["risk_score"]) if context_output else None
    second_model_bucket = get_risk_bucket(second_model_score) if second_model_score is not None else None

    # Stage 3: the sequential model output is the final score. No weighted blend.
    score = second_model_score if second_model_score is not None else first_model_score
    fusion_mode = "sequential_xgboost" if second_model_score is not None else "lightgbm_fallback"
    event_type = str(raw_payload.get("event_type", "")).upper()

    # Guardrail: for non-safe events, contextual model must not collapse very
    # high baseline risk into low-risk final bands.
    if (
        second_model_score is not None
        and event_type not in _EXPECTED_PAYMENT_EVENTS
        and _should_apply_context_floor(raw_payload)
    ):
        floor_score = _unsafe_event_context_floor(first_model_score)
        if floor_score > 0 and score < floor_score:
            score = floor_score
            fusion_mode = "operational_guardrail_context_floor"

    bucket = get_risk_bucket(score)

    # -----------------------------------------------------------------------
    # OPERATIONAL RULE OVERRIDES (Hard Caps based on Intent)
    # -----------------------------------------------------------------------
    if event_type in _INCOME_EVENTS:
        # Salary/income is pure new funds. It should never be scored as credit risk.
        score = min(score, _income_event_safe_cap(first_model_score, second_model_score))
        bucket = get_risk_bucket(score)
        fusion_mode = "operational_override_safe"
    elif event_type in _EXPECTED_PAYMENT_EVENTS:
        # Prevent an EMI payment on a clean account from mysteriously triggering high risk
        # If LightGBM base score is already < 50, we cap the final score to prevent drift.
        if first_model_score < 50.0:
            score = min(score, first_model_score)
            bucket = get_risk_bucket(score)
            fusion_mode = "operational_override_emi"

    return {
        "risk_score": score,
        "risk_bucket": bucket,
        "probability": round(float(prob), 4),
        "base_model_risk_score": first_model_score,
        "base_model_risk_bucket": first_model_bucket,
        "context_model_risk_score": second_model_score,
        "context_model_risk_bucket": second_model_bucket,
        "final_model_risk_score": score,
        "final_model_risk_bucket": bucket,
        "fusion_mode": fusion_mode,
        "event_type": event_type or "UNKNOWN",
        "pipeline_stage": "ingest->lightgbm->xgboost->final",
    }
