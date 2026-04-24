import pickle
import importlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sqlalchemy import func

from backend.timezone_util import get_ist_now

from backend.cache import get_customer_profile, get_customer_transactions
from backend.database import CustomerProfile, CustomerTransaction, RiskScore, SessionLocal

BACKEND_DIR = Path(__file__).resolve().parent
MODEL_PATH = BACKEND_DIR / "models" / "pie_context_xgb_model.pkl"
FEATURES_PATH = BACKEND_DIR / "models" / "pie_context_xgb_features.pkl"
THRESHOLD_PATH = BACKEND_DIR / "models" / "pie_context_xgb_threshold.pkl"
XG_DATASETS_DIR = BACKEND_DIR / "xg-datasets"
DATA_DIR = BACKEND_DIR / "data"
CONTEXTUAL_METRICS_PATH = DATA_DIR / "contextual_model_metrics_history.json"
RISKY_MERCHANTS = {"Crypto Exchange", "Gambling", "Wire Transfer", "Luxury Goods", "Jewelry", "Cash Advance"}
_RAW_PROB_FLOOR = 0.005
_CALIBRATED_PROB_FLOOR = 0.02
_SAFE_BUCKET_CODES = {
    "LOW_RISK": 0.0,
    "HIGH_RISK": 1.0,
    "CRITICAL": 2.0,
    "VERY_CRITICAL": 3.0,
}


def _safe_customer_hash(customer_id: str) -> int:
    return sum(ord(ch) for ch in customer_id) % 10000


def _bucket_to_code(bucket: str | None) -> float:
    return float(_SAFE_BUCKET_CODES.get(str(bucket).upper(), 0.0))


def _score_to_bucket(score: float) -> str:
    if score < 40:
        return "LOW_RISK"
    if score < 60:
        return "LOW_RISK"
    if score < 75:
        return "HIGH_RISK"
    if score < 99:
        return "CRITICAL"
    return "VERY_CRITICAL"


# Merchant categories that represent routine, low-risk spending.
_SAFE_MERCHANTS = frozenset({
    "Utilities", "Salary", "Finance", "Settlement", "Penalty",
})

# Event types that are expected scheduled payments (not discretionary spending).
_SAFE_EVENT_TYPES = frozenset({
    "PAYMENT", "PARTIAL_PAYMENT", "INCOME_CREDIT",
    "SETTLEMENT_OFFER", "LOAN_CLOSED",
})


def _proxy_target(
    transaction_row: CustomerTransaction,
    avg_spend: float,
    monthly_income: float,
    avg_prev_score: float,
    event_type: str = "",
) -> int:
    """
    Synthetic risk label for training the contextual XGBoost.

    Fixed thresholds:
    - Income-ratio threshold raised from 0.13 → 0.45 (13% was lower than any
      standard Indian EMI, so every EMI payment was flagged as risky).
    - The avg_spend spike rule is skipped for known-safe event types (payments,
      income credits) to avoid labelling routine EMIs as anomalous spend.
    """
    signal = 0
    is_safe_event = str(event_type).upper() in _SAFE_EVENT_TYPES

    if transaction_row.days_since_last_payment >= 25:
        signal += 1
    if transaction_row.previous_declines_24h >= 2:
        signal += 1
    if transaction_row.merchant_category in RISKY_MERCHANTS:
        signal += 1
    if bool(transaction_row.is_international):
        signal += 1

    # Only fire spend-spike for non-payment events (EMIs are SUPPOSED to be large)
    if not is_safe_event:
        if avg_spend > 0 and transaction_row.amount > avg_spend * 1.35:
            signal += 1

    # Raise threshold from 0.13 (13%) to 0.45 (45%) — standard EMI is 30-50% of income
    if monthly_income > 0 and transaction_row.amount / monthly_income > 0.45 and not is_safe_event:
        signal += 1

    if avg_prev_score >= 80:
        signal += 1
    return int(signal >= 3)


def _feature_row(
    *,
    customer_id: str,
    profile: CustomerProfile,
    tx_window: list[CustomerTransaction],
    current_tx: CustomerTransaction,
    avg_prev_risk_score: float,
    base_risk_score: float = 0.0,
    base_risk_bucket: str | None = None,
    event_type: str = "",
) -> dict:
    amounts = [float(tx.amount) for tx in tx_window] or [float(current_tx.amount)]
    dpds = [float(tx.days_since_last_payment) for tx in tx_window] or [float(current_tx.days_since_last_payment)]
    declines = [float(tx.previous_declines_24h) for tx in tx_window] or [float(current_tx.previous_declines_24h)]
    intl_ratio = sum(1.0 for tx in tx_window if tx.is_international) / max(len(tx_window), 1)
    risky_ratio = sum(1.0 for tx in tx_window if tx.merchant_category in RISKY_MERCHANTS) / max(len(tx_window), 1)
    avg_spend = sum(amounts) / max(len(amounts), 1)
    spend_std = pd.Series(amounts).std(ddof=0)
    spend_std = float(0.0 if pd.isna(spend_std) else spend_std)

    monthly_income = float(profile.monthly_income or 0)
    loan_amount = float(profile.loan_amount or 0)

    # -----------------------------------------------------------------------
    # Contextual features that teach XGBoost to recognise routine payments
    # -----------------------------------------------------------------------
    # Expected monthly EMI = loan_amount / 120 (10-year horizon as a proxy)
    expected_monthly_emi = loan_amount / 120.0 if loan_amount > 0 else monthly_income * 0.30
    tx_amt = float(current_tx.amount)
    emi_ratio = (tx_amt / max(expected_monthly_emi, 1.0)) if expected_monthly_emi > 0 else 1.0

    # 1 when the transaction amount is within the expected EMI range [50%, 150%]
    is_expected_emi_range = float(0.5 <= emi_ratio <= 1.5)

    # Encode the event type as an ordinal safety signal:
    #  2 = income / salary credit (safest)    1 = known payment event    0 = other
    evt_upper = str(event_type).upper()
    if evt_upper == "INCOME_CREDIT":
        payment_event_type = 2.0
    elif evt_upper in _SAFE_EVENT_TYPES:
        payment_event_type = 1.0
    else:
        payment_event_type = 0.0

    # 1 when merchant category is known-safe (Utilities, Salary, Finance, …)
    safe_category = float((current_tx.merchant_category or "") in _SAFE_MERCHANTS)

    # -----------------------------------------------------------------------
    # Derived interaction features for richer signal separation
    # -----------------------------------------------------------------------
    max_dpd = max(dpds) if dpds else 0.0
    dpd_trend = float(dpds[-1] - dpds[0]) / max(len(dpds), 1) if len(dpds) > 1 else 0.0
    amt_to_income = float(tx_amt / max(monthly_income, 1.0))
    amt_to_avg = float(tx_amt / max(avg_spend, 1.0))
    base_score_x_dpd = float(base_risk_score * max(dpds[-1] if dpds else 0, 0) / 100.0)
    base_score_x_amt = float(base_risk_score * amt_to_income)
    decline_intensity = float(sum(declines)) / max(len(declines), 1)
    balance_stress = float(tx_amt / max(loan_amount, 1.0)) if loan_amount > 0 else 0.0

    return {
        "customer_id_hash": _safe_customer_hash(customer_id),
        "loan_amount": loan_amount,
        "monthly_income": monthly_income,
        "account_age_months": float(profile.account_age_months or 0),
        "avg_spend": float(avg_spend),
        "avg_dpd": float(sum(dpds) / max(len(dpds), 1)),
        "avg_declines": float(sum(declines) / max(len(declines), 1)),
        "intl_ratio": float(intl_ratio),
        "risky_merchant_ratio": float(risky_ratio),
        "spend_volatility": spend_std,
        "spend_to_income": float(avg_spend / monthly_income) if monthly_income > 0 else 0.0,
        "loan_to_income": float(loan_amount / (monthly_income * 12.0)) if monthly_income > 0 else 0.0,
        "avg_spend_risk_score": float(avg_prev_risk_score),
        "base_risk_score": float(base_risk_score),
        "base_risk_bucket_code": _bucket_to_code(base_risk_bucket),
        "tx_amount": tx_amt,
        "tx_days_since_last_payment": float(current_tx.days_since_last_payment),
        "tx_previous_declines_24h": float(current_tx.previous_declines_24h),
        "tx_is_international": float(1 if current_tx.is_international else 0),
        # Contextual payment-intent features
        "emi_ratio": float(min(emi_ratio, 5.0)),          # capped at 5x to prevent outliers
        "is_expected_emi_range": is_expected_emi_range,
        "payment_event_type": payment_event_type,
        "safe_category": safe_category,
        # Derived interaction features
        "max_dpd": float(max_dpd),
        "dpd_trend": float(dpd_trend),
        "amt_to_income": float(amt_to_income),
        "amt_to_avg": float(amt_to_avg),
        "base_score_x_dpd": float(base_score_x_dpd),
        "base_score_x_amt": float(base_score_x_amt),
        "decline_intensity": float(decline_intensity),
        "balance_stress": float(balance_stress),
        # Categorical fields (one-hot encoded before model input)
        "occupation": profile.occupation or "Unknown",
        "loan_type": profile.loan_type or "Unknown",
        "spending_culture": profile.spending_culture or "Unknown",
        "risk_segment": profile.risk_segment or "Unknown",
        "branch": profile.branch or "Unknown",
        "merchant_category": current_tx.merchant_category or "Unknown",
    }


def _normalize_prior_risk_score(value: float) -> float:
    # External datasets may provide risk in [0, 1], while internal data uses [0, 100].
    return float(value * 100.0) if 0 <= value <= 1 else float(value)


def _prepare_training_dataset_from_csv() -> tuple[pd.DataFrame, pd.Series] | None:
    customers_path = XG_DATASETS_DIR / "customers.csv"
    transactions_path = XG_DATASETS_DIR / "transactions.csv"
    prior_scores_path = XG_DATASETS_DIR / "prior_risk_scores.csv"

    if not (customers_path.exists() and transactions_path.exists() and prior_scores_path.exists()):
        return None

    customers_df = pd.read_csv(customers_path)
    transactions_df = pd.read_csv(transactions_path)
    prior_scores_df = pd.read_csv(prior_scores_path)

    if customers_df.empty or transactions_df.empty:
        return None

    required_customer_cols = {
        "customer_id",
        "monthly_income",
        "loan_amount",
        "account_age_months",
        "occupation",
        "loan_type",
        "spending_culture",
        "risk_segment",
        "branch",
    }
    required_transaction_cols = {
        "customer_id",
        "transaction_time",
        "amount",
        "days_since_last_payment",
        "previous_declines_24h",
        "merchant_category",
        "is_international",
        "tx_baseline_risk_score",
        "tx_risk_bucket",
    }
    required_prior_cols = {"customer_id", "risk_score"}

    if not required_customer_cols.issubset(set(customers_df.columns)):
        raise ValueError("xg-datasets/customers.csv missing required columns")
    if not required_transaction_cols.issubset(set(transactions_df.columns)):
        raise ValueError("xg-datasets/transactions.csv missing required columns")
    if not required_prior_cols.issubset(set(prior_scores_df.columns)):
        raise ValueError("xg-datasets/prior_risk_scores.csv missing required columns")

    customers_df = customers_df.copy()
    transactions_df = transactions_df.copy()
    prior_scores_df = prior_scores_df.copy()

    transactions_df["transaction_time"] = pd.to_datetime(transactions_df["transaction_time"], errors="coerce")
    transactions_df = transactions_df.dropna(subset=["transaction_time"])
    transactions_df = transactions_df.sort_values(["customer_id", "transaction_time"])

    customers_df["monthly_income"] = pd.to_numeric(customers_df["monthly_income"], errors="coerce").fillna(0)
    customers_df["loan_amount"] = pd.to_numeric(customers_df["loan_amount"], errors="coerce").fillna(0)
    customers_df["account_age_months"] = pd.to_numeric(customers_df["account_age_months"], errors="coerce").fillna(0)
    customers_df["contextual_target"] = pd.to_numeric(customers_df["contextual_target"], errors="coerce").fillna(0).astype(int)

    transactions_df["amount"] = pd.to_numeric(transactions_df["amount"], errors="coerce").fillna(0)
    transactions_df["days_since_last_payment"] = pd.to_numeric(transactions_df["days_since_last_payment"], errors="coerce").fillna(0)
    transactions_df["previous_declines_24h"] = pd.to_numeric(transactions_df["previous_declines_24h"], errors="coerce").fillna(0)
    transactions_df["is_international"] = pd.to_numeric(transactions_df["is_international"], errors="coerce").fillna(0).astype(int)
    transactions_df["tx_baseline_risk_score"] = pd.to_numeric(transactions_df["tx_baseline_risk_score"], errors="coerce").fillna(0)

    prior_scores_df["risk_score"] = pd.to_numeric(prior_scores_df["risk_score"], errors="coerce").fillna(0)
    prior_scores_df["risk_score"] = prior_scores_df["risk_score"].map(_normalize_prior_risk_score)
    avg_risk_map = prior_scores_df.groupby("customer_id")["risk_score"].mean().to_dict()

    customers_map = customers_df.set_index("customer_id").to_dict(orient="index")

    rows: list[dict] = []
    targets: list[int] = []
    tx_label_candidates = ["tx_contextual_target", "contextual_target", "tx_target", "risk_target"]
    tx_label_col = next((name for name in tx_label_candidates if name in transactions_df.columns), None)
    if tx_label_col is not None:
        transactions_df[tx_label_col] = pd.to_numeric(transactions_df[tx_label_col], errors="coerce").fillna(0).astype(int)

    for customer_id, tx_group in transactions_df.groupby("customer_id"):
        profile = customers_map.get(customer_id)
        if not profile:
            continue

        tx_rows = tx_group.to_dict(orient="records")
        if len(tx_rows) < 15:
            continue

        avg_prev_score = float(avg_risk_map.get(customer_id, 0.0))

        for idx in range(14, len(tx_rows)):
            tx_window = tx_rows[max(0, idx - 20):idx]
            current_tx = tx_rows[idx]

            amounts = [float(tx["amount"]) for tx in tx_window] or [float(current_tx["amount"])]
            dpds = [float(tx["days_since_last_payment"]) for tx in tx_window] or [float(current_tx["days_since_last_payment"])]
            declines = [float(tx["previous_declines_24h"]) for tx in tx_window] or [float(current_tx["previous_declines_24h"])]

            intl_ratio = sum(1.0 for tx in tx_window if int(tx["is_international"]) == 1) / max(len(tx_window), 1)
            risky_ratio = sum(1.0 for tx in tx_window if str(tx["merchant_category"]) in RISKY_MERCHANTS) / max(len(tx_window), 1)
            avg_spend = sum(amounts) / max(len(amounts), 1)
            spend_std = pd.Series(amounts).std(ddof=0)
            spend_std = float(0.0 if pd.isna(spend_std) else spend_std)

            monthly_income = float(profile.get("monthly_income") or 0)
            loan_amount = float(profile.get("loan_amount") or 0)
            tx_amount = float(current_tx["amount"])
            tx_merchant = str(current_tx.get("merchant_category") or "Unknown")

            expected_monthly_emi = loan_amount / 120.0 if loan_amount > 0 else monthly_income * 0.30
            emi_ratio = (tx_amount / max(expected_monthly_emi, 1.0)) if expected_monthly_emi > 0 else 1.0

            event_type = str(current_tx.get("event_type") or "").upper()
            if event_type == "INCOME_CREDIT":
                payment_event_type = 2.0
            elif event_type in _SAFE_EVENT_TYPES:
                payment_event_type = 1.0
            else:
                payment_event_type = 0.0

            _base_risk = float(current_tx.get("tx_baseline_risk_score") or profile.get("baseline_risk_score") or avg_prev_score or 0.0)
            _avg_dpd = float(sum(dpds) / max(len(dpds), 1))
            _avg_declines = float(sum(declines) / max(len(declines), 1))
            max_dpd = max(dpds) if dpds else 0.0
            dpd_trend = float(dpds[-1] - dpds[0]) / max(len(dpds), 1) if len(dpds) > 1 else 0.0
            amt_to_income = float(tx_amount / max(monthly_income, 1.0))
            amt_to_avg = float(tx_amount / max(avg_spend, 1.0))
            base_score_x_dpd = float(_base_risk * max(dpds[-1] if dpds else 0, 0) / 100.0)
            base_score_x_amt = float(_base_risk * amt_to_income)
            decline_intensity = _avg_declines
            balance_stress = float(tx_amount / max(loan_amount, 1.0)) if loan_amount > 0 else 0.0

            row = {
                "customer_id_hash": _safe_customer_hash(str(customer_id)),
                "loan_amount": loan_amount,
                "monthly_income": monthly_income,
                "account_age_months": float(profile.get("account_age_months") or 0),
                "avg_spend": float(avg_spend),
                "avg_dpd": _avg_dpd,
                "avg_declines": _avg_declines,
                "intl_ratio": float(intl_ratio),
                "risky_merchant_ratio": float(risky_ratio),
                "spend_volatility": spend_std,
                "spend_to_income": float(avg_spend / monthly_income) if monthly_income > 0 else 0.0,
                "loan_to_income": float(loan_amount / (monthly_income * 12.0)) if monthly_income > 0 else 0.0,
                "avg_spend_risk_score": float(avg_prev_score),
                "base_risk_score": _base_risk,
                "base_risk_bucket_code": _bucket_to_code(current_tx.get("tx_risk_bucket") or profile.get("baseline_risk_bucket")),
                "tx_amount": tx_amount,
                "tx_days_since_last_payment": float(current_tx["days_since_last_payment"]),
                "tx_previous_declines_24h": float(current_tx["previous_declines_24h"]),
                "tx_is_international": float(1 if int(current_tx["is_international"]) == 1 else 0),
                "emi_ratio": float(min(emi_ratio, 5.0)),
                "is_expected_emi_range": float(0.5 <= emi_ratio <= 1.5),
                "payment_event_type": payment_event_type,
                "safe_category": float(tx_merchant in _SAFE_MERCHANTS),
                # Derived interaction features
                "max_dpd": float(max_dpd),
                "dpd_trend": float(dpd_trend),
                "amt_to_income": float(amt_to_income),
                "amt_to_avg": float(amt_to_avg),
                "base_score_x_dpd": float(base_score_x_dpd),
                "base_score_x_amt": float(base_score_x_amt),
                "decline_intensity": float(decline_intensity),
                "balance_stress": float(balance_stress),
                "occupation": str(profile.get("occupation") or "Unknown"),
                "loan_type": str(profile.get("loan_type") or "Unknown"),
                "spending_culture": str(profile.get("spending_culture") or "Unknown"),
                "risk_segment": str(profile.get("risk_segment") or "Unknown"),
                "branch": str(profile.get("branch") or "Unknown"),
                "merchant_category": tx_merchant,
            }
            rows.append(row)

            if tx_label_col is not None:
                target_value = int(current_tx.get(tx_label_col) or 0)
            else:
                signal = 0
                if row["tx_days_since_last_payment"] >= 25:
                    signal += 1
                if row["tx_previous_declines_24h"] >= 2:
                    signal += 1
                if row["merchant_category"] in RISKY_MERCHANTS:
                    signal += 1
                if row["tx_is_international"] == 1:
                    signal += 1

                is_safe_event = event_type in _SAFE_EVENT_TYPES
                if (not is_safe_event) and row["avg_spend"] > 0 and row["tx_amount"] > row["avg_spend"] * 1.35:
                    signal += 1
                if (not is_safe_event) and row["monthly_income"] > 0 and row["tx_amount"] / row["monthly_income"] > 0.45:
                    signal += 1
                if max(row["avg_spend_risk_score"], row["base_risk_score"]) >= 80:
                    signal += 1
                target_value = int(signal >= 3)
            targets.append(target_value)

    if not rows:
        return None

    frame = pd.DataFrame(rows).fillna(0)
    frame = pd.get_dummies(
        frame,
        columns=["occupation", "loan_type", "spending_culture", "risk_segment", "branch", "merchant_category"],
        dummy_na=False,
    )
    target = pd.Series(targets, dtype=int)
    return frame, target


def _prepare_training_dataset() -> tuple[pd.DataFrame, pd.Series]:
    rows: list[dict] = []
    targets: list[int] = []

    with SessionLocal() as db:
        profiles = db.query(CustomerProfile).all()
        avg_risk_map = {
            customer_id: float(avg_score or 0.0)
            for customer_id, avg_score in db.query(RiskScore.customer_id, func.avg(RiskScore.risk_score)).group_by(RiskScore.customer_id).all()
        }

        for profile in profiles:
            tx_rows = (
                db.query(CustomerTransaction)
                .filter(CustomerTransaction.customer_id == profile.customer_id)
                .order_by(CustomerTransaction.transaction_time.asc())
                .all()
            )
            if len(tx_rows) < 15:
                continue

            for idx in range(14, len(tx_rows)):
                tx_window = tx_rows[max(0, idx - 20):idx]
                current_tx = tx_rows[idx]
                avg_prev_score = avg_risk_map.get(profile.customer_id, 0.0)
                row = _feature_row(
                    customer_id=profile.customer_id,
                    profile=profile,
                    tx_window=tx_window,
                    current_tx=current_tx,
                    avg_prev_risk_score=avg_prev_score,
                    base_risk_score=float(current_tx.risk_score if current_tx.risk_score is not None else avg_prev_score),
                    base_risk_bucket=_score_to_bucket(float(current_tx.risk_score if current_tx.risk_score is not None else avg_prev_score)),
                    event_type=getattr(current_tx, "merchant_category", ""),
                )

                rows.append(row)
                if current_tx.risk_score is not None:
                    targets.append(int(float(current_tx.risk_score) >= 80.0))
                else:
                    avg_spend = row["avg_spend"]
                    targets.append(_proxy_target(
                        current_tx,
                        avg_spend,
                        float(profile.monthly_income or 0),
                        avg_prev_score,
                        event_type=getattr(current_tx, "merchant_category", ""),
                    ))

    if not rows:
        raise ValueError("Not enough transaction history to train contextual model")

    frame = pd.DataFrame(rows).fillna(0)
    frame = pd.get_dummies(
        frame,
        columns=["occupation", "loan_type", "spending_culture", "risk_segment", "branch", "merchant_category"],
        dummy_na=False,
    )
    target = pd.Series(targets, dtype=int)
    return frame, target


def _optimal_threshold(y_true: pd.Series, probabilities: pd.Series) -> float:
    best_threshold = 0.5
    best_f1 = -1.0
    for i in range(10, 91, 2):
        threshold = i / 100
        pred = (probabilities >= threshold).astype(int)
        value = f1_score(y_true, pred, zero_division=0)
        if value > best_f1:
            best_f1 = value
            best_threshold = threshold
    return float(best_threshold)


def train_contextual_model() -> dict:
    try:
        xgb_module = importlib.import_module("xgboost")
        XGBClassifier = getattr(xgb_module, "XGBClassifier")
        roc_auc_score = importlib.import_module("sklearn.metrics").roc_auc_score
        precision_score = importlib.import_module("sklearn.metrics").precision_score
        recall_score = importlib.import_module("sklearn.metrics").recall_score
        accuracy_score = importlib.import_module("sklearn.metrics").accuracy_score
    except Exception as exc:
        raise RuntimeError("xgboost or sklearn is not installed. Install dependencies from requirements.txt") from exc

    external_dataset = _prepare_training_dataset_from_csv()
    data_source = "xg-datasets" if external_dataset is not None else "database"
    frame, target = external_dataset if external_dataset is not None else _prepare_training_dataset()
    X_train, X_val, y_train, y_val = train_test_split(frame, target, test_size=0.2, random_state=42, stratify=target)

    # Handle class imbalance with SMOTE if available, otherwise use scale_pos_weight
    try:
        from imblearn.over_sampling import SMOTE
        smote = SMOTE(random_state=42, k_neighbors=min(5, max(1, int(y_train.sum()) - 1)))
        X_train, y_train = smote.fit_resample(X_train, y_train)
        print(f"[XGB_TRAIN] SMOTE applied: {len(X_train)} training samples")
    except ImportError:
        print("[XGB_TRAIN] imblearn not available, using scale_pos_weight for class balance")
    except Exception as smote_err:
        print(f"[XGB_TRAIN] SMOTE failed ({smote_err}), using scale_pos_weight")

    # Auto-calculate scale_pos_weight for class imbalance
    neg_count = int((y_train == 0).sum())
    pos_count = max(1, int((y_train == 1).sum()))
    spw = float(neg_count / pos_count)

    model = XGBClassifier(
        n_estimators=600,
        max_depth=6,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        colsample_bylevel=0.7,
        min_child_weight=3,
        gamma=0.1,
        reg_alpha=0.5,
        reg_lambda=2.0,
        scale_pos_weight=spw,
        objective="binary:logistic",
        eval_metric="auc",
        random_state=42,
        n_jobs=4,
        tree_method="hist",
        early_stopping_rounds=50,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    val_prob = pd.Series(model.predict_proba(X_val)[:, 1])
    threshold = _optimal_threshold(y_val.reset_index(drop=True), val_prob)
    val_pred = (val_prob >= threshold).astype(int)
    
    # Calculate comprehensive metrics
    f1_value = float(f1_score(y_val, val_pred, zero_division=0))
    accuracy = float(accuracy_score(y_val, val_pred))
    precision = float(precision_score(y_val, val_pred, zero_division=0))
    recall = float(recall_score(y_val, val_pred, zero_division=0))
    auc = float(roc_auc_score(y_val, val_prob))

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MODEL_PATH.open("wb") as handle:
        pickle.dump(model, handle)
    with FEATURES_PATH.open("wb") as handle:
        pickle.dump(frame.columns.tolist(), handle)
    with THRESHOLD_PATH.open("wb") as handle:
        pickle.dump(float(threshold), handle)

    # Save metrics to history
    metrics_entry = {
        "runAt": get_ist_now().isoformat(),
        "modelVersion": "contextual-xgb-sequential-v2",
        "modelType": "xgboost",
        "threshold": round(float(threshold), 4),
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1_value, 4),
        "auc": round(auc, 4),
        "driftScore": 0.0,
        "triggerReason": "manual-training",
        "datasetRows": int(len(frame)),
        "featureCount": int(frame.shape[1]),
        "dataSource": data_source,
    }
    _append_contextual_metrics(metrics_entry)

    return {
        "status": "trained",
        "data_source": data_source,
        "rows": int(len(frame)),
        "feature_count": int(frame.shape[1]),
        "threshold": round(float(threshold), 4),
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1_value, 4),
        "auc": round(auc, 4),
        "trained_at": get_ist_now().isoformat(),
    }


def _safe_read_contextual_metrics() -> list[dict]:
    """Load contextual model metrics history from JSON file."""
    if not CONTEXTUAL_METRICS_PATH.exists():
        return []
    try:
        with CONTEXTUAL_METRICS_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return []
    return payload if isinstance(payload, list) else []


def _save_contextual_metrics(history: list[dict]) -> None:
    """Save contextual model metrics history to JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with CONTEXTUAL_METRICS_PATH.open("w", encoding="utf-8") as handle:
        json.dump(history, handle, indent=2)


def _append_contextual_metrics(entry: dict) -> dict:
    """Append a new metrics entry to contextual model history."""
    history = _safe_read_contextual_metrics()
    history.append(entry)
    _save_contextual_metrics(history)
    return entry


def _load_artifacts():
    if not (MODEL_PATH.exists() and FEATURES_PATH.exists() and THRESHOLD_PATH.exists()):
        return None, None, None

    with MODEL_PATH.open("rb") as handle:
        model = pickle.load(handle)
    with FEATURES_PATH.open("rb") as handle:
        feature_columns = pickle.load(handle)
    with THRESHOLD_PATH.open("rb") as handle:
        threshold = pickle.load(handle)
    return model, feature_columns, threshold


def get_contextual_model_status() -> dict:
    model, feature_columns, threshold = _load_artifacts()
    return {
        "available": model is not None,
        "feature_count": int(len(feature_columns) if feature_columns else 0),
        "threshold": float(threshold) if threshold is not None else None,
    }


def get_contextual_model_monitoring(db=None) -> dict:
    """Get comprehensive contextual model monitoring report."""
    history = _safe_read_contextual_metrics()
    history = sorted(history, key=lambda item: item.get("runAt", ""))
    latest_run = history[-1] if history else None
    latest_accuracy = float(latest_run.get("accuracy", 0.0)) if latest_run else 0.0

    drift_report = {
        "driftScore": float(latest_run.get("driftScore", 0.0)) if latest_run else 0.0,
        "status": "healthy",
        "shouldRetrain": False,
        "baselineWindow": 0,
        "recentWindow": 0,
        "featureDrift": [],
    }

    # Reuse live transaction-based drift when DB session is available.
    if db is not None:
        try:
            from backend.monitoring import compute_drift_report

            drift_report = compute_drift_report(db)
        except Exception:
            pass

    retrain_recommended = bool(drift_report.get("shouldRetrain", False) or latest_accuracy < 0.95)
    status = "healthy"
    if retrain_recommended:
        status = "retrain" if drift_report.get("shouldRetrain", False) else "watch"

    return {
        "status": status,
        "shouldRetrain": retrain_recommended,
        "latestRun": latest_run,
        "accuracyTimeline": history[-12:],
        "drift": drift_report,
        "historyCount": len(history),
        "generatedAt": get_ist_now().isoformat(),
    }


def predict_contextual_risk(
    customer_id: str,
    base_risk_score: float,
    history_context: dict | None = None,
    transaction_context: dict | None = None,
) -> dict | None:
    model, feature_columns, threshold = _load_artifacts()
    if model is None or feature_columns is None:
        return None

    profile_data = get_customer_profile(customer_id)
    history = get_customer_transactions(customer_id)

    # Cache can be empty for manual predict paths; fall back to SQLite directly.
    if not profile_data:
        with SessionLocal() as db:
            profile_row = db.query(CustomerProfile).filter(CustomerProfile.customer_id == customer_id).first()
            if profile_row:
                profile_data = {
                    "customer_id": profile_row.customer_id,
                    "loan_amount": profile_row.loan_amount,
                    "monthly_income": profile_row.monthly_income,
                    "account_age_months": profile_row.account_age_months,
                    "occupation": profile_row.occupation,
                    "loan_type": profile_row.loan_type,
                    "spending_culture": profile_row.spending_culture,
                    "risk_segment": profile_row.risk_segment,
                    "branch": profile_row.branch,
                }
    if not profile_data:
        profile_data = {
            "customer_id": customer_id,
            "loan_amount": float((transaction_context or {}).get("loan_amount") or 0),
            "monthly_income": float((transaction_context or {}).get("monthly_income") or 0),
            "account_age_months": float((transaction_context or {}).get("account_age_months") or 0),
            "occupation": str((transaction_context or {}).get("occupation") or "Unknown"),
            "loan_type": str((transaction_context or {}).get("loan_type") or "Unknown"),
            "spending_culture": str((transaction_context or {}).get("spending_culture") or "Unknown"),
            "risk_segment": str((transaction_context or {}).get("risk_segment") or "Unknown"),
            "branch": str((transaction_context or {}).get("branch") or "Unknown"),
        }

    if not history:
        with SessionLocal() as db:
            tx_rows = (
                db.query(CustomerTransaction)
                .filter(CustomerTransaction.customer_id == customer_id)
                .order_by(CustomerTransaction.transaction_time.asc())
                .limit(180)
                .all()
            )
            history = [
                {
                    "amount": tx.amount,
                    "days_since_last_payment": tx.days_since_last_payment,
                    "previous_declines_24h": tx.previous_declines_24h,
                    "is_international": tx.is_international,
                    "merchant_category": tx.merchant_category,
                    "risk_score": tx.risk_score,
                    "event_type": "",
                }
                for tx in tx_rows
            ]

    class _Profile:
        def __init__(self, payload: dict):
            self.customer_id = payload.get("customer_id")
            self.loan_amount = payload.get("loan_amount")
            self.monthly_income = payload.get("monthly_income")
            self.account_age_months = payload.get("account_age_months")
            self.occupation = payload.get("occupation")
            self.loan_type = payload.get("loan_type")
            self.spending_culture = payload.get("spending_culture")
            self.risk_segment = payload.get("risk_segment")
            self.branch = payload.get("branch")

    class _Tx:
        def __init__(self, payload: dict):
            self.amount = float(payload.get("amount", 0))
            self.days_since_last_payment = int(payload.get("days_since_last_payment", 0))
            self.previous_declines_24h = int(payload.get("previous_declines_24h", 0))
            self.is_international = str(payload.get("is_international", "false")).strip().lower() in {"true", "1", "yes"}
            self.merchant_category = payload.get("merchant_category", "Unknown")
            self.risk_score = payload.get("risk_score")
            self.event_type = str(payload.get("event_type", "") or "")  # carry event intent

    profile_obj = _Profile(profile_data)
    tx_rows = [_Tx(item) for item in history]
    current_source = transaction_context or (history[-1] if history else {})
    if not current_source:
        current_source = {
            "amount": 0,
            "days_since_last_payment": 0,
            "previous_declines_24h": 0,
            "is_international": False,
            "merchant_category": "Unknown",
            "event_type": "",
        }
    current_tx = _Tx(current_source)
    tx_window = tx_rows[-20:] if tx_rows else [current_tx]

    hist_scores = [float(item.get("risk_score")) for item in history if item.get("risk_score") is not None]
    avg_prev_risk_score = float(sum(hist_scores) / len(hist_scores)) if hist_scores else float(base_risk_score)
    if history_context and history_context.get("avg_risk_score") is not None:
        avg_prev_risk_score = float(history_context.get("avg_risk_score") or avg_prev_risk_score)

    feature_row = _feature_row(
        customer_id=customer_id,
        profile=profile_obj,
        tx_window=tx_window,
        current_tx=current_tx,
        avg_prev_risk_score=avg_prev_risk_score,
        base_risk_score=float(base_risk_score),
        base_risk_bucket=_score_to_bucket(float(base_risk_score)),
        event_type=getattr(current_tx, "event_type", ""),  # propagate payment intent
    )

    input_df = pd.DataFrame([feature_row]).fillna(0)
    input_df = pd.get_dummies(
        input_df,
        columns=["occupation", "loan_type", "spending_culture", "risk_segment", "branch", "merchant_category"],
        dummy_na=False,
    )
    aligned = {column: (input_df[column] if column in input_df.columns else 0.0) for column in feature_columns}
    model_input = pd.DataFrame(aligned).fillna(0)

    prob = float(model.predict_proba(model_input)[0][1])
    # Clamp but use a very low floor to preserve the model's native spread
    prob = min(0.995, max(0.0005, prob))

    threshold_value = float(threshold)

    # -----------------------------------------------------------------------
    # Score construction: use the LightGBM baseline as the anchor and XGBoost
    # probability as a contextual adjustment signal.
    #
    # The XGBoost model is a binary classifier — its raw probability clusters
    # near 0 (safe) or near 1 (risky).  We convert this into a directional
    # nudge on the continuous LightGBM baseline to preserve full-spectrum
    # score spread across LOW / HIGH / CRITICAL / VERY_CRITICAL bands.
    # -----------------------------------------------------------------------
    base_score_clamped = min(99.5, max(0.5, float(base_risk_score)))

    if prob >= threshold_value:
        # Risky: scale upward.  Distance above threshold drives magnitude.
        excess = (prob - threshold_value) / max(1.0 - threshold_value, 1e-6)
        # Push score toward ceiling (99.5), proportional to excess
        score = base_score_clamped + (99.5 - base_score_clamped) * (excess ** 0.7)
    else:
        # Safe: apply a GENTLE downward pull that preserves risk tiers.
        # deficit is in [0, 1] — how far below the threshold the model's prob is.
        deficit = (threshold_value - prob) / max(threshold_value, 1e-6)

        # Adaptive reduction: pull harder on very-low-risk baselines, but only
        # mildly reduce moderate/high baselines so the HIGH_RISK band survives.
        if base_score_clamped >= 80:
            # High baseline: XGBoost safe signal = modest 15-20% reduction
            reduction_pct = 0.18 * (deficit ** 0.9)
        elif base_score_clamped >= 60:
            # Moderate baseline: reduce by up to 25%
            reduction_pct = 0.25 * (deficit ** 0.85)
        else:
            # Low baseline: can pull down more aggressively
            reduction_pct = 0.35 * (deficit ** 0.75)

        score = base_score_clamped * (1.0 - reduction_pct)

    # Inject per-customer uniqueness: features that vary per customer ensure
    # even identical baselines produce distinct final scores.
    avg_risk_signal = float(avg_prev_risk_score) / 100.0
    customer_delta = (avg_risk_signal - (base_score_clamped / 100.0)) * 12.0
    score = score + customer_delta

    # Additional spread from the XGBoost probability itself (varies per customer)
    prob_spread = (prob - 0.01) * 5.0  # maps [0.001, 0.2] -> [-0.045, 0.95]
    score = score + prob_spread

    # Keep ceiling at 99.0 before applying hash offset, so offset survives
    score = min(99.0, max(2.0, score))

    # Customer-hash micro-offset ensures even bounded scores differ
    hash_offset = (_safe_customer_hash(customer_id) % 100) * 0.008
    score = score + hash_offset

    score = round(score, 2)

    return {
        "risk_score": score,
        "probability": round(prob, 4),
        "base_risk_score": round(float(base_risk_score), 2),
    }

