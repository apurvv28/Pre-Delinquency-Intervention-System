import os
import pickle
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Load model artifacts once at startup
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_artifacts():
    model_path = os.getenv("MODEL_PATH", os.path.join(ROOT_DIR, "models", "pie_lightgbm_model_v2.pkl"))
    with open(model_path, "rb") as f:
        model = pickle.load(f)
        
    features_path = os.getenv("FEATURES_PATH", os.path.join(ROOT_DIR, "models", "pie_feature_columns_v2.pkl"))
    with open(features_path, "rb") as f:
        feature_columns = pickle.load(f)
        
    threshold_path = os.getenv("THRESHOLD_PATH", os.path.join(ROOT_DIR, "models", "pie_threshold_v2.pkl"))
    with open(threshold_path, "rb") as f:
        threshold = pickle.load(f)
        
    return model, feature_columns, threshold

model, feature_columns, threshold = load_artifacts()
print(f"Model loaded | Features: {len(feature_columns)} | Threshold: {threshold}")

def get_risk_bucket(score: float) -> str:
    if score < 30:   return "LOW_RISK"
    if score < 60:   return "HIGH_RISK"
    if score < 80:   return "CRITICAL"
    return "VERY_CRITICAL"

def get_intervention(bucket: str) -> str:
    mapping = {
        "LOW_RISK":      "No action required — monitor next cycle",
        "HIGH_RISK":     "WF1: Send template SMS reminder via Twilio",
        "CRITICAL":      "WF2: Generate LLM payment plan + alert",
        "VERY_CRITICAL": "WF3: Raise human intervention ticket immediately"
    }
    return mapping.get(bucket, "Unknown")

def calculate_features_from_transaction(tx: dict) -> dict:
    # If features are already passed (e.g. from the stream simulator), just return them
    if 'P_2_last' in tx:
        return tx
        
    # Otherwise, this is a raw transaction. We calculate the dataset features heuristically!
    amt = float(tx.get('amount', 0))
    bal = float(tx.get('current_balance', 0) or tx.get('latest_balance', 0) or 0)
    days_late = int(tx.get('days_since_last_payment', 0) or tx.get('latest_dpd', 0) or 0)
    declines = int(tx.get('previous_declines_24h', 0))
    merchant = str(tx.get('merchant_category') or tx.get('transaction_reason') or '')
    
    # Base defaults
    feats = {
        "P_2_last": 0.9, "P_2_mean": 0.8, "P_2_std": 0.1,
        "B_1_last": 0.05, "B_1_mean": 0.05, "B_2_last": 0.9,
        "D_39_last": 0.0, "D_41_last": 0.0, "R_1_last": 0.0,
        "S_3_last": 0.2, "util_ratio": 0.05, "pay_to_bal": 20.0,
        "delinq_trend_sum": 0.0, "bal_volatility": 0.0, "risk_composite": 0.0
    }
    
    # Calculate util ratio and pay_to_bal
    if bal > 0:
        feats['util_ratio'] = min(1.0, amt / bal)
        feats['pay_to_bal'] = max(0.0, 100.0 / (bal + 1))
    
    # Risk adjustments based on raw transaction behaviour
    risk_composite = 0.0
    
    if days_late > 30:
        feats['P_2_last'] -= 0.4
        feats['B_1_last'] += 0.5
        feats['delinq_trend_sum'] += 5.0
        risk_composite += 1.0
        
    if declines > 2:
        feats['P_2_last'] -= 0.2
        feats['D_39_last'] += 2.0
        risk_composite += declines * 0.2
        
    if getattr(tx, 'is_international', False) or str(tx.get('is_international', 'false')).lower() == 'true':
        risk_composite += 0.5
        
    if merchant.lower() in ['crypto', 'gambling', 'crypto exchange', 'jewelry']:
        feats['B_1_last'] += 0.8
        risk_composite += 1.0

    if amt > 5000:
        risk_composite += 1.0
        
    feats['risk_composite'] = risk_composite
    return feats

def predict_risk(raw_payload: dict) -> dict:
    # 1. Transform raw transaction fields into the ML features
    features = calculate_features_from_transaction(raw_payload)

    # 2. Build input dataframe aligned to training features
    input_df = pd.DataFrame([features])

    # Add missing columns with 0
    for col in feature_columns:
        if col not in input_df.columns:
            input_df[col] = 0.0

    # Keep only training columns in correct order
    input_df = input_df[feature_columns]
    input_df = input_df.fillna(0).replace([float('inf'), float('-inf')], 0)

    prob = model.predict_proba(input_df)[0][1]
    
    # --- HEURISTIC BOOST FOR SPARSE DATA ---
    # The frontend only submits 15 features. The remaining hundreds of features
    # are zero-filled, which pulls the LightGBM probability down artificially.
    # We use 'risk_composite' (which scales directly with the severity of the
    # other missing features in our generator) to accurately recover the bucket.
    composite = features.get("risk_composite", 0.0)
    
    if composite > 2.0:
        # Very Critical (Score 80+)
        prob = min(0.98, prob + 0.60 + (composite - 2.0) * 0.1)
    elif composite > 1.0:
        # Critical (Score 60 - 79)
        prob = min(0.79, prob + 0.40 + (composite - 1.0) * 0.1)
    elif composite > 0.2:
        # High Risk (Score 30 - 59)
        prob = min(0.59, prob + 0.20 + (composite - 0.2) * 0.1)

    score = round(float(prob) * 100, 2)
    bucket = get_risk_bucket(score)
    intervention = get_intervention(bucket)

    return {
        "risk_score":               score,
        "risk_bucket":              bucket,
        "intervention_recommended": intervention,
        "probability":              round(float(prob), 4)
    }