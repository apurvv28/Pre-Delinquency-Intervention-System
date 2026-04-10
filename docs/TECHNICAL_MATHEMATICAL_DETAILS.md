# PIE: Pre-Delinquency Intelligence Engine — Technical & Mathematical Details

## Table of Contents
1. [Feature Engineering](#1-feature-engineering)
2. [Model Architecture & Mathematical Formulation](#2-model-architecture--mathematical-formulation)
3. [Scoring & Calibration](#3-scoring--calibration)
4. [Drift Detection Metrics](#4-drift-detection-metrics)
5. [Retraining & Optimization](#5-retraining--optimization)
6. [System Architecture & Data Flow](#6-system-architecture--data-flow)
7. [Implementation Details](#7-implementation-details)
8. [Performance & Complexity Analysis](#8-performance--complexity-analysis)

---

## 1. Feature Engineering

### 1.1 Transaction-Level Features (Baseline Model Input)

#### Primary Features
| Feature | Type | Definition | Range |
|---------|------|-----------|-------|
| `amount` | float | Transaction amount | $0–∞ |
| `balance` | float | Account balance post-transaction | $-∞–∞ |
| `dpd` (Days Past Due) | int | Days overdue on existing obligations | 0–500+ |
| `decline_count` | int | Number of declined transactions (rolling 90-day) | 0–50+ |
| `merchant_type` | categorical | Merchant category (utilities, luxury, cash advance, etc.) | 10+ categories |
| `is_international` | bool | Cross-border transaction flag | {0, 1} |
| `is_weekend` | bool | Transaction on weekend | {0, 1} |
| `event_type` | categorical | Transaction event (PURCHASE, PAYMENT, INCOME_CREDIT, ...) | 8+ types |

#### Derived Features

$$\text{spending\_velocity} = \frac{\sum_{i=0}^{T} \text{amount}_i}{\text{days\_active}}$$

$$\text{decline\_rate} = \frac{\text{decline\_count}}{\text{total\_attempts}}$$

$$\text{balance\_volatility} = \frac{\text{std}(\text{balance\_history})}{\text{mean}(\text{balance\_history})} + \epsilon$$

$$\text{merchant\_diversity} = -\sum_{j} p_j \log(p_j)$$
where $p_j$ = fraction of transactions at merchant type $j$ (entropy metric)

$$\text{dpd\_acceleration} = \text{dpd}_{\text{current}} - \text{dpd}_{\text{prev}}$$

### 1.2 Contextual Features (Contextual Model Input)

#### Customer Profile Aggregates
| Feature | Definition |
|---------|-----------|
| `avg_monthly_income` | Mean income credits observed (rolling 12-month) |
| `avg_spend_amount` | Mean transaction amount |
| `spending_variance` | Variance in transaction amounts (high = erratic spending) |
| `income_expense_ratio` | $\frac{\text{avg\_income}}{\text{avg\_spend}}$ (sustainability metric) |
| `customer_age_months` | Duration of account in system |
| `prior_delinquency_count` | Historical delinquency incidents |

#### History Aggregates (20-Transaction Rolling Window)
$$\text{avg\_balance}_{20} = \frac{1}{20} \sum_{i=1}^{20} \text{balance}_{t-i}$$

$$\text{balance\_decrease\_ratio} = \frac{\text{count}(\text{balance}_t < \text{balance}_{t-1})}{20}$$

$$\text{payment\_consistency} = 1 - \frac{\sum_{i=1}^{20} |\text{amount}_{t-i} - \text{mean\_amount}|}{20 \cdot \text{mean\_amount}}$$

#### Prior Risk Trajectory
$$\text{prior\_risk\_score\_avg} = \frac{1}{N} \sum_{j=1}^{N} \text{score}_{t-j}$$

$$\text{risk\_trend} = \text{linear\_regression\_slope}(\text{score}_{t-N}, ..., \text{score}_t)$$

### 1.3 Feature Alignment & Schema

**Active Model Schema:**
All features must align to a versioned schema stored in model artifacts. Mismatch between transaction features and schema triggers retraining.

```python
active_schema = {
    "amount": float,
    "balance": float,
    "dpd": int,
    "decline_count": int,
    "spending_velocity": float,
    "merchant_type_[category_1]": int,  # one-hot encoded
    # ... additional features
}
```

**Missing Feature Imputation:**
- Numerical: Median of training data
- Categorical: "Unknown" category
- Logged as anomaly for monitoring

---

## 2. Model Architecture & Mathematical Formulation

### 2.1 Baseline Model: LightGBM Gradient Boosting

#### Model Specification
- **Algorithm**: Gradient Boosting Decision Trees (GBDT) with leaf-wise growth
- **Framework**: LightGBM (Microsoft's efficient GBDT)
- **Hyperparameters** (typical):
  - `num_leaves`: 31
  - `max_depth`: 5
  - `learning_rate`: 0.05
  - `num_rounds`: 100
  - `lambda_l1`, `lambda_l2`: 0.1 (regularization)

#### Mathematical Model
$$\hat{p}_{\text{baseline}}(x) = \frac{1}{1 + e^{-f(x)}}$$

where $f(x) = \sum_{m=1}^{M} T_m(x; \theta_m)$ is the ensemble of decision trees.

Each tree $T_m$ partitions feature space $\mathcal{X}$ into leaf nodes $L_m$:

$$T_m(x) = \sum_{l=1}^{L_m} c_{m,l} \mathbf{1}_{x \in R_{m,l}}$$

where $R_{m,l}$ is the region for leaf $l$ and $c_{m,l}$ is the leaf output value.

#### Training Objective
$$\mathcal{L} = \sum_{i=1}^{n} \ell(y_i, \hat{p}_i) + \sum_{m=1}^{M} \Omega(T_m)$$

- **Loss**: Binary cross-entropy $\ell(y, \hat{p}) = -y \log(\hat{p}) - (1-y) \log(1-\hat{p})$
- **Regularization**: $\Omega(T) = \gamma L + \frac{1}{2}\lambda \sum_l w_l^2$ (tree complexity + leaf weight penalty)

#### Feature Importance
$$\text{Importance}_j = \sum_{t: \text{split on } j} \text{Gain}_t$$

where Gain is the reduction in loss from splitting on feature $j$.

### 2.2 Contextual Model: XGBoost with Custom Objective

#### Model Specification
- **Algorithm**: XGBoost with regularized objective
- **Hyperparameters** (typical):
  - `max_depth`: 4–6 (shallower to avoid overfitting on smaller contextual data)
  - `learning_rate`: 0.05–0.1
  - `subsample`: 0.8 (regularization via subsampling)
  - `colsample_bytree`: 0.8

#### Mathematical Model
Similar to baseline, but trained on customer context features:

$$\hat{p}_{\text{context}}(x_c) = \frac{1}{1 + e^{-g(x_c)}}$$

where $x_c$ includes customer profile + history + prior risk aggregates.

#### Proxy Target Generation

For transactions without explicit labels, PIE generates pseudo-labels:

$$y_{\text{proxy}} = \begin{cases}
0 & \text{if safe merchant and safe event type} \\
0.3 & \text{if high income-to-expense ratio and low prior score} \\
0.7 & \text{if frequent declines or high dpd} \\
1.0 & \text{if explicit delinquency history}
\end{cases}$$

This allows training even with partial/delayed ground truth.

### 2.3 Fusion Engine: Weighted Ensemble

#### Baseline Fusion Strategy

$$\hat{p}_{\text{fused}} = w_{\text{base}} \cdot \hat{p}_{\text{baseline}} + w_{\text{context}} \cdot \hat{p}_{\text{context}}$$

where $w_{\text{base}} = 0.7$, $w_{\text{context}} = 0.3$.

#### Disagreement Detection & Dampening

$$\Delta p = |\hat{p}_{\text{baseline}} - \hat{p}_{\text{context}}|$$

$$\hat{p}_{\text{fused}} = \begin{cases}
w_{\text{base}} \cdot \hat{p}_{\text{baseline}} + w_{\text{context}} \cdot \hat{p}_{\text{context}} & \text{if } \Delta p < \tau_{\text{agree}} \\
\frac{\hat{p}_{\text{baseline}} + \hat{p}_{\text{context}}}{2} & \text{if } \Delta p \geq \tau_{\text{agree}}
\end{cases}$$

- $\tau_{\text{agree}} = 0.25$ (disagreement threshold)
- Dampening reduces weight on outlier predictions

#### Operational Overrides

$$\hat{p}_{\text{final}} = \begin{cases}
\min(\hat{p}_{\text{fused}}, 0.3) & \text{if event\_type} \in \{\text{INCOME\_CREDIT}, \text{PAYMENT}\} \\
\min(\hat{p}_{\text{fused}}, 0.4) & \text{if } \text{income\_expense\_ratio} > 1.5 \\
\hat{p}_{\text{fused}} & \text{otherwise}
\end{cases}$$

---

## 3. Scoring & Calibration

### 3.1 Probability-to-Score Mapping

After fusion, convert probability $\hat{p} \in [0, 1]$ to score $s \in [0, 100]$:

$$s = 100 \cdot \hat{p}$$

### 3.2 Threshold Calibration

A threshold $\theta$ maps score to binary decision (delinquent vs. non-delinquent):

$$\hat{y}(\theta) = \begin{cases}
1 & \text{if } s \geq \theta \\
0 & \text{otherwise}
\end{cases}$$

#### Threshold Optimization via F1 Sweep

$$F1(\theta) = 2 \cdot \frac{\text{Precision}(\theta) \cdot \text{Recall}(\theta)}{\text{Precision}(\theta) + \text{Recall}(\theta)}$$

where:
$$\text{Precision}(\theta) = \frac{\text{TP}(\theta)}{\text{TP}(\theta) + \text{FP}(\theta)}$$

$$\text{Recall}(\theta) = \frac{\text{TP}(\theta)}{\text{TP}(\theta) + \text{FN}(\theta)}$$

Optimal threshold:
$$\theta^* = \arg\max_{\theta} F1(\theta)$$

#### Risk Bucketing

$$\text{bucket}(s) = \begin{cases}
\text{LOW} & \text{if } s < 30 \\
\text{MEDIUM} & \text{if } 30 \leq s < 70 \\
\text{HIGH} & \text{if } 70 \leq s < 85 \\
\text{CRITICAL} & \text{if } s \geq 85
\end{cases}$$

---

## 4. Drift Detection Metrics

### 4.1 Population Stability Index (PSI)

Measures distribution shift in feature $X$ between baseline and current period:

$$\text{PSI}(X) = \sum_{i=1}^{n} \left( \frac{\text{Pct}_{current}(X_i) - \text{Pct}_{baseline}(X_i)}{0.5 \cdot (\text{Pct}_{baseline}(X_i) + 0.001)} \right)$$

where $\text{Pct}(X_i)$ = proportion of samples in bin $i$ (10 equal-frequency bins).

- **Green Zone**: PSI < 0.1 (negligible shift)
- **Yellow Zone**: 0.1 ≤ PSI < 0.25 (monitor)
- **Red Zone**: PSI ≥ 0.25 (significant shift, retraining recommended)

### 4.2 Kolmogorov-Smirnov (KS) Test

Measures maximum divergence between cumulative distributions:

$$\text{KS} = \max_x |F_{\text{baseline}}(x) - F_{\text{current}}(x)|$$

- **Green Zone**: KS < 0.1
- **Yellow Zone**: 0.1 ≤ KS < 0.2
- **Red Zone**: KS ≥ 0.2

**Statistical Test**:
$$D_n = \sqrt{n} \cdot \text{KS} \quad \sim \text{Kolmogorov Distribution}$$

Two-sample KS test rejects $H_0$ (distributions equal) if $D_n > K_\alpha$ (critical value for significance $\alpha$).

### 4.3 Jensen-Shannon Divergence

Symmetric version of Kullback-Leibler divergence:

$$\text{JS}(P \| Q) = \frac{1}{2} \text{KL}(P \| M) + \frac{1}{2} \text{KL}(Q \| M)$$

where $M = \frac{1}{2}(P + Q)$ and

$$\text{KL}(P \| Q) = \sum_i P(i) \log \frac{P(i)}{Q(i)}$$

- **Range**: [0, ln(2)] ≈ [0, 0.69] (symmetric)
- **Interpretation**: 0 = identical, 0.69 = completely different
- **Threshold**: JS > 0.15 triggers yellow, JS > 0.3 triggers red

### 4.4 Model Accuracy Degradation

Track held-out test set AUC over time:

$$\text{Accuracy\_Delta}(t) = \text{AUC}_{\text{current}}(t) - \text{AUC}_{\text{baseline}}$$

- **Trigger**: If $\text{Accuracy\_Delta} < -0.05$ (5% drop), initiate retraining

---

## 5. Retraining & Optimization

### 5.1 Retraining Data Selection

#### Training Set Composition
$$\mathcal{D}_{\text{train}} = \{\text{transactions from past 60–90 days with known outcomes}\}$$

Preferentially:
1. Transactions with confirmed ground-truth labels (payment status, default flag)
2. If insufficient labels: Use transactions from `pie_sample_cleaned.csv` or fallback synthetic data

#### Train-Test Split
$$\mathcal{D}_{\text{train}} = \mathcal{D}_{80\%}, \quad \mathcal{D}_{\text{test}} = \mathcal{D}_{20\%}$$

Stratified on outcome variable to maintain class balance.

### 5.2 Model Training

#### Baseline LightGBM Training
```python
params = {
    'objective': 'binary',
    'metric': 'auc',
    'num_leaves': 31,
    'max_depth': 5,
    'learning_rate': 0.05
}
model = lgb.train(params, train_data, num_boost_round=100, valid_sets=[valid_data])
```

**Early Stopping**: If validation AUC doesn't improve for 10 rounds, halt training.

#### Contextual XGBoost Training
```python
params = {
    'max_depth': 5,
    'eta': 0.05,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'objective': 'binary:logistic',
    'eval_metric': 'auc'
}
model = xgb.train(params, dtrain, num_boost_round=100, evals=[(dtest, 'eval')])
```

### 5.3 Threshold Optimization

Compute F1 score across threshold range:

$$\text{F1\_curve} = [F1(\theta) \text{ for } \theta \in [0.1, 0.2, ..., 0.9]]$$

$$\theta^* = \arg\max_{\theta} \text{F1\_curve}(\theta)$$

**Alternative Metrics**:
- **Youden's J**: $\text{Sensitivity} + \text{Specificity} - 1$
- **Matthews Correlation Coefficient**: Balanced metric for imbalanced classes

### 5.4 Model Validation Metrics

| Metric | Formula | Interpretation |
|--------|---------|-----------------|
| **AUC-ROC** | Area under ROC curve | Probability model ranks positive higher than negative |
| **Precision** | $\frac{TP}{TP+FP}$ | Of predicted positives, how many are correct? |
| **Recall** | $\frac{TP}{TP+FN}$ | Of actual positives, how many are detected? |
| **F1 Score** | $2 \cdot \frac{\text{Prec} \cdot \text{Rec}}{\text{Prec} + \text{Rec}}$ | Harmonic mean of precision & recall |
| **Specificity** | $\frac{TN}{TN+FP}$ | Of actual negatives, how many are correct? |

### 5.5 Artifact Persistence

After training, serialize:
- **Model**: `pickle.dump(model, open('pie_lgb_model.pkl', 'wb'))`
- **Features**: `pickle.dump(feature_columns, open('pie_lgb_features.pkl', 'wb'))`
- **Threshold**: `json.dump({'threshold': theta_opt}, open('pie_lgb_threshold.json', 'w'))`
- **Metrics**: Append to `model_metrics_history.json`

```json
{
  "timestamp": "2024-04-10T10:30:00Z",
  "model_version": "v2.1",
  "accuracy": 0.85,
  "precision": 0.78,
  "recall": 0.81,
  "f1": 0.795,
  "auc": 0.89,
  "threshold": 0.65
}
```

---

## 6. System Architecture & Data Flow

### 6.1 Transaction Flow Diagram

```
[Transaction Source]
        ↓
[Redis Stream Producer] → [Redis Stream Buffer]
        ↓
[Stream Consumer] (XREADGROUP + ACK)
        ↓
┌─────────────────────────────────┐
│ Feature Assembly                │
│ - Parse transaction metadata    │
│ - Retrieve customer history     │
│ - Compute derived features      │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│ Stage 1: Baseline Scoring       │
│ - Load LightGBM model           │
│ - predict_proba()               │
│ - Calibrate to 0-100 scale      │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│ Stage 2: Contextual Scoring     │
│ - Load XGBoost model            │
│ - Build context features        │
│ - predict_proba()               │
│ - Calibrate to 0-100 scale      │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│ Fusion & Overrides              │
│ - Weighted blend (70/30)        │
│ - Disagreement dampening        │
│ - Business rules (INCOME cap)   │
└─────────────────────────────────┘
        ↓
┌─────────────────────────────────┐
│ Output & Persistence            │
│ - Write RiskScore to SQLite     │
│ - Update Redis cache            │
│ - Publish live score event      │
│ - Trigger intervention (if >75) │
└─────────────────────────────────┘
```

### 6.2 Data Model

#### CustomerTransaction Table
```sql
CREATE TABLE customer_transaction (
    id INTEGER PRIMARY KEY,
    customer_id TEXT NOT NULL,
    transaction_id TEXT UNIQUE NOT NULL,
    amount REAL,
    balance REAL,
    dpd INTEGER,
    decline_count INTEGER,
    merchant_type TEXT,
    is_international BOOLEAN,
    event_type TEXT,
    created_at TIMESTAMP,
    INDEX(customer_id),
    INDEX(created_at)
);
```

#### RiskScore Table
```sql
CREATE TABLE risk_score (
    id INTEGER PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    customer_id TEXT NOT NULL,
    base_model_risk_score REAL,
    base_model_risk_bucket TEXT,
    context_model_risk_score REAL,
    context_model_risk_bucket TEXT,
    final_model_risk_score REAL,
    final_model_risk_bucket TEXT,
    inference_timestamp TIMESTAMP,
    model_version TEXT,
    FOREIGN KEY(transaction_id) REFERENCES customer_transaction(transaction_id),
    INDEX(customer_id),
    INDEX(final_model_risk_bucket)
);
```

#### Monitoring Table
```sql
CREATE TABLE model_monitoring (
    id INTEGER PRIMARY KEY,
    check_timestamp TIMESTAMP,
    feature_psi REAL,
    feature_ks REAL,
    feature_js REAL,
    prediction_drift_indicator TEXT,
    accuracy_delta REAL,
    recommendation TEXT,
    INDEX(check_timestamp)
);
```

### 6.3 Redis Cache Structure

**Keys & TTL**:
- `customer:<customer_id>:latest_score` → JSON (TTL: 1 hour)
- `customer:<customer_id>:history` → Sorted set of recent scores (TTL: 7 days)
- `model:active:baseline` → Serialized model + features (TTL: indefinite)
- `model:active:context` → Serialized model + features (TTL: indefinite)
- `intervention:queue` → Sorted set by score (TTL: 30 days)

---

## 7. Implementation Details

### 7.1 Online Inference Pseudocode

```python
def score_transaction(transaction):
    # 1. Feature Assembly
    customer_id = transaction['customer_id']
    raw_features = extract_features(transaction)
    history = get_customer_transactions(customer_id, last_n=20)
    context_features = aggregate_history(history)
    
    # 2. Baseline Scoring
    baseline_model = load_model('pie_lgb_model.pkl')
    baseline_features = align_features(raw_features, baseline_schema)
    baseline_prob = baseline_model.predict_proba(baseline_features)[1]
    baseline_score = 100 * baseline_prob
    baseline_bucket = bucket_score(baseline_score)
    
    # 3. Contextual Scoring (conditional)
    if customer_id and not is_safe_event(transaction):
        context_model = load_model('pie_xgb_model.pkl')
        all_context_features = concatenate(baseline_features, context_features)
        context_prob = context_model.predict_proba(all_context_features)[1]
        context_score = 100 * context_prob
    else:
        context_score = None
    
    # 4. Fusion
    if context_score is not None:
        if abs(baseline_score - context_score) < 25:
            final_score = 0.7 * baseline_score + 0.3 * context_score
        else:
            final_score = (baseline_score + context_score) / 2
    else:
        final_score = baseline_score
    
    # 5. Overrides
    if transaction['event_type'] == 'INCOME_CREDIT':
        final_score = min(final_score, 30)
    
    # 6. Persist & Publish
    insert_risk_score(transaction['transaction_id'], final_score)
    cache_score(customer_id, final_score)
    publish_score_event(transaction['transaction_id'], final_score)
    
    if final_score > 75:
        trigger_intervention(customer_id, final_score)
    
    return final_score
```

### 7.2 Drift Detection Pseudocode

```python
def detect_drift(feature_name, baseline_data, current_data, window_days=30):
    # 1. Compute PSI
    psi = compute_psi(baseline_data[feature_name], current_data[feature_name])
    
    # 2. Compute KS
    ks_stat, p_value = ks_2samp(baseline_data[feature_name], current_data[feature_name])
    
    # 3. Compute JS Divergence
    js_div = jensen_shannon_divergence(baseline_data[feature_name], current_data[feature_name])
    
    # 4. Determine Status
    status = 'GREEN'
    if psi > 0.25 or ks_stat > 0.2 or js_div > 0.3:
        status = 'RED'
    elif psi > 0.1 or ks_stat > 0.1 or js_div > 0.15:
        status = 'YELLOW'
    
    # 5. Log & Alert
    log_monitoring_record(feature_name, psi, ks_stat, js_div, status)
    if status == 'RED':
        trigger_retraining_pipeline()
    
    return {'feature': feature_name, 'psi': psi, 'ks': ks_stat, 'js': js_div, 'status': status}
```

### 7.3 Stream Consumer Implementation

**Technology**: Redis Streams + Consumer Group model

```python
def consume_stream():
    consumer_group = 'risk-scoring-group'
    consumer_name = f'consumer-{os.getpid()}'
    
    # Create consumer group (idempotent)
    redis_client.xgroup_create('transaction-stream', consumer_group, id='0', mkstream=True)
    
    while True:
        # Read messages with blocking timeout
        messages = redis_client.xreadgroup(
            consumer_group, consumer_name,
            streams={'transaction-stream': '>'},
            count=100,
            block=5000
        )
        
        for stream_key, msg_list in messages:
            for msg_id, msg_data in msg_list:
                try:
                    transaction = json.loads(msg_data[b'payload'])
                    risk_score = score_transaction(transaction)
                    
                    # Acknowledge message
                    redis_client.xack('transaction-stream', consumer_group, msg_id)
                except Exception as e:
                    log_error(f"Failed to process {msg_id}: {e}")
                    # Negative acknowledge (keep in pending list for retry)
```

---

## 8. Performance & Complexity Analysis

### 8.1 Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| **Feature Assembly** | O(1) | Fixed window (20 transactions) |
| **Baseline Inference (LightGBM)** | O(M × D) | M trees, D = max tree depth (~5) → ~250 comparisons |
| **Contextual Inference (XGBoost)** | O(M × D) | Similar to baseline |
| **Fusion & Calibration** | O(1) | Simple arithmetic |
| **Database Write** | O(log N) | SQLite index lookup |
| **Redis Cache Write** | O(1) | Hash set operation |
| **Stream Consumer Poll** | O(log N) | Redis XREADGROUP retrieval |
| **E2E Single Transaction** | O(1) | All operations bounded |

### 8.2 Space Complexity

| Component | Space | Notes |
|-----------|-------|-------|
| **LightGBM Model (Active)** | ~5–10 MB | 100 trees, ~50 leaves each, in-memory |
| **XGBoost Model (Active)** | ~5–10 MB | Smaller dataset, fewer features |
| **Feature Schemas** | ~100 KB | JSON schemas for alignment |
| **Customer History Cache** | ~100 MB | 1M customers × 20 transactions × 50 bytes |
| **Intervention Queue** | ~10–50 MB | Sorted set of pending interventions |

### 8.3 Throughput & Latency

#### Baseline Performance (Single Machine)
- **Transactions/sec**: 100–500 (depending on Redis + DB I/O)
- **Avg E2E Latency**: 50–200 ms per transaction
- **P95 Latency**: 300–500 ms (under spike load)
- **Model Inference Latency**: 10–50 ms (in-memory tree traversal)

#### Scaling Strategy
1. **Horizontal Scaling**: Multiple stream consumer instances on different partitions
2. **Batch Scoring**: Accumulate 100–1000 transactions, score in batch for ~10× throughput improvement
3. **Model Caching**: Keep models in GPU memory (RAPIDS, CuML) for accelerated inference
4. **Async Processing**: Offload slow operations (database writes, email notifications)

### 8.4 Retraining Overhead

- **Data Loading**: O(N) where N = training samples (typically 100K–1M) → ~10–100 sec
- **Feature Engineering**: O(N × F) where F = feature count (~50) → ~30 sec
- **Model Training** (LightGBM): O(N × M) where M = trees (~100) → ~60–300 sec
- **Threshold Sweep**: O(1000 thresholds) → ~5 sec
- **Total Retraining**: ~5–15 minutes (non-blocking, parallel)

---

## 9. Additional Mathematical Formulations

### 9.1 Fairness & Disparate Impact

Check if model predictions differ significantly across demographic groups:

$$\text{Disparate Impact Ratio} = \frac{\text{FPR}_{\text{group A}}}{\text{FPR}_{\text{group B}}}$$

- **Rule of 4/5**: If ratio < 0.8 or > 1.25, potential disparate impact
- **Mitigation**: Stratified threshold tuning, feature regularization, outcome balancing

### 9.2 Calibration Curve Analysis

Plot predicted probability vs. observed frequency:

$$\text{Calibration Error} = \frac{1}{n} \sum_{i=1}^{n} (\hat{p}_i - y_i)$$

Should be near 0 for well-calibrated model. Use Platt scaling or isotonic regression to improve.

### 9.3 Bayesian Model Averaging (Future Enhancement)

Combine predictions from multiple model versions:

$$\hat{p}_{\text{BMA}} = \sum_{m=1}^{M} w_m \hat{p}_m$$

where weights $w_m = \frac{L_m}{\sum_j L_j}$ and $L_m$ = likelihood of model $m$ given data.

---

## Conclusion

PIE's technical architecture leverages state-of-the-art gradient boosting, streaming infrastructure, and drift-aware ML lifecycle management to deliver high-fidelity, real-time risk scoring at production scale. The mathematical formulations ensure transparency, fairness, and reproducibility in an operationally demanding financial context.
