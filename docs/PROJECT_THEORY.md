# PIE: Pre-Delinquency Intelligence Engine — Project Theory

## Executive Summary

PIE (Pre-Delinquency Intelligence Engine) is a production-grade ML-driven risk intelligence platform designed to shift financial institutions from reactive to proactive delinquency management. By ingesting live transaction behavior, scoring risk in near-real-time, and orchestrating intervention workflows, PIE enables risk teams to prevent defaults before they occur, reducing credit losses and improving customer outcomes.

---

## 1. Problem Statement & Business Context

### The Challenge
Traditional credit risk management operates **reactively**:
- Risk is assessed at loan origination or periodic reviews
- Delinquency is discovered after payment default
- Intervention happens post-incident, reducing recovery options
- Limited insight into dynamic behavioral deterioration

### The Solution Paradigm
PIE flips the model to **proactive, behavioral risk assessment**:
- Continuous monitoring of transaction patterns post-origination
- Early detection of behavioral signals indicative of financial stress
- Risk scoring triggered by live events, not batch schedules
- Intervention orchestration before delinquency occurs

### Business Impact
- **Loss Prevention**: Early intervention reduces charge-offs
- **Customer Experience**: Non-intrusive, data-driven engagement
- **Operational Efficiency**: Prioritized intervention queues for risk teams
- **Governance**: Explainable risk factors and audit trails

---

## 2. Conceptual ML Architecture

### Two-Stage Ensemble Approach

PIE employs a **hybrid dual-model architecture**:

#### Stage 1: Baseline Model (LightGBM)
- **Scope**: Transaction-level features only
- **Purpose**: Fast, stateless scoring from raw transaction attributes
- **Features**: Amount, balance, merchant type, international flag, prior decline count, etc.
- **Output**: Base risk probability (0–100)
- **Rationale**: Provides immediate, consistent signals independent of customer profile

#### Stage 2: Contextual Model (XGBoost)
- **Scope**: Customer profile + history + prior risk trajectory
- **Purpose**: Refine baseline score using customer-level context
- **Features**: Customer segment, avg spend, income, credit history, payment patterns, prior risk scores
- **Output**: Context-adjusted probability
- **Rationale**: Recognizes that identical transactions carry different risk for different customer profiles

#### Fusion Engine
- **Strategy**: Weighted blend (70% baseline, 30% contextual) when models **agree**
- **Disagreement Handling**: Dampen or apply weighted fusion when predictions diverge significantly
- **Operational Overrides**: Apply business rules (e.g., INCOME_CREDIT always receives lower risk cap)
- **Output**: Final risk score (0–100) and bucket (LOW/MEDIUM/HIGH/CRITICAL)

### Why This Design?
1. **Resilience**: Baseline model functions independently if contextual data is unavailable
2. **Interpretability**: Two-stage approach allows isolation of transaction-level vs. customer-level signals
3. **Flexibility**: Easy to swap models, adjust weights, or introduce new stages
4. **Calibration**: Separate thresholds for each stage enable independent tuning

---

## 3. Core System Components

### 3.1 Ingestion & Streaming Layer

**Transaction Sources:**
- Simulator: Synthetic transaction generation for testing and demo
- Producer: Manual/API-triggered transaction ingestion
- Stream: Redis Stream as persistent, ordered event buffer

**Why Redis Streams?**
- Ensures event ordering (critical for temporal features)
- Consumer group model enables exactly-once processing
- Automatic retention and lag tracking
- Lightweight and performant for edge cases

### 3.2 Feature Engine

**Real-Time Feature Computation:**
- Reads transaction metadata (amount, merchant, balance)
- Retrieves customer history (last 20 transactions, aggregates)
- Computes derived features: total spend trend, decline rate, dpd change, etc.
- Aligns to active model schema

**History Buffering:**
- Maintains rolling 20-transaction window per customer
- Aggregates: average amount, balance, DPD (Days Past Due), merchant diversity
- Updates on each new transaction

### 3.3 Model Inference Pipeline

**Online Scoring:**
1. Extract transaction and context features
2. Run baseline LightGBM inference
3. If customer ID present and not a safe event, run contextual XGBoost
4. Fuse predictions via weighted blend or dampening logic
5. Apply operational overrides (e.g., expected payment events → lower risk cap)
6. Persist result to database and Redis cache

**Latency Considerations:**
- Baseline scoring: ~10–50ms (in-memory model)
- Contextual scoring: ~50–200ms (including history retrieval)
- Total E2E: <500ms typical
- Redis caching reduces repeated queries

### 3.4 Monitoring & Drift Detection

**Continuous Health Checks:**
- **Population Stability Index (PSI)**: Measures feature distribution shift
- **Kolmogorov-Smirnov (KS) Test**: Detects statistical divergence in baseline vs. recent data
- **Jensen-Shannon Divergence**: Symmetric divergence metric for robust drift detection
- **Accuracy Degradation**: Tracks baseline model performance on ground-truth labels

**Drift Thresholds:**
- Green: PSI < 0.1, KS < 0.1, accuracy > baseline - 5%
- Yellow: PSI 0.1–0.25, KS 0.1–0.2 (watch)
- Red: PSI > 0.25, KS > 0.2 (retraining triggered)

**Why This Matters:**
- Models degrade as data distributions shift (seasonal cycles, economic changes, fraud trends)
- Automated detection ensures timely retraining cycles
- Ground-truth labels (payment outcomes) feed the feedback loop

### 3.5 Retraining & Artifact Management

**Retraining Trigger:**
- Manual override by risk team
- Automated trigger from drift thresholds
- Periodic scheduled retraining (e.g., weekly)

**Retraining Workflow:**
1. Select training data (cleaned historical transactions with known outcomes)
2. Train new baseline LightGBM on training set
3. Optimize classification threshold using F1 score sweep
4. Validate on held-out test set
5. Log metrics to model history (accuracy, precision, recall, F1, AUC)
6. Save artifact (model, features, threshold)
7. Activate new model in production (atomic swap)

**Contextual Model Retraining:**
- Separate pipeline using XG-datasets CSVs or transaction history
- Builds explicit or proxy labels based on customer behavior
- Similar threshold optimization and artifact logging

**Artifact Versioning:**
- Each retraining produces timestamped model artifact
- Active model reference points to current production version
- Historical versions retained for rollback and auditing

---

## 4. Risk Scoring Semantics

### Score Interpretation
- **Range**: 0–100 (continuous), mapped to buckets: LOW (0–30), MEDIUM (30–70), HIGH (70–85), CRITICAL (85–100)
- **Meaning**: Probability or intensity of delinquency risk within a specified time horizon (e.g., 30-day forward window)
- **Calibration**: Threshold tuning ensures score distribution aligns with observed default rates

### Dynamic Overrides
- **INCOME_CREDIT events**: Capped at max 30 (recognition of positive cash flow)
- **Expected payment paths**: Recognized and protected from escalation (customer initiated a payment)
- **Customer-specific**: VIP tiers, prior good behavior, can influence cap adjustments

---

## 5. Intervention Orchestration

### Workflow
1. **High-risk Score Detection**: Scores exceeding intervention threshold (e.g., 75) trigger workflow
2. **Intervention Creation**: Assign recommendation (e.g., "Contact customer, offer payment plan")
3. **Queue & Preview**: Risk team reviews intervention queue, previews customer data
4. **Action**: Approve, reject, or defer intervention
5. **Logging & Feedback**: Record action taken; feedback loop for model learning

### Why This Design?
- Prevents alert fatigue: Not all high-risk scores require human action
- Governance: Audit trail of decisions
- Feedback: Outcomes inform future ground-truth labels

---

## 6. Operational Context

### Real-Time Dashboard
- **Risk Overview**: Current score distribution, intervention queue metrics
- **Customer Lookup**: Drill into individual customer risk profiles, transaction history
- **Model Insights**: Drift metrics, retraining history, feature importance
- **Intervention Management**: Create, review, approve, export intervention actions
- **Settings**: Configuration adjustments, model refresh triggers, threshold tuning

### API Exposure
- **REST Endpoints**: `/risk/score`, `/interventions`, `/models/drift`, auth routes
- **Authentication**: Google OAuth session-based flow for risk team
- **Rate Limiting & Caching**: Redis-backed response caching and session state
- **Audit Logging**: All API calls logged for compliance

---

## 7. MLOps & Governance

### Model Lifecycle
1. **Training**: Develop and validate on historical data
2. **Artifact Storage**: Serialized models, feature schemas, thresholds versioned
3. **Activation**: Atomic swap from staging to production
4. **Monitoring**: Continuous drift and accuracy tracking
5. **Retraining**: Triggered by drift, schedule, or manual intervention
6. **Rollback**: Previous model versions available for quick reversion

### Data Governance
- **Feature Store**: Redis caching + database persistence
- **Ground Truth**: Delayed labels (loan payment status, default outcome) feed retraining
- **Privacy**: Aggregated metrics for monitoring, no raw transaction PII in logs
- **Compliance**: Audit trails, retention policies, consent management

### Scalability Design
- **Horizontal**: Multiple stream consumer instances processing partitions
- **Vertical**: Batch retraining on separate infrastructure, non-blocking
- **Caching**: Redis reduces database hits; in-memory models eliminate disk I/O
- **Containerization**: Docker/K8s ready for cloud deployment

---

## 8. Key Innovations & Differentiators

### Behavioral Continuity
- Contextual model captures customer personality and history
- Prevents over-flagging of customers with temporarily low scores
- Recognizes improving payment behavior (positive feedback loop)

### Drift-Aware Retraining
- Automated detection prevents stale models
- Threshold optimization via F1 sweep ensures bias-recall trade-off
- History tracking provides transparency into model evolution

### Operational Overrides
- Recognizes business logic (expected payment events, income)
- Rules engine allows quick deployment of policy changes without model retraining

### Real-Time Intervention Loop
- Closes gap between prediction and action
- Enables dynamic adjustment of intervention criteria
- Provides feedback signal for model improvement

---

## 9. Validation & Testing Strategy

### Data Validation
- **Schema Checks**: Feature alignment to model input spec
- **Range Checks**: Score 0–100, bucket cardinality, timestamp ordering
- **Consistency**: Customer ID references, transaction integrity

### Model Validation
- **Backtest**: Score predictions vs. historical outcomes
- **Precision/Recall**: Threshold tuning for acceptable FPR/FNR trade-off
- **Fairness**: Check for disparate impact across demographic or geographic segments
- **Stability**: Verify model behavior under stress (e.g., extreme transaction amounts)

### System Validation
- **Load Testing**: Stream consumer throughput under peak transaction volume
- **Latency Profiling**: E2E scoring latency distribution
- **Failover**: Behavior when Redis/database unavailable
- **Retraining Correctness**: Verify artifact swap and model swapping logic

---

## 10. Future Enhancements

### Explainability
- SHAP feature importance to show which factors drove each score
- Counterfactual explanations: "Score would be X if transaction amount was Y"

### Advanced Ensemble
- Introduce gradient boosting meta-learner (stacking)
- Temporal ensembles: weight recent models more heavily

### Contextual Refinement
- Incorporate macro-economic indicators (unemployment, interest rates)
- Peer comparison: score relative to demographic cohorts

### Intervention Optimization
- ML-driven action recommendation (which intervention type is most effective?)
- Contact time optimization (when is customer most likely to respond?)

### Multi-Horizon Scoring
- Separate models for 7-day, 30-day, 90-day delinquency risk
- Time-series forecasting of risk trajectory

---

## 11. Conclusion

PIE demonstrates how modern ML infrastructure—combining streaming, ensemble modeling, drift detection, and operational orchestration—can transform credit risk workflows from reactive to proactive. By placing explainable, near-real-time predictions at the center of intervention strategy, PIE enables financial institutions to reduce credit losses, improve customer relationships, and operate with greater operational agility.
