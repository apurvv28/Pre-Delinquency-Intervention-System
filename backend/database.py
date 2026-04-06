import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Float, Integer, String, Text, Boolean, create_engine, func, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pie.db")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class CustomerProfile(Base):
    __tablename__ = "customer_profiles"

    customer_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    branch = Column(String, nullable=False)
    loan_type = Column(String, nullable=False)
    risk_segment = Column(String, nullable=False)
    monthly_income = Column(Float, nullable=False)
    loan_amount = Column(Float, nullable=True)
    occupation = Column(String, nullable=True)
    spending_culture = Column(String, nullable=True)
    email = Column(String, nullable=True)
    rm_email = Column(String, nullable=True)
    rm_phone = Column(String, nullable=True)
    branch_address = Column(String, nullable=True)
    intervention_status = Column(String, nullable=True)
    pre_npa = Column(Boolean, default=False)
    account_age_months = Column(Integer, nullable=False)
    relationship_manager = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())


class RiskScore(Base):
    __tablename__ = "risk_scores"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, nullable=False, index=True)
    risk_score = Column(Float, nullable=False)
    risk_bucket = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())


class CustomerTransaction(Base):
    __tablename__ = "customer_transactions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, nullable=False, index=True)
    transaction_index = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    balance_after = Column(Float, nullable=False)
    days_since_last_payment = Column(Integer, nullable=False)
    previous_declines_24h = Column(Integer, nullable=False)
    merchant_category = Column(String, nullable=False)
    is_international = Column(Boolean, default=False)
    transaction_time = Column(DateTime, default=func.now(), index=True)
    risk_score = Column(Float, nullable=True)
    risk_bucket = Column(String, nullable=True)
    is_seeded = Column(Boolean, default=True)
    raw_json = Column(Text, nullable=False)


class InterventionQueue(Base):
    __tablename__ = "intervention_queue"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, nullable=False, index=True)
    risk_score = Column(Float, nullable=False)
    tier_label = Column(String, nullable=False)
    engine_tier = Column(Integer, nullable=False, index=True)
    status = Column(String, nullable=False, default="PENDING")
    delivery_status = Column(String, nullable=False, default="QUEUED")
    created_at = Column(DateTime, default=func.now(), index=True)
    scheduled_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    approved_by = Column(String, nullable=True)
    maker_id = Column(String, nullable=True)
    checker_id = Column(String, nullable=True)
    retry_count = Column(Integer, default=0)
    case_file_path = Column(String, nullable=True)
    case_file_id = Column(String, nullable=True)
    email_subject = Column(String, nullable=True)
    template_variables_json = Column(Text, nullable=True)
    email_html = Column(Text, nullable=True)
    admin_note = Column(Text, nullable=True)
    approval_comment = Column(Text, nullable=True)
    rm_escalation_flag = Column(Boolean, default=False)
    collections_flag = Column(Boolean, default=False)
    response_status = Column(String, nullable=False, default="NO_RESPONSE")
    response_due_at = Column(DateTime, nullable=True)


class InterventionAuditLog(Base):
    __tablename__ = "intervention_audit_log"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    intervention_id = Column(String, nullable=True, index=True)
    customer_id = Column(String, nullable=False, index=True)
    action = Column(String, nullable=False, index=True)
    actor = Column(String, nullable=False)
    details_json = Column(Text, nullable=False)
    prev_hash = Column(String, nullable=True)
    event_hash = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime, default=func.now(), index=True)


class DriftLog(Base):
    __tablename__ = "drift_log"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    checked_at = Column(DateTime, default=func.now(), index=True)
    composite_score = Column(Float, nullable=False)
    psi_score = Column(Float, nullable=False)
    ks_score = Column(Float, nullable=False)
    js_score = Column(Float, nullable=False)
    data_quality_score = Column(Float, nullable=False)
    stability_label = Column(String, nullable=False)
    feature_breakdown_json = Column(Text, nullable=False)
    triggered_retraining = Column(Boolean, default=False)
    trigger_mode = Column(String, nullable=False, default="manual")


class RetrainJob(Base):
    __tablename__ = "retrain_jobs"

    job_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, nullable=False, default="pending", index=True)
    trigger_type = Column(String, nullable=False)
    triggered_by = Column(String, nullable=False)
    drift_score = Column(Float, nullable=True)
    colab_url = Column(String, nullable=True)
    response_json = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), index=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    completed_at = Column(DateTime, nullable=True)


class ModelRegistry(Base):
    __tablename__ = "model_registry"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    version = Column(String, nullable=False, unique=True, index=True)
    trained_at = Column(DateTime, default=func.now(), index=True)
    auc_roc = Column(Float, nullable=False)
    gini = Column(Float, nullable=False)
    ks_stat = Column(Float, nullable=False)
    precision = Column(Float, nullable=True)
    recall = Column(Float, nullable=True)
    f1 = Column(Float, nullable=True)
    drift_score_at_trigger = Column(Float, nullable=True)
    drive_file_id = Column(String, nullable=True)
    status = Column(String, nullable=False, default="candidate", index=True)
    model_path = Column(String, nullable=True)
    preprocessor_path = Column(String, nullable=True)
    metadata_json = Column(Text, nullable=True)


class ModelAuditLog(Base):
    __tablename__ = "model_audit_log"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    action = Column(String, nullable=False, index=True)
    actor = Column(String, nullable=False)
    details_json = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), index=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_profile_columns()
    print("Backend SQLite tables ensured")


def _ensure_profile_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("customer_profiles")}
    except Exception:
        return

    alter_statements = []
    if "loan_amount" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN loan_amount FLOAT")
    if "occupation" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN occupation VARCHAR")
    if "spending_culture" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN spending_culture VARCHAR")
    if "email" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN email VARCHAR")
    if "rm_email" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN rm_email VARCHAR")
    if "rm_phone" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN rm_phone VARCHAR")
    if "branch_address" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN branch_address VARCHAR")
    if "intervention_status" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN intervention_status VARCHAR")
    if "pre_npa" not in columns:
        alter_statements.append("ALTER TABLE customer_profiles ADD COLUMN pre_npa BOOLEAN DEFAULT 0")

    if not alter_statements:
        return

    with engine.begin() as conn:
        for statement in alter_statements:
            conn.execute(text(statement))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
