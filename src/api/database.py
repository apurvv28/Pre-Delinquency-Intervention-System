import os
from sqlalchemy import (create_engine, Column, String,
                        Float, Text, DateTime, func)
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pie.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}  # needed for SQLite
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- ORM Models ---

class RiskScore(Base):
    __tablename__ = "risk_scores"

    id          = Column(String, primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    customer_id = Column(String, nullable=False, index=True)
    risk_score  = Column(Float, nullable=False)
    risk_bucket = Column(String, nullable=False)
    created_at  = Column(DateTime, default=func.now())

class Intervention(Base):
    __tablename__ = "interventions"

    id                = Column(String, primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    customer_id       = Column(String, nullable=False, index=True)
    risk_bucket       = Column(String, nullable=False)
    intervention_type = Column(String, nullable=False)
    message           = Column(Text)
    status            = Column(String, default="PENDING")
    created_at        = Column(DateTime, default=func.now())

class Transaction(Base):
    __tablename__ = "transactions"

    id               = Column(String, primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    customer_id      = Column(String, nullable=False, index=True)
    transaction_data = Column(Text, nullable=False)  # JSON string
    processed_at     = Column(DateTime, default=func.now())

def init_db():
    Base.metadata.create_all(bind=engine)
    print("SQLite DB initialized — pie.db created")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()