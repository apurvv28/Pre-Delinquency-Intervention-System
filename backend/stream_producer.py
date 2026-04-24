import json
import os
import random
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pytz

from backend.cache import (
    append_stream_metric,
    get_hash_fields,
    set_hash_fields,
    stream_publish,
)
from backend.database import CustomerProfile, SessionLocal
from backend.predict import predict_risk

IST = pytz.timezone("Asia/Kolkata")
STREAM_HEALTH_KEY = "pie:stream:health"

# Exact event types requested.
EVENT_TYPES = {
    "SALARY_CREDIT",
    "UPI_DEBIT",
    "UPI_CREDIT",
    "ATM_WITHDRAWAL",
    "EMI_DEBIT",
    "NEFT_TRANSFER_OUT",
    "NEFT_TRANSFER_IN",
    "RTGS_CREDIT",
    "IMPS_DEBIT",
    "BILL_PAYMENT",
    "MERCHANT_PURCHASE",
    "INTERNATIONAL_DEBIT",
    "GST_PAYMENT",
    "INSURANCE_PREMIUM",
    "SIP_DEBIT",
    "FD_INTEREST_CREDIT",
    "PENSION_CREDIT",
    "CASH_DEPOSIT",
    "CHEQUE_BOUNCE",
    "LOAN_DISBURSEMENT_CREDIT",
}

CREDIT_EVENTS = {
    "SALARY_CREDIT",
    "UPI_CREDIT",
    "NEFT_TRANSFER_IN",
    "RTGS_CREDIT",
    "FD_INTEREST_CREDIT",
    "PENSION_CREDIT",
    "CASH_DEPOSIT",
    "LOAN_DISBURSEMENT_CREDIT",
}

# Scenario codes map to A-J requirements.
SCENARIO_CODES = [
    "A_SALARY_DELAY_STRESS",
    "B_EMI_STACK_STRESS",
    "C_INCOME_STABILITY",
    "D_SMALL_BUSINESS_CASHFLOW_CRISIS",
    "E_GAMBLING_PATTERN",
    "F_MEDICAL_EMERGENCY",
    "G_INTERNATIONAL_SPENDING_SPIKE",
    "H_RECOVERY_PATTERN",
    "I_PENSIONER_MEDICAL_DRAIN",
    "J_STUDENT_SEMESTER_CRUNCH",
]


MERCHANT_POOLS: dict[str, list[str]] = {
    "food_delivery": ["zomato", "swiggy", "zepto", "blinkit"],
    "transport": ["ola", "uber", "rapido", "irctc", "redbus", "indigo_airlines"],
    "grocery": ["dmart", "bigbasket", "reliance_fresh", "more_supermarket"],
    "fuel": ["hp_petrol_pump", "indian_oil", "bpcl_pump"],
    "utilities": ["mseb_electricity", "bescom", "tata_power", "jio_recharge", "airtel_bill"],
    "entertainment": ["netflix", "hotstar", "pvr_cinemas", "bookmyshow", "sony_liv"],
    "healthcare": ["apollo_pharmacy", "medplus", "fortis_hospital", "practo"],
    "education": ["byju", "unacademy", "college_fee", "school_fee"],
    "ecommerce": ["amazon_india", "flipkart", "meesho", "myntra"],
    "finance": ["mutual_fund_sip", "insurance_premium", "loan_emi"],
    "luxury": ["taj_hotels", "louis_vuitton", "apple_store", "forex_usd"],
    "rural_kirana": ["kirana_store", "agri_supply_store", "fertilizer_seed_store"],
    "gaming": ["online_gaming", "teen_patti", "rummy_circle"],
    "business": ["vendor_neft", "logistics_partner", "petrol_fleet"],
}


@dataclass
class IndianBankingPersona:
    customer_id: str
    archetype: str
    monthly_income: float
    loan_amount: float
    loan_type: str
    occupation: str
    branch: str
    overdraft_limit: float
    current_balance: float
    simulated_time: datetime
    last_salary_date: datetime | None = None
    last_emi_date: datetime | None = None
    last_pension_date: datetime | None = None
    last_payment_date: datetime | None = None
    last_atm_date: datetime | None = None
    scenario: str | None = None
    scenario_step: int = 0
    scenario_until: datetime | None = None
    health_shock_multiplier: float = 1.0
    decline_events: deque = field(default_factory=lambda: deque(maxlen=64))

    def register_decline(self, now_ist: datetime) -> None:
        self.decline_events.append(now_ist)

    def previous_declines_24h(self, now_ist: datetime) -> int:
        cutoff = now_ist - timedelta(hours=24)
        while self.decline_events and self.decline_events[0] < cutoff:
            self.decline_events.popleft()
        return len(self.decline_events)


class RealismEngine:
    def __init__(self, rng: random.Random):
        self.rng = rng

    @staticmethod
    def now_ist() -> datetime:
        return datetime.now(IST)

    def advance_persona_time(self, persona: IndianBankingPersona) -> datetime:
        # Keep a fast-forward simulation clock to express monthly/seasonal patterns
        # while still emitting in real time.
        increment_minutes = self.rng.randint(45, 240)
        persona.simulated_time = persona.simulated_time + timedelta(minutes=increment_minutes)
        return persona.simulated_time

    @staticmethod
    def is_weekend(ts: datetime) -> bool:
        return ts.weekday() >= 5

    @staticmethod
    def is_festival_window(ts: datetime) -> bool:
        # Holi ~Mar, Diwali ~Oct-Nov seasonal bump.
        return ts.month in {3, 10, 11}

    @staticmethod
    def is_kharif(ts: datetime) -> bool:
        return ts.month in {6, 7, 8, 9, 10}

    @staticmethod
    def is_rabi(ts: datetime) -> bool:
        return ts.month in {11, 12, 1, 2, 3}

    @staticmethod
    def set_time(ts: datetime, hour: int, minute: int) -> datetime:
        return ts.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def pick_upi_hour(self) -> int:
        roll = self.rng.random()
        if roll < 0.38:
            return self.rng.randint(12, 14)
        if roll < 0.76:
            return self.rng.randint(19, 22)
        return self.rng.randint(7, 23)

    def clamp_balance(self, persona: IndianBankingPersona, proposed: float) -> float:
        min_balance = -persona.overdraft_limit
        return round(max(min_balance, proposed), 2)

    def event_hour(self, event_type: str, base: datetime) -> datetime:
        if event_type == "SALARY_CREDIT":
            return self.set_time(base, self.rng.randint(9, 11), self.rng.randint(0, 55))
        if event_type == "PENSION_CREDIT":
            return self.set_time(base, self.rng.randint(9, 11), self.rng.randint(0, 55))
        if event_type == "EMI_DEBIT":
            return self.set_time(base, 6, self.rng.randint(0, 20))
        if event_type == "ATM_WITHDRAWAL":
            return self.set_time(base, self.rng.randint(10, 20), self.rng.randint(0, 55))
        if event_type == "CHEQUE_BOUNCE":
            return self.set_time(base, self.rng.randint(9, 10), self.rng.randint(30, 59))
        if event_type == "BILL_PAYMENT":
            return self.set_time(base, self.rng.randint(8, 23), self.rng.randint(0, 55))
        if event_type == "UPI_DEBIT" or event_type == "UPI_CREDIT":
            return self.set_time(base, self.pick_upi_hour(), self.rng.randint(0, 59))
        return self.set_time(base, self.rng.randint(7, 22), self.rng.randint(0, 59))


class TransactionGenerator:
    def __init__(self, rng: random.Random, realism: RealismEngine):
        self.rng = rng
        self.realism = realism

    def _pick(self, pool_key: str) -> str:
        return self.rng.choice(MERCHANT_POOLS[pool_key])

    def _inr(self, low: float, high: float) -> float:
        """Generate a unique INR amount with time-based entropy to avoid repeats."""
        # Inject entropy from current timestamp nanoseconds + random jitter
        entropy = (time.time() * 1000000) % 1.0
        jitter = self.rng.gauss(0, (high - low) * 0.08)  # 8% std-dev jitter
        base = self.rng.uniform(low, high)
        amount = base + jitter + (entropy * (high - low) * 0.05)
        return round(max(low * 0.85, min(high * 1.15, amount)), 2)

    def _is_due(self, last_date: datetime | None, ts: datetime, *, day: int) -> bool:
        if ts.day != day:
            return False
        if last_date is None:
            return True
        return last_date.month != ts.month or last_date.year != ts.year

    def _is_due_any(self, last_date: datetime | None, ts: datetime, *, days: set[int]) -> bool:
        if ts.day not in days:
            return False
        if last_date is None:
            return True
        return last_date.month != ts.month or last_date.year != ts.year or last_date.day != ts.day

    def _student_fee_due(self, persona: IndianBankingPersona, ts: datetime) -> bool:
        # Semester fee event around Jan/Jul once.
        if ts.month not in {1, 7}:
            return False
        if ts.day not in {4, 5, 6, 7, 8, 9, 10}:
            return False
        return persona.last_payment_date is None or persona.last_payment_date.month != ts.month

    def _inject_or_progress_scenario(self, persona: IndianBankingPersona, ts: datetime) -> None:
        if persona.scenario and persona.scenario_until and ts >= persona.scenario_until:
            persona.scenario = None
            persona.scenario_step = 0
        if persona.scenario is None and self.rng.random() < 0.015:
            persona.scenario = self.rng.choice(SCENARIO_CODES)
            persona.scenario_step = 0
            persona.scenario_until = ts + timedelta(days=self.rng.randint(3, 14))

    def _scenario_event(self, persona: IndianBankingPersona, ts: datetime) -> dict | None:
        s = persona.scenario
        if not s:
            return None

        step = persona.scenario_step

        if s == "A_SALARY_DELAY_STRESS":
            # Spend continues without salary, then bounce.
            if step < 3:
                persona.scenario_step += 1
                return {
                    "event_type": "UPI_DEBIT",
                    "merchant_category": self._pick("food_delivery"),
                    "amount": self._inr(350, 2800),
                    "is_international": False,
                }
            persona.scenario_step += 1
            return {
                "event_type": "CHEQUE_BOUNCE",
                "merchant_category": "loan_emi",
                "amount": 500.0,
                "is_international": False,
                "decline": True,
            }

        if s == "B_EMI_STACK_STRESS":
            if step < 3:
                persona.scenario_step += 1
                return {
                    "event_type": "EMI_DEBIT",
                    "merchant_category": "loan_emi",
                    "amount": self._inr(7000, 36000),
                    "is_international": False,
                }
            persona.scenario_step += 1
            return {
                "event_type": "ATM_WITHDRAWAL",
                "merchant_category": "atm_cash",
                "amount": self._inr(800, 3500),
                "is_international": False,
            }

        if s == "C_INCOME_STABILITY":
            persona.scenario_step += 1
            if self.rng.random() < 0.45:
                return {
                    "event_type": "BILL_PAYMENT",
                    "merchant_category": self._pick("utilities"),
                    "amount": self._inr(700, 3200),
                    "is_international": False,
                }
            return {
                "event_type": "UPI_DEBIT",
                "merchant_category": self._pick("grocery"),
                "amount": self._inr(200, 1800),
                "is_international": False,
            }

        if s == "D_SMALL_BUSINESS_CASHFLOW_CRISIS":
            persona.scenario_step += 1
            if step < 3:
                return {
                    "event_type": "NEFT_TRANSFER_OUT",
                    "merchant_category": "vendor_neft",
                    "amount": self._inr(12000, 90000),
                    "is_international": False,
                }
            return {
                "event_type": "CHEQUE_BOUNCE",
                "merchant_category": "gst_payment_bounce",
                "amount": 500.0,
                "is_international": False,
                "decline": True,
            }

        if s == "E_GAMBLING_PATTERN":
            persona.scenario_step += 1
            forced_ts = self.realism.set_time(ts, self.rng.randint(23, 23), self.rng.randint(0, 59))
            persona.simulated_time = forced_ts
            if step % 4 == 3:
                return {
                    "event_type": "UPI_CREDIT",
                    "merchant_category": "family_topup",
                    "amount": self._inr(500, 3000),
                    "is_international": False,
                }
            return {
                "event_type": "UPI_DEBIT",
                "merchant_category": self._pick("gaming"),
                "amount": self._inr(1200, 8500),
                "is_international": False,
            }

        if s == "F_MEDICAL_EMERGENCY":
            persona.scenario_step += 1
            if step == 0:
                return {
                    "event_type": "MERCHANT_PURCHASE",
                    "merchant_category": "fortis_hospital",
                    "amount": self._inr(50000, 200000),
                    "is_international": False,
                }
            return {
                "event_type": "UPI_DEBIT",
                "merchant_category": self._pick("healthcare"),
                "amount": self._inr(600, 6500) * persona.health_shock_multiplier,
                "is_international": False,
            }

        if s == "G_INTERNATIONAL_SPENDING_SPIKE":
            persona.scenario_step += 1
            return {
                "event_type": "INTERNATIONAL_DEBIT",
                "merchant_category": "forex_usd",
                "amount": self._inr(10000, 180000),
                "is_international": True,
            }

        if s == "H_RECOVERY_PATTERN":
            persona.scenario_step += 1
            if step == 0:
                return {
                    "event_type": "LOAN_DISBURSEMENT_CREDIT",
                    "merchant_category": "loan_disbursement",
                    "amount": self._inr(90000, 450000),
                    "is_international": False,
                }
            return {
                "event_type": "EMI_DEBIT",
                "merchant_category": "loan_emi",
                "amount": self._inr(6000, 28000),
                "is_international": False,
            }

        if s == "I_PENSIONER_MEDICAL_DRAIN":
            persona.scenario_step += 1
            persona.health_shock_multiplier = min(3.0, persona.health_shock_multiplier + 0.12)
            return {
                "event_type": "UPI_DEBIT",
                "merchant_category": self._pick("healthcare"),
                "amount": self._inr(900, 5500) * persona.health_shock_multiplier,
                "is_international": False,
            }

        if s == "J_STUDENT_SEMESTER_CRUNCH":
            persona.scenario_step += 1
            if step == 0:
                return {
                    "event_type": "MERCHANT_PURCHASE",
                    "merchant_category": "college_fee",
                    "amount": self._inr(45000, 130000),
                    "is_international": False,
                }
            return {
                "event_type": "UPI_DEBIT",
                "merchant_category": self._pick("food_delivery"),
                "amount": self._inr(70, 350),
                "is_international": False,
            }

        return None

    def _salaried_urban(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self._is_due_any(persona.last_salary_date, ts, days={1, 5}):
            return {
                "event_type": "SALARY_CREDIT",
                "merchant_category": "salary_credit",
                "amount": self._inr(25000, 120000),
                "is_international": False,
            }
        if self._is_due(persona.last_emi_date, ts, day=3):
            return {
                "event_type": "EMI_DEBIT",
                "merchant_category": "loan_emi",
                "amount": self._inr(5000, 40000),
                "is_international": False,
            }
        if persona.last_atm_date is None or (ts.date() - persona.last_atm_date.date()).days >= 7:
            return {
                "event_type": "ATM_WITHDRAWAL",
                "merchant_category": "atm_cash",
                "amount": self._inr(2000, 10000),
                "is_international": False,
            }

        if self.realism.is_weekend(ts):
            amount = self._inr(250, 2800) * 1.4
            merchant = self._pick("entertainment") if self.rng.random() < 0.45 else self._pick("food_delivery")
        else:
            amount = self._inr(120, 2200)
            merchant = self._pick("food_delivery") if self.rng.random() < 0.45 else self._pick("transport")

        if self.rng.random() < 0.12:
            merchant = self._pick("grocery")
            amount = self._inr(700, 4200)
        if self.rng.random() < 0.08:
            merchant = self._pick("fuel")
            amount = self._inr(2000, 5000)

        return {
            "event_type": "UPI_DEBIT",
            "merchant_category": merchant,
            "amount": amount,
            "is_international": False,
        }

    def _salaried_rural(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self._is_due_any(persona.last_salary_date, ts, days={1, 5}):
            return {
                "event_type": "SALARY_CREDIT",
                "merchant_category": "salary_credit",
                "amount": self._inr(8000, 25000),
                "is_international": False,
            }
        if self.rng.random() < 0.25:
            return {
                "event_type": "ATM_WITHDRAWAL",
                "merchant_category": "atm_cash",
                "amount": self._inr(900, 6500),
                "is_international": False,
            }
        if ts.day in {6, 7, 8, 9, 10} and self.rng.random() < 0.12:
            return {
                "event_type": "BILL_PAYMENT",
                "merchant_category": "jio_recharge",
                "amount": self._inr(200, 600),
                "is_international": False,
            }
        if self.realism.is_kharif(ts) or self.realism.is_rabi(ts):
            if self.rng.random() < 0.18:
                return {
                    "event_type": "NEFT_TRANSFER_OUT",
                    "merchant_category": "fertilizer_seed_store",
                    "amount": self._inr(1200, 12000),
                    "is_international": False,
                }
        return {
            "event_type": "UPI_DEBIT",
            "merchant_category": "kirana_store",
            "amount": self._inr(50, 500),
            "is_international": False,
        }

    def _self_employed_business(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if ts.day == 20 and self.rng.random() < 0.5:
            return {
                "event_type": "GST_PAYMENT",
                "merchant_category": "gst_portal",
                "amount": self._inr(15000, 180000),
                "is_international": False,
            }
        if self.rng.random() < 0.22:
            return {
                "event_type": "NEFT_TRANSFER_IN",
                "merchant_category": "business_receipt",
                "amount": self._inr(50000, 500000),
                "is_international": False,
            }
        if self.rng.random() < 0.46:
            return {
                "event_type": "NEFT_TRANSFER_OUT",
                "merchant_category": "vendor_neft",
                "amount": self._inr(15000, 175000),
                "is_international": False,
            }
        if self.rng.random() < 0.18:
            return {
                "event_type": "IMPS_DEBIT",
                "merchant_category": "logistics_partner",
                "amount": self._inr(5000, 45000),
                "is_international": False,
            }
        return {
            "event_type": "ATM_WITHDRAWAL",
            "merchant_category": "atm_cash",
            "amount": self._inr(1000, 12000),
            "is_international": False,
        }

    def _daily_wage_worker(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self.rng.random() < 0.52:
            return {
                "event_type": "UPI_CREDIT",
                "merchant_category": "daily_wage_credit",
                "amount": self._inr(300, 800),
                "is_international": False,
            }
        if self.rng.random() < 0.18:
            return {
                "event_type": "BILL_PAYMENT",
                "merchant_category": "jio_recharge",
                "amount": self._inr(99, 299),
                "is_international": False,
            }
        return {
            "event_type": "UPI_DEBIT",
            "merchant_category": "kirana_store",
            "amount": self._inr(50, 450),
            "is_international": False,
        }

    def _retired_pensioner(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self._is_due(persona.last_pension_date, ts, day=1):
            return {
                "event_type": "PENSION_CREDIT",
                "merchant_category": "pension_credit",
                "amount": self._inr(15000, 45000),
                "is_international": False,
            }
        if ts.month in {1, 4, 7, 10} and ts.day in {1, 2, 3} and self.rng.random() < 0.45:
            return {
                "event_type": "FD_INTEREST_CREDIT",
                "merchant_category": "fd_interest",
                "amount": self._inr(1500, 12000),
                "is_international": False,
            }
        if ts.day <= 7 and self.rng.random() < 0.4:
            return {
                "event_type": "BILL_PAYMENT",
                "merchant_category": self._pick("utilities"),
                "amount": self._inr(700, 3800),
                "is_international": False,
            }
        return {
            "event_type": "UPI_DEBIT",
            "merchant_category": self._pick("healthcare"),
            "amount": self._inr(250, 6500),
            "is_international": False,
        }

    def _student(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self._is_due_any(persona.last_salary_date, ts, days={1, 2}):
            return {
                "event_type": "UPI_CREDIT",
                "merchant_category": "parent_transfer",
                "amount": self._inr(5000, 15000),
                "is_international": False,
            }
        if self._student_fee_due(persona, ts):
            return {
                "event_type": "MERCHANT_PURCHASE",
                "merchant_category": "college_fee",
                "amount": self._inr(35000, 110000),
                "is_international": False,
            }
        if self.rng.random() < 0.5:
            merchant = self._pick("food_delivery")
            amount = self._inr(90, 700)
        elif self.rng.random() < 0.8:
            merchant = self._pick("entertainment")
            amount = self._inr(120, 1400)
        else:
            merchant = self._pick("gaming")
            amount = self._inr(150, 2500)
        return {
            "event_type": "UPI_DEBIT",
            "merchant_category": merchant,
            "amount": amount,
            "is_international": False,
        }

    def _high_net_worth(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if self.rng.random() < 0.18:
            return {
                "event_type": "RTGS_CREDIT",
                "merchant_category": "wealth_transfer",
                "amount": self._inr(500000, 5000000),
                "is_international": False,
            }
        if ts.day == 10 and self.rng.random() < 0.45:
            return {
                "event_type": "SIP_DEBIT",
                "merchant_category": "mutual_fund_sip",
                "amount": self._inr(15000, 250000),
                "is_international": False,
            }
        if self.rng.random() < 0.25:
            return {
                "event_type": "INTERNATIONAL_DEBIT",
                "merchant_category": "forex_usd",
                "amount": self._inr(25000, 600000),
                "is_international": True,
            }
        if self.rng.random() < 0.35:
            return {
                "event_type": "EMI_DEBIT",
                "merchant_category": "loan_emi",
                "amount": self._inr(25000, 300000),
                "is_international": False,
            }
        return {
            "event_type": "MERCHANT_PURCHASE",
            "merchant_category": self._pick("luxury"),
            "amount": self._inr(8000, 350000),
            "is_international": self.rng.random() < 0.2,
        }

    def _base_event_for_archetype(self, persona: IndianBankingPersona, ts: datetime) -> dict:
        if persona.archetype == "SALARIED_URBAN":
            return self._salaried_urban(persona, ts)
        if persona.archetype == "SALARIED_RURAL":
            return self._salaried_rural(persona, ts)
        if persona.archetype == "SELF_EMPLOYED_BUSINESS":
            return self._self_employed_business(persona, ts)
        if persona.archetype == "DAILY_WAGE_WORKER":
            return self._daily_wage_worker(persona, ts)
        if persona.archetype == "RETIRED_PENSIONER":
            return self._retired_pensioner(persona, ts)
        if persona.archetype == "STUDENT":
            return self._student(persona, ts)
        return self._high_net_worth(persona, ts)

    # ------------------------------------------------------------------
    # Spending culture modifiers — scale amounts so that customers with
    # "Essentials First" culture transact within modest, everyday ranges
    # rather than generating amounts that look anomalous to risk models.
    # ------------------------------------------------------------------
    _SPENDING_CULTURE_MODIFIERS = {
        "Essentials First": {"scale": 0.55, "max_cap_income_pct": 0.12, "debit_bias": 0.85},
        "Balanced":         {"scale": 0.80, "max_cap_income_pct": 0.25, "debit_bias": 0.70},
        "Lifestyle Heavy":  {"scale": 1.20, "max_cap_income_pct": 0.45, "debit_bias": 0.60},
        "Volatile":         {"scale": 1.45, "max_cap_income_pct": 0.65, "debit_bias": 0.50},
    }

    def _apply_spending_culture(self, persona: IndianBankingPersona, tx: dict) -> dict:
        """Scale transaction amount based on the customer's spending culture.

        This prevents 'Essentials First' customers from generating large
        discretionary spends that push them into critical risk bands.
        """
        # Look up spending culture from DB-seeded profile
        culture = None
        try:
            from backend.cache import get_customer_profile
            profile = get_customer_profile(persona.customer_id)
            if profile:
                culture = profile.get("spending_culture")
        except Exception:
            pass

        if not culture or culture not in self._SPENDING_CULTURE_MODIFIERS:
            return tx  # no modification

        mods = self._SPENDING_CULTURE_MODIFIERS[culture]
        event_type = str(tx.get("event_type", "")).upper()

        # Don't modify credit events (salary, pension, etc.) or EMI debits
        if event_type in CREDIT_EVENTS or event_type in {"EMI_DEBIT", "INSURANCE_PREMIUM", "SIP_DEBIT"}:
            return tx

        amount = float(tx.get("amount", 0))
        scaled = amount * mods["scale"]

        # Cap at a percentage of monthly income for non-credit events
        income_cap = persona.monthly_income * mods["max_cap_income_pct"]
        if income_cap > 0 and scaled > income_cap:
            scaled = income_cap * self.rng.uniform(0.6, 1.0)

        tx["amount"] = round(max(50.0, scaled), 2)
        return tx

    def generate_next(self, persona: IndianBankingPersona) -> dict:
        ts = self.realism.advance_persona_time(persona)
        self._inject_or_progress_scenario(persona, ts)

        tx = self._scenario_event(persona, ts)
        if tx is None:
            tx = self._base_event_for_archetype(persona, ts)

        # Apply spending culture modifiers BEFORE further processing
        tx = self._apply_spending_culture(persona, tx)

        event_type = str(tx["event_type"]).upper()
        if event_type not in EVENT_TYPES:
            event_type = "UPI_DEBIT"

        tx_time = self.realism.event_hour(event_type, ts)
        amount = round(float(tx["amount"]), 2)

        # Add per-transaction amount jitter to ensure uniqueness
        jitter_pct = self.rng.gauss(0, 0.03)  # ±3% gaussian jitter
        time_entropy = ((time.time() * 1e6) % 997) / 997.0 * 0.02  # ±2% time entropy
        amount = round(amount * (1.0 + jitter_pct + time_entropy), 2)
        amount = max(10.0, amount)  # floor at ₹10

        is_international = bool(tx.get("is_international", False))
        merchant = str(tx.get("merchant_category") or "unknown_merchant")

        if self.realism.is_festival_window(tx_time) and event_type in {"UPI_DEBIT", "MERCHANT_PURCHASE"}:
            amount = round(amount * self.rng.uniform(1.08, 1.30), 2)

        if self.realism.is_weekend(tx_time) and event_type in {"UPI_DEBIT", "MERCHANT_PURCHASE"}:
            amount = round(amount * self.rng.uniform(1.15, 1.45), 2)

        balance_before = persona.current_balance

        if event_type in CREDIT_EVENTS:
            balance_after = balance_before + amount
        else:
            balance_after = balance_before - amount

        if event_type == "CHEQUE_BOUNCE":
            # Always include bounce charge as required.
            balance_after = balance_before - (amount + 500.0)
            amount = amount + 500.0

        balance_after = self.realism.clamp_balance(persona, balance_after)

        # If capped due to insufficient funds on debit, mark as decline.
        declined = bool(tx.get("decline", False))
        if event_type not in CREDIT_EVENTS and balance_before - amount < -persona.overdraft_limit:
            declined = True

        if declined:
            persona.register_decline(tx_time)

        persona.current_balance = balance_after

        if event_type in {"EMI_DEBIT", "BILL_PAYMENT", "NEFT_TRANSFER_OUT", "IMPS_DEBIT"}:
            persona.last_payment_date = tx_time
        if event_type == "EMI_DEBIT":
            persona.last_emi_date = tx_time
        if event_type in {"SALARY_CREDIT", "UPI_CREDIT", "NEFT_TRANSFER_IN"}:
            persona.last_salary_date = tx_time
        if event_type == "PENSION_CREDIT":
            persona.last_pension_date = tx_time
        if event_type == "ATM_WITHDRAWAL":
            persona.last_atm_date = tx_time

        if persona.last_payment_date is None:
            days_since_last_payment = 30
        else:
            days_since_last_payment = max(0, (tx_time.date() - persona.last_payment_date.date()).days)

        return {
            "customer_id": persona.customer_id,
            "amount": round(amount, 2),
            "current_balance": round(balance_after, 2),
            "days_since_last_payment": int(days_since_last_payment),
            "previous_declines_24h": int(persona.previous_declines_24h(tx_time)),
            "merchant_category": merchant,
            "is_international": bool(is_international),
            "is_weekend": self.realism.is_weekend(tx_time),
            "event_type": event_type,
            "transaction_time": tx_time.isoformat(),
            "archetype": persona.archetype,
            "scenario": persona.scenario,
        }


class AdvancedRedisStreamProducer:
    def __init__(self):
        self.stream_key = os.getenv("REDIS_STREAM_KEY", "pie:transactions")
        self.stream_min_seconds = max(5.0, float(os.getenv("TRANSACTION_STREAM_MIN_SECONDS", "5")))
        self.stream_max_seconds = max(self.stream_min_seconds, float(os.getenv("TRANSACTION_STREAM_MAX_SECONDS", "10")))
        self.stream_seed = int(os.getenv("STREAM_RANDOM_SEED", "42"))
        self.stream_maxlen = int(os.getenv("STREAM_MAXLEN", "250000"))

        self.stop_event = threading.Event()
        self.pause_event = threading.Event()

        self.rng = random.Random(self.stream_seed)
        self.realism = RealismEngine(self.rng)
        self.generator = TransactionGenerator(self.rng, self.realism)

        self.personas = self._load_personas()
        self._round_robin_index = 0
        self.next_due_at: dict[str, float] = {
            p.customer_id: time.time() + self.rng.uniform(0.2, 2.5) for p in self.personas
        }

        self.started_at = time.time()
        self.published_count = 0
        self.event_type_counter: Counter[str] = Counter()
        self.archetype_counter: Counter[str] = Counter()
        self.last_publish_at = time.time()

    def _archetype_from_profile(self, profile: CustomerProfile) -> str:
        occupation = str(profile.occupation or "").lower()
        branch = str(profile.branch or "")
        loan_type = str(profile.loan_type or "")
        monthly_income = float(profile.monthly_income or 0)

        if monthly_income >= 90000 or float(profile.loan_amount or 0) >= 2000000:
            return "HIGH_NET_WORTH"
        if loan_type == "Education Loan" and monthly_income <= 35000:
            return "STUDENT"
        if occupation in {"small business owner", "self employed", "consultant"}:
            return "SELF_EMPLOYED_BUSINESS"
        if occupation == "freelancer" and monthly_income <= 30000:
            return "DAILY_WAGE_WORKER"
        if occupation == "government employee" and int(profile.account_age_months or 0) >= 72:
            return "RETIRED_PENSIONER"

        # Rural proxy: smaller salaried profiles outside major metro branch labels.
        is_metro = any(city in branch for city in ["Mumbai", "Pune", "Bengaluru", "Delhi"])
        if monthly_income <= 30000 and not is_metro:
            return "SALARIED_RURAL"
        return "SALARIED_URBAN"

    def _opening_balance(self, archetype: str, monthly_income: float) -> tuple[float, float]:
        if archetype == "HIGH_NET_WORTH":
            return self.rng.uniform(300000, 3000000), 5000.0
        if archetype == "SELF_EMPLOYED_BUSINESS":
            return self.rng.uniform(20000, 450000), 2500.0
        if archetype == "SALARIED_URBAN":
            return self.rng.uniform(0.8 * monthly_income, 3.5 * monthly_income), 0.0
        if archetype == "SALARIED_RURAL":
            return self.rng.uniform(0.4 * monthly_income, 1.8 * monthly_income), 0.0
        if archetype == "DAILY_WAGE_WORKER":
            return self.rng.uniform(150, 1800), 0.0
        if archetype == "RETIRED_PENSIONER":
            return self.rng.uniform(1.2 * monthly_income, 4.0 * monthly_income), 0.0
        if archetype == "STUDENT":
            return self.rng.uniform(300, 3500), 0.0
        return self.rng.uniform(2000, 12000), 0.0

    def _load_personas(self) -> list[IndianBankingPersona]:
        personas: list[IndianBankingPersona] = []
        with SessionLocal() as db:
            rows = db.query(CustomerProfile).order_by(CustomerProfile.customer_id.asc()).all()
            for row in rows:
                archetype = self._archetype_from_profile(row)
                opening_balance, overdraft_limit = self._opening_balance(archetype, float(row.monthly_income or 0))
                simulated_time = self.realism.now_ist() - timedelta(days=self.rng.randint(1, 32), hours=self.rng.randint(1, 16))
                persona = IndianBankingPersona(
                    customer_id=row.customer_id,
                    archetype=archetype,
                    monthly_income=float(row.monthly_income or 0),
                    loan_amount=float(row.loan_amount or 0),
                    loan_type=str(row.loan_type or ""),
                    occupation=str(row.occupation or ""),
                    branch=str(row.branch or ""),
                    overdraft_limit=overdraft_limit,
                    current_balance=round(opening_balance, 2),
                    simulated_time=simulated_time,
                )
                if self.rng.random() < 0.15:
                    persona.scenario = self.rng.choice(SCENARIO_CODES)
                    persona.scenario_step = 0
                    persona.scenario_until = simulated_time + timedelta(days=self.rng.randint(5, 20))
                personas.append(persona)

        print(f"[STREAM] Loaded {len(personas)} customer profiles from database for streaming")
        return personas

    def _next_round_robin_persona(self) -> IndianBankingPersona:
        persona = self.personas[self._round_robin_index]
        self._round_robin_index = (self._round_robin_index + 1) % len(self.personas)
        return persona

    def _emit_event(self, persona: IndianBankingPersona) -> None:
        payload = self.generator.generate_next(persona)

        # Keep compatibility with downstream consumer/parser keys.
        payload["balance_after"] = payload["current_balance"]
        payload["timestamp"] = payload["transaction_time"]
        payload["days_past_due"] = payload["days_since_last_payment"]

        # Score each streamed event before publishing so registry/dashboard do not
        # persist zero placeholders from the consumer default path.
        try:
            prediction = predict_risk(payload)
            payload["risk_score"] = float(prediction.get("risk_score", 0.0) or 0.0)
            payload["risk_bucket"] = str(prediction.get("risk_bucket") or "UNKNOWN")
            payload["base_model_risk_score"] = prediction.get("base_model_risk_score")
            payload["context_model_risk_score"] = prediction.get("context_model_risk_score")
        except Exception as score_err:
            print(f"[STREAM] scoring failed for {payload.get('customer_id')}: {score_err}")
            payload["risk_score"] = 0.0
            payload["risk_bucket"] = "UNKNOWN"

        stream_id = stream_publish(payload, maxlen=self.stream_maxlen)

        self.published_count += 1
        self.last_publish_at = time.time()
        self.event_type_counter[payload["event_type"]] += 1
        self.archetype_counter[payload["archetype"]] += 1

        print(
            f"[TX] {payload['customer_id']} | {payload['archetype']} | {payload['event_type']} | "
            f"{payload['merchant_category']} | INR {payload['amount']:.2f} | Bal {payload['current_balance']:.2f} | "
            f"Score {payload['risk_score']:.2f} {payload['risk_bucket']} | "
            f"Decl24h {payload['previous_declines_24h']} | Scn {payload['scenario'] or 'NONE'} | {stream_id}"
        )

        if self.published_count % 25 == 0:
            self._publish_metrics()

    def _publish_metrics(self) -> None:
        runtime = max(1.0, time.time() - self.started_at)
        eps = round(self.published_count / runtime, 3)
        pending = int(get_hash_fields(STREAM_HEALTH_KEY).get("pending_count", 0) or 0)

        health_payload = {
            "events_published": str(self.published_count),
            "events_per_second": str(eps),
            "archetype_distribution": json.dumps(dict(self.archetype_counter)),
            "event_type_distribution": json.dumps(dict(self.event_type_counter)),
            "pending_count": str(pending),
            "updated_at": self.realism.now_ist().isoformat(),
        }
        set_hash_fields(STREAM_HEALTH_KEY, health_payload)
        append_stream_metric(
            {
                "timestamp": self.realism.now_ist().isoformat(),
                "events_published": self.published_count,
                "events_per_second": eps,
                "archetype_distribution": dict(self.archetype_counter),
                "event_type_distribution": dict(self.event_type_counter),
            },
            key="pie:stream:metrics",
            maxlen=10000,
        )

    def run(self) -> None:
        print(
            f"[STREAM] Producer started | Stream={self.stream_key} | Personas={len(self.personas)} | "
            f"Per-customer cadence={self.stream_min_seconds:.1f}-{self.stream_max_seconds:.1f}s"
        )
        if not self.personas:
            print("[STREAM] No personas loaded; producer exiting")
            return

        while not self.stop_event.is_set():
            if self.pause_event.is_set():
                time.sleep(0.25)
                continue

            persona = self._next_round_robin_persona()
            now = time.time()
            due_at = self.next_due_at.get(persona.customer_id, now)

            if now >= due_at:
                self._emit_event(persona)
                self.next_due_at[persona.customer_id] = now + self.rng.uniform(self.stream_min_seconds, self.stream_max_seconds)

            time.sleep(0.03)

        self._publish_metrics()

    def stop(self) -> None:
        self.stop_event.set()

    def pause(self) -> None:
        self.pause_event.set()

    def resume(self) -> None:
        self.pause_event.clear()

    def report(self) -> dict[str, Any]:
        runtime = max(1.0, time.time() - self.started_at)
        return {
            "status": "running" if not self.stop_event.is_set() else "stopped",
            "events_published": self.published_count,
            "events_per_second": round(self.published_count / runtime, 3),
            "archetype_distribution": dict(self.archetype_counter),
            "event_type_distribution": dict(self.event_type_counter),
            "active_scenarios": {
                p.customer_id: p.scenario for p in self.personas if p.scenario
            },
            "stream_health": get_hash_fields(STREAM_HEALTH_KEY),
        }


# ---------------------------------------------------------------------------
# Module-level lifecycle helpers (kept for integration compatibility)
# ---------------------------------------------------------------------------

_PRODUCER: AdvancedRedisStreamProducer | None = None
_PRODUCER_THREAD: threading.Thread | None = None


def start_advanced_stream_producer() -> None:
    global _PRODUCER, _PRODUCER_THREAD
    if _PRODUCER_THREAD and _PRODUCER_THREAD.is_alive():
        return

    _PRODUCER = AdvancedRedisStreamProducer()
    _PRODUCER_THREAD = threading.Thread(
        target=_PRODUCER.run,
        daemon=True,
        name="advanced-stream-producer",
    )
    _PRODUCER_THREAD.start()


def stop_advanced_stream_producer() -> None:
    global _PRODUCER, _PRODUCER_THREAD
    if _PRODUCER:
        _PRODUCER.stop()
    if _PRODUCER_THREAD and _PRODUCER_THREAD.is_alive():
        _PRODUCER_THREAD.join(timeout=3)
    _PRODUCER = None
    _PRODUCER_THREAD = None


def pause_advanced_stream_producer() -> None:
    if _PRODUCER:
        _PRODUCER.pause()


def resume_advanced_stream_producer() -> None:
    if _PRODUCER:
        _PRODUCER.resume()


def get_stream_test_report() -> dict[str, Any]:
    if not _PRODUCER:
        return {
            "status": "producer_not_running",
            "events_published": 0,
            "events_per_second": 0.0,
            "archetype_distribution": {},
            "event_type_distribution": {},
            "active_scenarios": {},
            "stream_health": get_hash_fields(STREAM_HEALTH_KEY),
        }
    return _PRODUCER.report()
