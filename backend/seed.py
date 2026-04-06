import json
import random
from datetime import datetime, timedelta, timezone

from backend.cache import append_customer_transaction, get_customer_profile_list, is_seed_complete, mark_seed_complete, set_customer_profile, set_customer_profile_list, set_customer_transactions
from backend.database import CustomerProfile, CustomerTransaction, SessionLocal
from backend.timezone_util import get_ist_now

CUSTOMER_COUNT = 50
SEED_TRANSACTIONS_PER_CUSTOMER = 100

BRANCHES = ["Mumbai-Central", "Pune-Main", "Delhi-South", "Bengaluru-East", "Kolkata-HQ", "Chennai-North"]
LOAN_TYPES = ["Personal Loan", "Home Loan", "Auto Loan", "Education Loan", "Business Loan"]
OCCUPATIONS = ["Salaried", "Self Employed", "Small Business Owner", "Consultant", "Government Employee", "Freelancer"]
RELATIONSHIP_MANAGERS = ["Ananya Rao", "Rohit Sharma", "Priya Nair", "Vikram Mehta", "Sara Khan"]
CUSTOMER_NAMES = [
    "Aarav", "Vihaan", "Aditya", "Arjun", "Kabir", "Reyansh", "Ishaan", "Sai", "Krish", "Atharv",
    "Anaya", "Diya", "Ira", "Myra", "Aanya", "Sara", "Nisha", "Pooja", "Riya", "Tara",
    "Rohan", "Karan", "Nikhil", "Manish", "Siddharth", "Rahul", "Vivek", "Harsh", "Amit", "Naveen",
    "Meera", "Sana", "Kavya", "Pallavi", "Simran", "Neha", "Shreya", "Ritika", "Tanvi", "Nandini",
    "Jatin", "Mohan", "Pranav", "Sahil", "Yash", "Tejas", "Arnav", "Bhavesh", "Om", "Dev",
]


def _risk_segment_for_index(index: int) -> str:
    bucket = index % 10
    if bucket < 5:
        return "LOW"
    if bucket < 8:
        return "MEDIUM"
    if bucket == 8:
        return "HIGH"
    return "CRITICAL"


def _profile_for_index(index: int) -> dict:
    customer_id = f"CUST-{index + 1:04d}"
    risk_segment = _risk_segment_for_index(index)
    branch = BRANCHES[index % len(BRANCHES)]
    loan_type = LOAN_TYPES[index % len(LOAN_TYPES)]
    name = f"{CUSTOMER_NAMES[index % len(CUSTOMER_NAMES)]} {CUSTOMER_NAMES[(index + 17) % len(CUSTOMER_NAMES)]}"
    monthly_income = round(25000 + (index % 7) * 8500 + (15000 if risk_segment == "CRITICAL" else 0), 2)
    loan_amount = round(monthly_income * (10 + (index % 9) * 1.8), 2)
    account_age_months = 18 + (index * 3) % 84
    spending_culture = {
        "LOW": "Essentials First",
        "MEDIUM": "Balanced",
        "HIGH": "Lifestyle Heavy",
        "CRITICAL": "Volatile",
    }.get(risk_segment, "Balanced")
    return {
        "customer_id": customer_id,
        "name": name,
        "branch": branch,
        "loan_type": loan_type,
        "risk_segment": risk_segment,
        "monthly_income": monthly_income,
        "loan_amount": loan_amount,
        "occupation": OCCUPATIONS[index % len(OCCUPATIONS)],
        "spending_culture": spending_culture,
        "email": f"{customer_id.lower()}@examplebank.local",
        "rm_email": f"rm.{(index % len(RELATIONSHIP_MANAGERS)) + 1}@examplebank.local",
        "rm_phone": f"+91-90000{index + 10000:05d}",
        "branch_address": f"{branch}, Main Road",
        "intervention_status": "MONITORING",
        "pre_npa": False,
        "account_age_months": account_age_months,
        "relationship_manager": RELATIONSHIP_MANAGERS[index % len(RELATIONSHIP_MANAGERS)],
    }


def build_customer_profiles() -> list[dict]:
    return [_profile_for_index(index) for index in range(CUSTOMER_COUNT)]


def _base_amount_for_segment(risk_segment: str) -> float:
    return {
        "LOW": 750.0,
        "MEDIUM": 1600.0,
        "HIGH": 3400.0,
        "CRITICAL": 6200.0,
    }.get(risk_segment, 900.0)


def build_transaction_record(profile: dict, transaction_index: int, rng: random.Random, seeded: bool, previous_history: list[dict]) -> dict:
    segment = profile["risk_segment"]
    base_amount = _base_amount_for_segment(segment)
    history_amounts = [float(item["amount"]) for item in previous_history[-12:]] if previous_history else [base_amount]
    history_dpds = [int(item["days_since_last_payment"]) for item in previous_history[-12:]] if previous_history else [4]

    trend_factor = 1 + min(0.5, transaction_index / 240.0)
    amount = round(max(40.0, rng.gauss(base_amount * trend_factor, base_amount * 0.18)), 2)

    if segment == "LOW":
        days_since_last_payment = max(0, int(rng.gauss(3, 2)))
        declines = max(0, int(rng.gauss(0, 0.5)))
        merchant = rng.choice(["Groceries", "Fuel", "Pharmacy", "Utilities", "Coffee Shop"])
    elif segment == "MEDIUM":
        days_since_last_payment = max(0, int(rng.gauss(10, 4)))
        declines = max(0, int(rng.gauss(1, 1)))
        merchant = rng.choice(["Online Shopping", "Electronics", "Restaurant", "Travel", "Fuel"])
    elif segment == "HIGH":
        days_since_last_payment = max(0, int(rng.gauss(22, 6)))
        declines = max(1, int(rng.gauss(2, 1)))
        merchant = rng.choice(["Electronics", "Travel", "Luxury Goods", "Cash Advance", "Online Shopping"])
    else:
        days_since_last_payment = max(0, int(rng.gauss(42, 10)))
        declines = max(2, int(rng.gauss(4, 2)))
        merchant = rng.choice(["Crypto Exchange", "Gambling", "Wire Transfer", "Luxury Goods", "Jewelry"])

    balance_after = round(max(200.0, profile["monthly_income"] * 2.4 - amount * rng.uniform(0.8, 1.8)), 2)
    is_international = rng.random() < (0.08 if segment == "LOW" else 0.18 if segment == "MEDIUM" else 0.38)

    avg_amount = sum(history_amounts) / len(history_amounts)
    avg_dpd = sum(history_dpds) / len(history_dpds)

    transaction_time = (get_ist_now() - timedelta(minutes=(SEED_TRANSACTIONS_PER_CUSTOMER - transaction_index) * 240)).isoformat()
    raw_payload = {
        "amount": amount,
        "current_balance": balance_after,
        "days_since_last_payment": days_since_last_payment,
        "previous_declines_24h": declines,
        "is_international": str(is_international).lower(),
        "merchant_category": merchant,
    }

    return {
        "customer_id": profile["customer_id"],
        "transaction_index": transaction_index,
        "amount": amount,
        "balance_after": balance_after,
        "days_since_last_payment": days_since_last_payment,
        "previous_declines_24h": declines,
        "merchant_category": merchant,
        "is_international": is_international,
        "transaction_time": transaction_time,
        "seeded": seeded,
        "raw_json": raw_payload,
        "avg_amount": round(avg_amount, 2),
        "avg_dpd": round(avg_dpd, 2),
    }


def seed_backend_data(*, seed_transactions: bool = True) -> list[dict]:
    with SessionLocal() as db:
        existing_profile_count = db.query(CustomerProfile).count()
        existing_transaction_count = db.query(CustomerTransaction).count()
        expected_seed_transactions = CUSTOMER_COUNT * SEED_TRANSACTIONS_PER_CUSTOMER

        has_required_data = existing_profile_count >= CUSTOMER_COUNT
        if seed_transactions:
            has_required_data = has_required_data and (existing_transaction_count >= expected_seed_transactions)

        if has_required_data:
            existing_profiles = db.query(CustomerProfile).order_by(CustomerProfile.customer_id.asc()).all()
            profiles = [
                {
                    "customer_id": profile.customer_id,
                    "name": profile.name,
                    "branch": profile.branch,
                    "loan_type": profile.loan_type,
                    "risk_segment": profile.risk_segment,
                    "monthly_income": profile.monthly_income,
                    "loan_amount": profile.loan_amount,
                    "occupation": profile.occupation,
                    "spending_culture": profile.spending_culture,
                    "email": profile.email,
                    "rm_email": profile.rm_email,
                    "rm_phone": profile.rm_phone,
                    "branch_address": profile.branch_address,
                    "intervention_status": profile.intervention_status,
                    "pre_npa": profile.pre_npa,
                    "account_age_months": profile.account_age_months,
                    "relationship_manager": profile.relationship_manager,
                }
                for profile in existing_profiles
            ]
            set_customer_profile_list(profiles)
            mark_seed_complete()
            return profiles

    if is_seed_complete():
        cached_profiles = get_customer_profile_list()
        if cached_profiles:
            return cached_profiles

    profiles = build_customer_profiles()

    with SessionLocal() as db:
        for profile in profiles:
            existing = db.get(CustomerProfile, profile["customer_id"])
            if existing is None:
                db.add(CustomerProfile(**profile))
            else:
                existing.name = profile["name"]
                existing.branch = profile["branch"]
                existing.loan_type = profile["loan_type"]
                existing.risk_segment = profile["risk_segment"]
                existing.monthly_income = profile["monthly_income"]
                existing.loan_amount = profile["loan_amount"]
                existing.occupation = profile["occupation"]
                existing.spending_culture = profile["spending_culture"]
                existing.email = profile["email"]
                existing.rm_email = profile["rm_email"]
                existing.rm_phone = profile["rm_phone"]
                existing.branch_address = profile["branch_address"]
                existing.intervention_status = profile["intervention_status"]
                existing.pre_npa = profile["pre_npa"]
                existing.account_age_months = profile["account_age_months"]
                existing.relationship_manager = profile["relationship_manager"]

        db.commit()

        if not seed_transactions:
            for profile in profiles:
                set_customer_profile(profile)
                set_customer_transactions(profile["customer_id"], [])
            set_customer_profile_list(profiles)
            mark_seed_complete()
            return profiles

        for profile in profiles:
            rng = random.Random(int(profile["customer_id"].split("-")[-1]))
            history: list[dict] = []
            transactions_for_cache: list[dict] = []

            for transaction_index in range(1, SEED_TRANSACTIONS_PER_CUSTOMER + 1):
                transaction = build_transaction_record(profile, transaction_index, rng, True, history)
                history.append(transaction)
                transactions_for_cache.append(transaction)
                db.add(
                    CustomerTransaction(
                        customer_id=profile["customer_id"],
                        transaction_index=transaction_index,
                        amount=transaction["amount"],
                        balance_after=transaction["balance_after"],
                        days_since_last_payment=transaction["days_since_last_payment"],
                        previous_declines_24h=transaction["previous_declines_24h"],
                        merchant_category=transaction["merchant_category"],
                        is_international=transaction["is_international"],
                        transaction_time=datetime.fromisoformat(transaction["transaction_time"]),
                        risk_score=None,
                        risk_bucket=None,
                        is_seeded=True,
                        raw_json=json.dumps(transaction["raw_json"]),
                    )
                )

            set_customer_profile(profile)
            set_customer_transactions(profile["customer_id"], transactions_for_cache)

        db.commit()

    set_customer_profile_list(profiles)
    mark_seed_complete()
    return profiles
