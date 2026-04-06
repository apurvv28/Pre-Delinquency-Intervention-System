import time
import random
import uuid
import sys
import os

# Add root directory to python path for module imports
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from src.kafka.producer import publish_transaction
from src.api.predict import predict_risk

def generate_random_transaction():
    risk_profile = random.choices(
        ["LOW", "HIGH", "CRITICAL", "VERY_CRITICAL"],
        weights=[0.6, 0.2, 0.1, 0.1],
        k=1
    )[0]
    
    # Base low-risk transaction
    tx = {
        "amount": round(random.uniform(5.0, 150.0), 2),
        "current_balance": round(random.uniform(500.0, 3000.0), 2),
        "days_since_last_payment": random.randint(0, 15),
        "previous_declines_24h": 0,
        "is_international": "false",
        "merchant_category": random.choice(["Groceries", "Coffee Shop", "Restaurant", "Gas Station"])
    }
    
    if risk_profile == "HIGH":
        tx["amount"] = round(random.uniform(300.0, 1500.0), 2)
        tx["days_since_last_payment"] = random.randint(15, 35)
        tx["merchant_category"] = random.choice(["Electronics", "Travel", "Online Shopping"])
    elif risk_profile == "CRITICAL":
        tx["amount"] = round(random.uniform(2000.0, 5000.0), 2)
        tx["days_since_last_payment"] = random.randint(30, 60)
        tx["previous_declines_24h"] = random.randint(1, 3)
        tx["is_international"] = random.choice(["true", "false"])
        tx["merchant_category"] = random.choice(["Jewelry", "Luxury Goods", "Cash Advance"])
    elif risk_profile == "VERY_CRITICAL":
        tx["amount"] = round(random.uniform(5000.0, 15000.0), 2)
        tx["days_since_last_payment"] = random.randint(60, 120)
        tx["previous_declines_24h"] = random.randint(3, 8)
        tx["is_international"] = "true"
        tx["merchant_category"] = random.choice(["Crypto Exchange", "Gambling", "Wire Transfer"])
        
    return tx

def simulate_stream(interval=2.0):
    print("Starting real-time transaction simulation...")
    print("Press Ctrl+C to stop.")
    
    while True:
        try:
            customer_id = f"CUST-{random.randint(1, 50):04d}"
            transaction = generate_random_transaction()
            
            # Pass through the ML model for prediction before streaming
            prediction = predict_risk(transaction)
            
            payload = {
                "customer_id": customer_id,
                "transaction_data": transaction,
                "features": transaction, # Send the raw tx as 'features' to maintain backwards compatibility 
                "risk_score": prediction["risk_score"],
                "risk_bucket": prediction["risk_bucket"],
                "intervention_recommended": prediction["intervention_recommended"]
            }
            
            success = publish_transaction(customer_id, payload)
            if not success:
                print("Failed to publish. Retrying in 5 seconds...")
                time.sleep(5)
                continue
                
            sleep_time = random.uniform(interval * 0.5, interval * 1.5)
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            print("\nSimulation stopped.")
            break
        except Exception as e:
            print(f"Error in simulation loop: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    # You can change the interval to control the speed of the stream
    simulate_stream(interval=3.0)
