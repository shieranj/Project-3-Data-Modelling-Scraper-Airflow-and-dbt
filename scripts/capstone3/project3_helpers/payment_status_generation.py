from faker import Faker
import random
import uuid

fake = Faker("id_ID")

STATUS = ["FAILED", "SUCCESS","PENDING"]
FAILED_REASON = ["INSUFICIENT_FUNDS", "TRANSACTION_LIMITS_EXCEED", "EXPIRED", "INVALID_PAYMENT","BANK_REVERSAL"]
PENDING_REASON = ["SYS_DOWNTIME", "MERCHANT_DELAY", "PRE_AUTH"]

def generate_payment_status(num_records = 10):
    events = []

    for _ in range(num_records):
        status = random.choice(STATUS)
        if status == "FAILED":
            reason = random.choice(FAILED_REASON)
        elif status == "PENDING":
            reason = random.choice(PENDING_REASON)
        else:
            reason = None

        event_time = fake.date_time_between(
            start_date="-5d",
            end_date = "now"
        )

        events.append({
            "event_id": f"EVT_{str(uuid.uuid4())}",
            "event_time": event_time.isoformat(),
            "payment_id": f"PAY_{str(uuid.uuid4())}",
            "trx_id": f"TRX_{str(uuid.uuid4())}",
            "status": status, 
            "reason": reason
        })
    
    return events


