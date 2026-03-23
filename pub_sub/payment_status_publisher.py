import os
import json
from scripts.capstone3.project3_helpers.payment_status_generation import generate_payment_status
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PUBSUB")
PROJECT_ID=os.getenv("PROJECT_ID")
TOPIC_ID="capstone3_shieran_project3"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def main():
    data = generate_payment_status(5)
    print(f"Publishing {len(data)} payment status...")

    for item in data:
        result = json.dumps(item)
        future = publisher.publish(topic_path, result.encode("utf-8"))
        message_id = future.result()
        print(f"Published payment status, event_id:{item['event_id']} - message ID: {message_id}")
    
    print(f"All payment status published!")

if __name__ == "__main__":
    main()