import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID=os.getenv("PROJECT_ID")
SUBSCRIBER_ID="capstone3_shieran_project3-sub"

subcriber = pubsub_v1.SubscriberClient()
subcriber_path = subcriber.subscription_path(PROJECT_ID, SUBSCRIBER_ID)

def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message.ack() #auto ack

print("Listening for messages....")
streaming_pull_future = subcriber.subscribe(subcriber_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
