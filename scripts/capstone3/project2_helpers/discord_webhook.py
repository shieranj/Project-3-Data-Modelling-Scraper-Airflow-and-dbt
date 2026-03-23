import requests

def discord_webhook(discord_webhook_url,content):
    try:
        payload = {
            "content": content
        }
        result = requests.post(discord_webhook_url, json=payload)
        result.raise_for_status()
        print("Alert sent to Discord!")
    except Exception as e:
        print(f"Error sending to discord channel! - status code {result.status_code}")
        

