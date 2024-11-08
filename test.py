import requests
import json
import random
import time


url = 'http://localhost:3000/emoji' 

def generate_emoji_data(j):
    emoji_types = ['😊']
    emoji_data = {
        'emoji_type': random.choice(emoji_types),
        'user_id': random.randint(1,4000),  
        'timestamp': time.time()  
    }
    return emoji_data


def send_emoji_data(iterations=2000):
    for i in range(iterations):
        emoji_data = generate_emoji_data(i)
        headers = {'Content-Type': 'application/json'}
        try:
            response = requests.post(url, data=json.dumps(emoji_data), headers=headers)
            if response.status_code == 200:
                print(f"Successfully sent data: {emoji_data} {i}")
            else:
                print(f"Failed to send data: {emoji_data} - Status Code: {response.status_code}")
        except Exception as e:
            print(f"Error sending data: {e}")


if __name__ == "__main__":
    send_emoji_data(iterations=10000)