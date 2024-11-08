from kafka import KafkaConsumer
import json

def subscriber(cluster_topic, subscriber_id):
    consumer = KafkaConsumer(
        cluster_topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Subscriber {subscriber_id} listening to {cluster_topic}")
    for message in consumer:
        data = message.value
        print(f"[Subscriber {subscriber_id} - {cluster_topic}] Received data: {data}")



if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python subscriber.py <cluster_topic> <subscriber_id>")
    else:
        cluster_topic = sys.argv[1]
        subscriber_id = sys.argv[2]
        subscriber(cluster_topic, subscriber_id)
        