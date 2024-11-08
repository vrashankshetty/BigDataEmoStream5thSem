from kafka import KafkaConsumer
import json
import sys

def cluster_publisher(cluster_topic):
    consumer = KafkaConsumer(
        cluster_topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"Cluster Publisher for {cluster_topic} is active.")
    for message in consumer:
        data = message.value
        print(f"[{cluster_topic}] Distributing data to subscribers: {data}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python cluster_publisher.py <cluster_topic>")
    else:
        cluster_topic = sys.argv[1]
        cluster_publisher(cluster_topic)