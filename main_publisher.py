from kafka import KafkaConsumer, KafkaProducer
import json


consumer = KafkaConsumer(
    'aggregatedemojis',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def main_publisher():
    print("Main Publisher is active.")
    for message in consumer:
        data = message.value
        print(f"Received aggregated data: {data}")

        producer.send('cluster1', data)
        producer.send('cluster2', data)
        producer.send('cluster3', data)
        producer.flush()
        print("Data forwarded to clusters.")

if __name__ == "__main__":
    main_publisher()