from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import datetime

app = Flask(__name__)


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@app.route('/emoji', methods=['POST'])
def receive_emoji():
    data = request.json
    print("data",data)
    data['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        producer.send('emojievents', data)
        producer.flush()
        return jsonify({'status': 'success', 'message': 'Emoji data sent to Kafka'}), 200
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to process emoji data'}), 500



if __name__ == '__main__':
    app.run(port=3000)