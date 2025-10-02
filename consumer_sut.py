import json
import networkx as nx
from kafka import KafkaConsumer, errors
import threading
import time
from flask import Flask, jsonify
from flask_cors import CORS

# --- Settings ---
KAFKA_TOPIC = 'traffic_updates'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
GRAPH_FILE = "irpin_drive_graph.graphml"
ALPHA = 0.3
API_PORT = 5001 # Unique port

# --- Global graph state ---
live_graph = nx.read_graphml(GRAPH_FILE)
graph_lock = threading.Lock()
for u, v, data in live_graph.edges(data=True):
    data['current_speed'] = 50.0

# --- Reusable Kafka Connection Function ---
def connect_to_kafka_with_retry():
    """Tries to connect to Kafka with several retries."""
    max_retries = 10
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            print(f"SUT Consumer: Attempting to connect to Kafka (Attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='sut-consumer-group',
                key_deserializer=lambda k: k.decode('utf-8'),
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            print("✅ SUT Consumer: Kafka Consumer connected.")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"❌ SUT Consumer: No brokers available.")
        except Exception as e:
            print(f"❌ SUT Consumer: An unexpected error occurred: {e}")
        
        print(f"SUT Consumer: Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
    
    print("❌ SUT Consumer: Failed to connect to Kafka after several retries. Exiting.")
    return None

# --- Flask App for UI ---
app = Flask(__name__)
CORS(app)

@app.route('/graph')
def get_graph_data():
    with graph_lock:
        nodes_data = [{'id': node, 'lat': data['y'], 'lon': data['x']} for node, data in live_graph.nodes(data=True)]
        edges_data = [{'source': u, 'target': v, 'speed': data.get('current_speed', 50.0)} for u, v, data in live_graph.edges(data=True)]
    return jsonify({'nodes': nodes_data, 'edges': edges_data})

# --- Kafka Consumer Logic ---
def run_sut_consumer():
    consumer = connect_to_kafka_with_retry()
    if not consumer:
        return # Exit if connection failed

    print("🚀 SUT Consumer is now processing messages...")
    for message in consumer:
        data, u, v = message.value, *eval(message.key)
        with graph_lock:
            try:
                current_speed = live_graph.edges[u, v, 0]['current_speed']
                updated_speed = (ALPHA * data['speed']) + (1 - ALPHA) * current_speed
                live_graph.edges[u, v, 0]['current_speed'] = updated_speed
            except KeyError:
                pass

if __name__ == "__main__":
    api_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=API_PORT), daemon=True)
    api_thread.start()
    run_sut_consumer()