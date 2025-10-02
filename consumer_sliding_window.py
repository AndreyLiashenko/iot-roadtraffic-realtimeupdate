import json, networkx as nx, threading, time
from kafka import KafkaConsumer, errors
from collections import deque
from flask import Flask, jsonify
from flask_cors import CORS

# --- Settings ---
KAFKA_TOPIC, GRAPH_FILE = 'traffic_updates', "irpin_drive_graph.graphml"
KAFKA_BOOTSTRAP_SERVERS, API_PORT = 'kafka:29092', 5003 # Unique port
WINDOW_SIZE = 50

# --- Global state ---
edge_windows = {}
edge_speeds = {}
state_lock = threading.Lock()
base_graph_nodes = [{'id': node, 'lat': data['y'], 'lon': data['x']} 
                    for node, data in nx.read_graphml(GRAPH_FILE).nodes(data=True)]

# --- Reusable Kafka Connection Function ---
def connect_to_kafka_with_retry():
    """Tries to connect to Kafka with several retries."""
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            print(f"Sliding-Window Consumer: Attempting to connect to Kafka (Attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='sliding-window-group',
                key_deserializer=lambda k: k.decode('utf-8'),
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            print("✅ Sliding-Window Consumer: Kafka Consumer connected.")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"❌ Sliding-Window Consumer: No brokers available.")
        except Exception as e:
            print(f"❌ Sliding-Window Consumer: An unexpected error occurred: {e}")
        
        print(f"Sliding-Window Consumer: Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
    
    print("❌ Sliding-Window Consumer: Failed to connect to Kafka. Exiting.")
    return None

# --- Flask App for UI ---
app = Flask(__name__)
CORS(app)

@app.route('/graph')
def get_graph_data():
    with state_lock:
        # Create a list of edges from the current speed dictionary
        edges_data = [{'source': u, 'target': v, 'speed': speed} 
                      for (u, v), speed in edge_speeds.items()]
    return jsonify({'nodes': base_graph_nodes, 'edges': edges_data})

# --- Kafka Consumer Logic ---
def run_sliding_window_consumer():
    consumer = connect_to_kafka_with_retry()
    if not consumer:
        return
        
    print("🚀 Sliding-Window Consumer is now processing messages...")
    for message in consumer:
        edge_key_str, new_speed = message.key, message.value['speed']
        with state_lock:
            if edge_key_str not in edge_windows:
                edge_windows[edge_key_str] = deque(maxlen=WINDOW_SIZE)
            
            window = edge_windows[edge_key_str]
            window.append(new_speed)
            current_avg_speed = sum(window) / len(window)
            edge_speeds[eval(edge_key_str)] = current_avg_speed

if __name__ == "__main__":
    api_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=API_PORT), daemon=True)
    api_thread.start()
    run_sliding_window_consumer()