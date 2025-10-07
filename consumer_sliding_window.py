import json, time, threading
from kafka import KafkaConsumer, errors
from collections import deque
from flask import Flask, jsonify
from flask_cors import CORS
from prometheus_client import Summary, start_http_server
from csr_graph import CSRGraph
from edge_mapper import EdgeMapper

# --- Settings ---
SERVICE_NAME = "sliding-window-consumer"
KAFKA_TOPIC = "traffic_updates"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
GRAPH_FILE = "irpin_drive_graph.graphml"
UI_API_PORT = 5003
METRICS_PORT = 9103
WINDOW_SIZE = 50

# --- Metrics ---
PROCESSING_TIME = Summary("message_processing_seconds", "Time spent processing a message", ["consumer_name"])

# --- Graph ---
mapper = EdgeMapper(GRAPH_FILE)
csr_graph = CSRGraph(mapper)
windows = {}

# --- Flask UI ---
app = Flask(__name__)
CORS(app)
app.config["GRAPH"] = csr_graph

@app.route("/graph")
def get_graph_data():
    return jsonify(app.config["GRAPH"].get_graph_data())

# --- Kafka Logic ---
def connect_to_kafka_with_retry(group_id):
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id=group_id,
                key_deserializer=lambda k: k.decode("utf-8"),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            print(f"✅ [{SERVICE_NAME}] Kafka connected.")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"❌ [{SERVICE_NAME}] Kafka unavailable, retrying...")
            time.sleep(5)
    return None

@PROCESSING_TIME.labels(consumer_name="sliding_window").time()
def process_message(msg):
    edge_id = msg.key
    new_speed = msg.value["speed"]

    if edge_id not in windows:
        windows[edge_id] = deque(maxlen=WINDOW_SIZE)
    
    window = windows[edge_id]
    window.append(new_speed)

    avg_speed = sum(window) / len(window)
    csr_graph.update_speed(edge_id, avg_speed)

def run_consumer():
    consumer = connect_to_kafka_with_retry("sliding-window-group")
    if not consumer:
        return
    for msg in consumer:
        try:
            process_message(msg)
        except Exception as e:
            print(f"⚠️ Error: {e}")

if __name__ == "__main__":
    start_http_server(port=METRICS_PORT, addr="0.0.0.0")
    threading.Thread(target=run_consumer, daemon=True).start()
    app.run(host="0.0.0.0", port=UI_API_PORT)