import json
import time
import threading
from kafka import KafkaConsumer, errors
from flask import Flask, jsonify
from flask_cors import CORS
from prometheus_client import Summary, start_http_server
from csr_graph import CSRGraph
from edge_mapper import EdgeMapper

# --- Settings ---
SERVICE_NAME = "sut-consumer"
KAFKA_TOPIC = "traffic_updates"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
GRAPH_FILE = "irpin_drive_graph.graphml"
UI_API_PORT = 5001
METRICS_PORT = 9101
ALPHA = 0.7

# --- Prometheus Metrics ---
PROCESSING_TIME = Summary(
    "message_processing_seconds",
    "Time spent processing a message",
    ["consumer_name"]
)

# --- Graph + Mapper Initialization ---
print(f"[{SERVICE_NAME}] Initializing graph...")
mapper = EdgeMapper(GRAPH_FILE)
csr_graph = CSRGraph(mapper)
print(f"✅ [{SERVICE_NAME}] Graph loaded with {len(mapper.edge_id_map)} edges.")

# --- Flask UI ---
app = Flask(__name__)
CORS(app)
app.config["GRAPH"] = csr_graph

@app.route("/graph")
def get_graph_data():
    graph = app.config["GRAPH"]
    return jsonify(graph.get_graph_data())

# --- Kafka Logic ---
def connect_to_kafka_with_retry(group_id):
    """Tries to connect to Kafka with multiple retries."""
    for attempt in range(10):
        try:
            print(f"[{SERVICE_NAME}] Connecting to Kafka (attempt {attempt + 1}/10)...")
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
            print(f"❌ [{SERVICE_NAME}] Kafka unavailable, retrying in 5s...")
            time.sleep(5)
    return None

@PROCESSING_TIME.labels(consumer_name="sut").time()
def process_sut_message(message):
    """Processes a single Kafka message (Exponential Smoothing)."""
    try:
        data = message.value
        edge_id = message.key  # key = unique edge_id
        current_speed = csr_graph.get_speed(edge_id)
        updated_speed = (ALPHA * data["speed"]) + (1 - ALPHA) * current_speed
        csr_graph.update_speed(edge_id, updated_speed)
    except Exception as e:
        print(f"⚠️ Error processing message: {e}")

def run_consumer():
    consumer = connect_to_kafka_with_retry("sut-consumer-group")
    if not consumer:
        print(f"❌ [{SERVICE_NAME}] Failed to start consumer.")
        return

    print(f"🚀 [{SERVICE_NAME}] Consuming messages...")
    for msg in consumer:
        process_sut_message(msg)

# --- Entry Point ---
if __name__ == "__main__":
    print(f"📊 [{SERVICE_NAME}] Starting Prometheus metrics server on port {METRICS_PORT}...")
    start_http_server(port=METRICS_PORT, addr="0.0.0.0")

    print(f"🚀 [{SERVICE_NAME}] Starting consumer and Flask UI...")
    threading.Thread(target=run_consumer, daemon=True).start()
    app.run(host="0.0.0.0", port=UI_API_PORT)