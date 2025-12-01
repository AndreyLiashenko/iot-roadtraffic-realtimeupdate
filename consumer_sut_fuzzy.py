import json
import time
import threading
from kafka import KafkaConsumer, errors
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from prometheus_client import Summary, start_http_server
import numpy as np
from sklearn.decomposition import PCA

from csr_graph import CSRGraph
from edge_mapper import EdgeMapper
from adaptive_phase_clusterer import AdaptivePhaseSpaceClusterer

SERVICE_NAME = "sut-fuzzy-consumer"
KAFKA_TOPIC = "traffic_updates"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
GRAPH_FILE = "irpin_drive_graph.graphml"
UI_API_PORT = 5004
METRICS_PORT = 9104
BATCH_SIZE = 25000

PROCESSING_TIME = Summary("message_processing_seconds", "Time spent", ["consumer_name"])

batch_X = []
event_counter = 0

print(f"[{SERVICE_NAME}] Initializing graph...")
mapper = EdgeMapper(GRAPH_FILE)
csr_graph = CSRGraph(mapper)
print(f"✅ [{SERVICE_NAME}] Graph loaded.")

# Initialize Clusterer
print(f"[{SERVICE_NAME}] Initializing Adaptive Phase Space Clusterer...")
clusterer = AdaptivePhaseSpaceClusterer(
    dim=3,
    dimension_weights=[1.0, 1.0, 2.5], 
    min_sigma_vec=[0.05, 0.2, 0.03], # Golden Standard
)

app = Flask(__name__)
CORS(app)
app.config["GRAPH"] = csr_graph
app.config["CLUSTERER"] = clusterer

@app.route("/graph")
def get_graph_data():
    return jsonify(csr_graph.get_graph_data())

@app.route("/monitor")
def serve_monitor():
    return send_from_directory('ui', 'monitor.html')

@app.route("/monitor3d")
def monitor_3d_page():
    return send_from_directory('ui', 'monitor_3d.html')

@app.route("/clusters")
def get_clusters():
    c = clusterer
    current_time = time.time()
    return jsonify({
        "centers": c.centers.tolist(),
        "sigmas": c.sigmas.tolist(),
        "alphas": c.alphas.tolist(),
        "weights": c.weights.tolist(),
        # NEW FIELDS
        "merge_counts": [cl.get('merge_count', 0) for cl in c._clusters],
        "ages": [current_time - cl.get('created_at', current_time) for cl in c._clusters],
        "k": int(c.centers.shape[0]),
        "initialized": c.is_initialized
    })

def connect_with_retry(group_id):
    for attempt in range(10):
        try:
            print(f"[{SERVICE_NAME}] Connecting to Kafka ({attempt+1}/10)...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id=group_id,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            print("✅ Kafka connected")
            return consumer
        except errors.NoBrokersAvailable:
            time.sleep(5)
    return None

@PROCESSING_TIME.labels(consumer_name="fuzzy_sut").time()
def process_fuzzy_message(msg):
    global batch_X, event_counter
    try:
        event_counter += 1
        data = msg.value
        edge_id = msg.key.decode("utf-8") if msg.key else None

        # Extract Phase Vector
        speed_efficiency = float(data.get("speed_efficiency", 0.0))
        load_factor      = float(data.get("load_factor", 0.0))
        traffic_trend    = float(data.get("traffic_trend", 0.0))
        raw_speed        = float(data.get("speed", 0.0))

        x = [speed_efficiency, load_factor, traffic_trend]
        batch_X.append(x)

        if event_counter % 500 == 0:
            print(f"[MSG#{event_counter}] Eff={speed_efficiency:.2f} Load={load_factor:.2f} -> Batch={len(batch_X)}")

        if len(batch_X) >= BATCH_SIZE:
            print(f"\n[PHASE SPACE] Updating with {len(batch_X)} samples...")
            X_np = np.asarray(batch_X, dtype=float)
            clusterer.update_batch(X_np)
            print(f"[PHASE SPACE] Regimes: {len(clusterer.centers)}\n")
            batch_X = []

        # Inference
        alpha = clusterer.infer_alpha(x)
        prev_speed = csr_graph.get_speed(edge_id)
        updated_speed = alpha * raw_speed + (1 - alpha) * prev_speed
        csr_graph.update_speed(edge_id, updated_speed)

    except Exception as e:
        print(f"⚠️ Error: {e}")

def run_consumer():
    consumer = connect_with_retry("sut-fuzzy-group")
    if not consumer: return
    print(f"🚀 [{SERVICE_NAME}] Consuming messages...")
    for msg in consumer:
        process_fuzzy_message(msg)

if __name__ == "__main__":
    start_http_server(port=METRICS_PORT, addr="0.0.0.0")
    threading.Thread(target=run_consumer, daemon=True).start()
    app.run(host="0.0.0.0", port=UI_API_PORT)