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
from evolving_fuzzy import EvolvingFuzzyClusterer

# -----------------------------
# Settings
# -----------------------------
SERVICE_NAME = "sut-fuzzy-consumer"
KAFKA_TOPIC = "traffic_updates"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
GRAPH_FILE = "irpin_drive_graph.graphml"
UI_API_PORT = 5004
METRICS_PORT = 9104
BATCH_SIZE = 5000

# -----------------------------
# Prometheus Metric
# -----------------------------
PROCESSING_TIME = Summary(
    "message_processing_seconds",
    "Time spent processing a message",
    ["consumer_name"]
)

# -----------------------------
# Globals
# -----------------------------
batch_X = []
event_counter = 0

# -----------------------------
# Load Graph
# -----------------------------
print(f"[{SERVICE_NAME}] Initializing graph...")
mapper = EdgeMapper(GRAPH_FILE)
csr_graph = CSRGraph(mapper)
print(f"✅ [{SERVICE_NAME}] Graph loaded with {len(mapper.edge_id_map)} edges.")

# -----------------------------
# Initialize Clusterer
# -----------------------------
clusterer = EvolvingFuzzyClusterer(dim=3)

# -----------------------------
# Flask UI
# -----------------------------
app = Flask(__name__)
CORS(app)

app.config["GRAPH"] = csr_graph
app.config["CLUSTERER"] = clusterer


# ----------------------------------------------------
# REST ENDPOINTS for D3.js Cluster Dashboard
# ----------------------------------------------------

@app.route("/graph")
def get_graph_data():
    graph = app.config["GRAPH"]
    return jsonify(graph.get_graph_data())

@app.route("/monitor")
def serve_monitor():
    return send_from_directory('ui', 'monitor.html')

@app.route("/monitor3d")
def monitor_3d_page():
    return send_from_directory('ui', 'monitor_3d.html')

@app.route("/clusters")
def get_clusters():
    c = clusterer
    return jsonify({
        "centers": c.centers.tolist(),
        "sigmas": c.sigmas.tolist(),
        "alphas": c.alphas.tolist(),
        "weights": c.weights.tolist(),
        "k": int(c.centers.shape[0])
    })


@app.route("/clusters_scatter")
def clusters_scatter():
    """Return 2D PCA projection for D3.js scatter plot."""
    c = clusterer

    if c.centers.shape[0] < 1:
        return jsonify({"pca": [], "sigmas": [], "alphas": []})

    centers = c.centers
    sigmas = c.sigmas.tolist()
    alphas = c.alphas.tolist()

    try:
        if centers.shape[0] >= 2:
            pca = PCA(n_components=2)
            C2 = pca.fit_transform(centers).tolist()
        else:
            C2 = [[0, 0]]
    except Exception:
        C2 = [[0, 0] for _ in range(len(sigmas))]

    return jsonify({
        "pca": C2,
        "sigmas": sigmas,
        "alphas": alphas
    })


@app.route("/clusters_summary")
def summary():
    c = clusterer
    return jsonify({
        "num_clusters": int(c.centers.shape[0]),
        "avg_sigma": float(np.mean(c.sigmas)) if len(c.sigmas) else 0,
        "avg_alpha": float(np.mean(c.alphas)) if len(c.alphas) else 0
    })


# -----------------------------
# Kafka Logic
# -----------------------------
def connect_with_retry(group_id):
    for attempt in range(10):
        try:
            print(f"[{SERVICE_NAME}] Connecting to Kafka ({attempt+1}/10)...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id=group_id,
                key_deserializer=lambda k: k.decode("utf-8"),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            print("✅ Kafka connected")
            return consumer
        except errors.NoBrokersAvailable:
            print("❌ Kafka unavailable, retrying...")
            time.sleep(5)
    return None


# -----------------------------
# Message Processing
# -----------------------------
@PROCESSING_TIME.labels(consumer_name="fuzzy_sut").time()
def process_fuzzy_message(msg):
    global batch_X, event_counter

    try:
        event_counter += 1

        data = msg.value
        edge_id = msg.key

        speed = data["speed"]
        load_factor = data.get("load_factor", 0.0)
        density = data.get("data_density_60s", 0.0)

        x = [speed, load_factor, density]
        batch_X.append(x)

        # Log every 500 events
        if event_counter % 100 == 0:
            print(
                f"[MSG#{event_counter}] "
                f"edge={edge_id}, s={speed:.1f}, load={load_factor:.2f}, d={density:.1f}, "
                f"batch={len(batch_X)}/{BATCH_SIZE}, clusters={len(clusterer.centers)}"
            )

        # ---------- Batch update ----------
        if len(batch_X) >= BATCH_SIZE:
            print(f"\n[CLUSTER] Running update_batch on {len(batch_X)} samples...")

            X_np = np.asarray(batch_X, dtype=float)
            clusterer.update_batch(X_np)

            print(f"[CLUSTER] Now K={len(clusterer.centers)} clusters\n")
            batch_X = []  # reset

        # ---------- Inference ----------
        alpha = clusterer.infer_alpha(x)
        prev_speed = csr_graph.get_speed(edge_id)
        updated = alpha * speed + (1 - alpha) * prev_speed
        csr_graph.update_speed(edge_id, updated)

        if event_counter % 100 == 0:
            print(f"[SMOOTH] prev={prev_speed:.1f}, new={speed:.1f}, α={alpha:.3f}, updated={updated:.1f}")

    except Exception as e:
        print(f"⚠️ Error: {e}")


# -----------------------------
# Consumer Thread
# -----------------------------
def run_consumer():
    consumer = connect_with_retry("sut-fuzzy-group")
    if not consumer:
        print("❌ Cannot start consumer")
        return

    print(f"🚀 [{SERVICE_NAME}] Consuming messages...")
    for msg in consumer:
        process_fuzzy_message(msg)


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    print(f"📊 Starting metrics at {METRICS_PORT}...")
    start_http_server(port=METRICS_PORT, addr="0.0.0.0")

    print(f"🚀 Starting consumer + UI at {UI_API_PORT}...")
    threading.Thread(target=run_consumer, daemon=True).start()

    app.run(host="0.0.0.0", port=UI_API_PORT)