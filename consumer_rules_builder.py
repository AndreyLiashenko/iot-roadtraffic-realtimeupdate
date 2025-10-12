import os
import json
import time
import math
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime


# ==========================================================
# ⚙️ ENVIRONMENT CONFIG
# ==========================================================
SERVICE_NAME = "rules-builder-consumer"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "traffic_updates")
RULES_TOPIC = os.getenv("RULES_TOPIC", "alpha_rules")
GROUP_ID = os.getenv("GROUP_ID", "rules-builder-group")

N_CLUSTERS = int(os.getenv("N_CLUSTERS", 6))
BUILD_EVERY_N_EVENTS = int(os.getenv("BUILD_EVERY_N_EVENTS", 200000))
BUILD_EVERY_SECONDS = int(os.getenv("BUILD_EVERY_SECONDS", 300))
ELBOW_BUFFER_SIZE = int(os.getenv("ELBOW_BUFFER_SIZE", 20000))
ELBOW_K_MAX = int(os.getenv("ELBOW_K_MAX", 10))
ELBOW_MIN_POINTS = int(os.getenv("ELBOW_MIN_POINTS", 2000))

RULES_JSON_PATH = os.getenv("RULES_JSON_PATH", "/app/alpha_rules.json")
ALPHA_MIN = float(os.getenv("ALPHA_MIN", 0.3))
ALPHA_MAX = float(os.getenv("ALPHA_MAX", 0.9))


# ==========================================================
# 🧠 HELPERS
# ==========================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"{now_str()} [{SERVICE_NAME}] {msg}", flush=True)

def detect_elbow_point(sse):
    """Geometric elbow detection (k vs SSE)."""
    n_points = len(sse)
    if n_points < 3:
        return None
    coords = np.vstack((range(1, n_points + 1), sse)).T
    p1, p2 = coords[0], coords[-1]
    line_vec = p2 - p1
    line_vec_norm = line_vec / np.sqrt(np.sum(line_vec ** 2))
    vec_from_first = coords - p1
    scalar_proj = np.dot(vec_from_first, line_vec_norm)
    proj = np.outer(scalar_proj, line_vec_norm)
    dist = np.sqrt(np.sum((vec_from_first - proj) ** 2, axis=1))
    return int(np.argmax(dist) + 1)


# ==========================================================
# 🧮 ONLINE CLUSTER STATE
# ==========================================================
class OnlineClusterState:
    def __init__(self):
        self.buffer = []

    def add_sample(self, x):
        """Add new 3D sample (speed, load_factor, data_density)."""
        # Skip invalid or zero samples
        if any(math.isnan(v) or v <= 0.0 for v in x):
            return
        self.buffer.append(x)
        if len(self.buffer) > ELBOW_BUFFER_SIZE:
            self.buffer.pop(0)

    def compute_elbow(self):
        """Compute optimal K and return trained KMeans model."""
        if len(self.buffer) < ELBOW_MIN_POINTS:
            log(f"⚠️ Not enough data for Elbow: {len(self.buffer)}/{ELBOW_MIN_POINTS}")
            return None

        X = np.array(self.buffer)
        # Normalize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        sse = []
        for k in range(1, ELBOW_K_MAX + 1):
            km = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(X_scaled)
            sse.append(km.inertia_)

        k_opt = detect_elbow_point(sse)
        if not k_opt or k_opt < 2:
            k_opt = min(3, ELBOW_K_MAX)
        log(f"[Elbow] SSE={np.round(sse, 2).tolist()}, selected k={k_opt}")

        # Apply time-based weights (newer samples weigh more)
        weights = np.exp(-0.001 * np.arange(len(X_scaled))[::-1])

        model = KMeans(n_clusters=k_opt, random_state=42, n_init="auto").fit(X_scaled, sample_weight=weights)
        return model, X_scaled, scaler


# ==========================================================
# 🧩 RULES BUILDER APP
# ==========================================================
class RulesBuilderApp:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.state = OnlineClusterState()
        self.last_build_time = 0
        self.event_count = 0

    def connect_kafka(self):
        log("Connecting consumer...")
        self.consumer = KafkaConsumer(
            SOURCE_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        log("✅ Consumer connected.")

        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        log("✅ Producer connected.")

    def run(self):
        log(f"🚀 Consuming from '{SOURCE_TOPIC}'...")
        for msg in self.consumer:
            self.handle_message(msg.value)

    def handle_message(self, data):
        """Process Kafka message and add sample."""
        try:
            speed = float(data.get("speed", 0.0))
            load_factor = float(data.get("load_factor", 0.0))
            density = float(data.get("data_density_60s", 0.0))
        except Exception:
            return

        self.state.add_sample([speed, load_factor, density])
        self.event_count += 1

        if self.event_count % 1000 == 0:
            log(f"Processed {self.event_count} messages, buffer={len(self.state.buffer)}")

        # Check time or count triggers
        if (
            len(self.state.buffer) >= ELBOW_MIN_POINTS
            and (self.event_count % BUILD_EVERY_N_EVENTS == 0 or time.time() - self.last_build_time > BUILD_EVERY_SECONDS)
        ):
            self.rebuild_rules()

    def rebuild_rules(self):
        """Perform elbow clustering and build fuzzy rules."""
        log("⚙️ Building fuzzy rules...")
        result = self.state.compute_elbow()
        if not result:
            log("⚠️ Elbow check failed (no model).")
            return

        model, X_scaled, scaler = result
        centers_scaled = model.cluster_centers_
        centers = scaler.inverse_transform(centers_scaled)

        rules = []
        cluster_sigmas = []
        max_sigma_norm = 0

        # Compute per-cluster sigmas
        for i, center in enumerate(centers_scaled):
            cluster_points = X_scaled[model.labels_ == i]
            sigma = np.std(cluster_points, axis=0) if len(cluster_points) > 0 else np.zeros_like(center)
            sigma_norm = float(np.linalg.norm(sigma))
            cluster_sigmas.append(sigma_norm)
            max_sigma_norm = max(max_sigma_norm, sigma_norm)

        sigma_mean = np.mean(cluster_sigmas) if cluster_sigmas else 1.0

        # Compute α per cluster
        for i, center in enumerate(centers):
            sigma_norm = cluster_sigmas[i]
            alpha = round(ALPHA_MIN + (ALPHA_MAX - ALPHA_MIN) * (1 - sigma_norm / (sigma_mean + 1e-6)), 3)
            rules.append({
                "center": center.tolist(),
                "sigma": np.std(X_scaled[model.labels_ == i], axis=0).tolist(),
                "alpha": alpha
            })
            log(f"📊 Cluster {i}: speed={center[0]:.2f}, load={center[1]:.2f}, density={center[2]:.2f}, α={alpha}")

        # Save to file
        with open(RULES_JSON_PATH, "w") as f:
            json.dump(rules, f, indent=2)
        log(f"💾 Rules saved to {RULES_JSON_PATH}")

        # Publish to Kafka
        self.producer.send(RULES_TOPIC, rules)
        self.producer.flush()
        log(f"📤 Rules published to topic '{RULES_TOPIC}'")

        self.last_build_time = time.time()


# ==========================================================
# 🏁 ENTRY POINT
# ==========================================================
def main():
    app = RulesBuilderApp()
    app.connect_kafka()
    app.run()


if __name__ == "__main__":
    main()