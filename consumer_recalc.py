import json
import time
import threading
import signal
import sys
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaConsumer, errors
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify
from flask_cors import CORS
from prometheus_client import Summary, start_http_server
from csr_graph import CSRGraph
from edge_mapper import EdgeMapper

# ============================================================
# --- Settings ---
# ============================================================
SERVICE_NAME = "recalc-consumer"
KAFKA_TOPIC = "traffic_updates"
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
GRAPH_FILE = "irpin_drive_graph.graphml"
UI_API_PORT = 5002
METRICS_PORT = 9102
DB_HOST, DB_NAME, DB_USER, DB_PASSWORD = "postgres", "traffic_db", "user", "password"

# ============================================================
# --- Prometheus Metrics ---
# ============================================================
DB_WRITE_TIME = Summary("db_write_seconds", "Time spent writing message to DB", ["consumer_name"])

# ============================================================
# --- Graph initialization ---
# ============================================================
mapper = EdgeMapper(GRAPH_FILE)
csr_graph = CSRGraph(mapper)

# ============================================================
# --- Flask UI ---
# ============================================================
app = Flask(__name__)
CORS(app)
app.config["GRAPH"] = csr_graph

@app.route("/graph")
def get_graph_data():
    """Return current graph data (for UI visualization)."""
    return jsonify(app.config["GRAPH"].get_graph_data())

# ============================================================
# --- Database Setup ---
# ============================================================
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5
    )

def setup_database():
    """Create historical traffic_data table if missing."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS traffic_data (
                id BIGSERIAL PRIMARY KEY,
                edge_key TEXT NOT NULL,
                speed DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_traffic_data_edge_key ON traffic_data (edge_key);
            CREATE INDEX IF NOT EXISTS idx_traffic_data_created_at ON traffic_data (created_at DESC);
        """)
        conn.commit()
    print("✅ Database ready with history schema.")

# ============================================================
# --- Kafka Connection with Retry ---
# ============================================================
def connect_to_kafka_with_retry(group_id: str):
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                group_id=group_id,
                enable_auto_commit=True,
                key_deserializer=lambda k: k.decode("utf-8"),
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print(f"✅ [{SERVICE_NAME}] Kafka connected.")
            print(f"📡 Subscribed to topic: {KAFKA_TOPIC}")
            print(f"📊 Assigned partitions: {consumer.partitions_for_topic(KAFKA_TOPIC)}")
            return consumer
        except errors.NoBrokersAvailable as e:
            print(f"❌ [{SERVICE_NAME}] Kafka unavailable (attempt {attempt}/10): {e}")
            time.sleep(5)
    print("❌ Failed to connect to Kafka after 10 retries.")
    return None

# ============================================================
# --- Periodic Recalculation Logic ---
# ============================================================
def recalculate_graph_from_db():
    """Recalculate average speeds per edge for the last minute."""
    print("🔄 [DB Recalc] Updating graph from recent data...")
    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT edge_key, AVG(speed)
                FROM traffic_data
                WHERE created_at > NOW() - INTERVAL '1 minute'
                GROUP BY edge_key;
            """)
            results = cur.fetchall()

        csr_graph.reset_speeds()
        for edge_key, avg_speed in results:
            csr_graph.update_speed(edge_key, avg_speed)

        print(f"✅ [DB Recalc] Updated {len(results)} edges.")
    except Exception as e:
        print(f"⚠️ [DB Recalc] Error while recalculating graph: {e}")

# ============================================================
# --- Kafka → DB Writer ---
# ============================================================
@DB_WRITE_TIME.labels(consumer_name="db_recalc").time()
def write_message_to_db(cursor, msg):
    """Insert new speed measurement into historical table."""
    cursor.execute("""
        INSERT INTO traffic_data (edge_key, speed, created_at)
        VALUES (%s, %s, NOW());
    """, (msg.key, msg.value["speed"]))

def run_kafka_to_db_writer():
    """Continuously read messages from Kafka and write them to DB."""
    consumer = connect_to_kafka_with_retry("recalc-db-writer-group")
    if not consumer:
        return

    print("🚀 [DB Writer] Started consuming messages.")
    with get_db_connection() as conn, conn.cursor() as cur:
        for msg in consumer:
            try:
                print(f"📥 Received message: {msg.key} → {msg.value}")
                write_message_to_db(cur, msg)
                conn.commit()
            except psycopg2.Error as db_err:
                conn.rollback()
                print(f"⚠️ [DB Writer] DB error: {db_err.pgerror or db_err}")
            except Exception as e:
                print(f"⚠️ [DB Writer] General error: {e}")

# ============================================================
# --- Graceful Shutdown ---
# ============================================================
def handle_shutdown(signum, frame):
    print("\n🛑 Graceful shutdown initiated.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# ============================================================
# --- Main Entrypoint ---
# ============================================================
if __name__ == "__main__":
    print(f"🚀 Starting {SERVICE_NAME} service...")
    setup_database()

    # Start Prometheus metrics endpoint
    start_http_server(port=METRICS_PORT, addr="0.0.0.0")

    # Scheduler for recalculation (every 1 minute)
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        recalculate_graph_from_db,
        trigger="interval",
        minutes=1,
        next_run_time=datetime.now() + timedelta(seconds=20)
    )
    scheduler.start()

    # Kafka → DB thread
    threading.Thread(target=run_kafka_to_db_writer, daemon=True).start()

    # Flask UI thread
    app.run(host="0.0.0.0", port=UI_API_PORT)