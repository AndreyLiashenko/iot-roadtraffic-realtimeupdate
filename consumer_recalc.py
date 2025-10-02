import json, time, threading, psycopg2, networkx as nx
from kafka import KafkaConsumer, errors
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify
from datetime import datetime, timedelta
from flask_cors import CORS

# --- Settings ---
KAFKA_TOPIC, GRAPH_FILE = 'traffic_updates', "irpin_drive_graph.graphml"
KAFKA_BOOTSTRAP_SERVERS, API_PORT = 'kafka:29092', 5002 # Unique port
DB_HOST, DB_NAME, DB_USER, DB_PASSWORD = "postgres", "traffic_db", "user", "password"

# --- Global graph state ---
live_graph = nx.read_graphml(GRAPH_FILE)
graph_lock = threading.Lock()
for u, v, data in live_graph.edges(data=True):
    data['current_speed'] = 50.0

# --- Reusable Kafka Connection Function ---
def connect_to_kafka_with_retry():
    max_retries = 10
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            print(f"DB-Recalc Consumer: Attempting to connect to Kafka (Attempt {attempt + 1}/{max_retries})...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset='earliest',
                group_id='recalc-db-writer-group', key_deserializer=lambda k: k.decode('utf-8'),
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            print("✅ DB-Recalc Consumer: Kafka Consumer connected.")
            return consumer
        except errors.NoBrokersAvailable:
            print(f"❌ DB-Recalc Consumer: No brokers available.")
        except Exception as e:
            print(f"❌ DB-Recalc Consumer: An unexpected error occurred: {e}")
        print(f"DB-Recalc Consumer: Retrying in {retry_delay} seconds...")
        time.sleep(retry_delay)
    print("❌ DB-Recalc Consumer: Failed to connect to Kafka. Exiting.")
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

# --- DB & Recalculation Logic ---
def get_db_connection():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

def setup_database():
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS traffic_data (
                id SERIAL PRIMARY KEY, edge_key VARCHAR(255) NOT NULL, speed REAL NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_timestamp ON traffic_data (created_at);
        """)
    print("✅ Database table setup complete.")

def recalculate_graph_from_db():
    print("🔄 [DB Recalc] Starting graph recalculation...")
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT edge_key, AVG(speed) FROM traffic_data
            WHERE created_at > NOW() - INTERVAL '5 minutes' GROUP BY edge_key;
        """)
        results = cur.fetchall()
    with graph_lock:
        for row in results:
            edge_key_str, avg_speed = row
            try:
                u, v = eval(edge_key_str)
                live_graph.edges[u, v, 0]['current_speed'] = avg_speed
            except (KeyError, SyntaxError): pass
    print(f"✅ [DB Recalc] Graph recalculated. Updated {len(results)} edges.")

# --- Kafka Consumer Logic ---
def run_kafka_to_db_writer():
    consumer = connect_to_kafka_with_retry()
    if not consumer: return
    print("🚀 DB-Recalc writer is now logging messages to PostgreSQL...")
    with get_db_connection() as conn, conn.cursor() as cur:
        for message in consumer:
            cur.execute("INSERT INTO traffic_data (edge_key, speed) VALUES (%s, %s)",
                        (message.key, message.value['speed']))
            conn.commit()

if __name__ == "__main__":
    setup_database()
    scheduler = BackgroundScheduler(daemon=True)
    # --- THIS LINE IS FIXED ---
    scheduler.add_job(recalculate_graph_from_db, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler.start()
    
    api_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=API_PORT), daemon=True)
    api_thread.start()
    
    run_kafka_to_db_writer()