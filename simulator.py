import requests
import networkx as nx
import random
import time
import threading
import argparse
from requests.adapters import HTTPAdapter, Retry

API_URL = "http://localhost:8001/update-traffic"
GRAPH_FILE = "irpin_drive_graph.graphml"

# ============================================================
# 🔹 Custom HTTP adapter with detailed retry logging
# ============================================================
class LoggingHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        retries: Retry = self.max_retries
        if retries is not None and hasattr(retries, "increment"):
            original_increment = retries.increment

            def increment_with_logging(method, url, *args, **kw):
                retry_num = kw.get("total", 0)
                reason = kw.get("error") or "unknown"
                backoff = kw.get("backoff", 0)
                if retry_num is not None and retry_num < self.max_retries.total:
                    attempt = self.max_retries.total - retry_num
                    print(
                        f"🔁 Retry attempt #{attempt} for {url} "
                        f"(reason: {reason}) — next retry in {backoff:.1f}s"
                    )
                return original_increment(method, url, *args, **kw)

            retries.increment = increment_with_logging

        return super().send(request, **kwargs)


# ============================================================
# 🔹 Global HTTP session with retries & connection pooling
# ============================================================
session = requests.Session()
retries = Retry(
    total=3,  # кількість спроб
    backoff_factor=0.5,  # 0.5s, 1s, 2s
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
adapter = LoggingHTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
session.mount("http://", adapter)
session.mount("https://", adapter)


# ============================================================
# 🔹 Traffic state manager
# ============================================================
class TrafficManager:
    """Thread-safe class for managing traffic state on graph edges."""
    def __init__(self, graph):
        self._edge_states = {}
        self._lock = threading.Lock()

        for u, v, key in graph.edges(keys=True):
            edge_id = (u, v, key)
            base_speed = 50.0
            capacity = 10 + random.randint(-5, 5)
            self._edge_states[edge_id] = {
                "car_count": 0,
                "base_speed": base_speed,
                "current_speed": base_speed,
                "capacity": max(1, capacity),
                "incident": False,
            }

    def _recalculate_speed(self, edge_id):
        state = self._edge_states[edge_id]
        if state["incident"]:
            state["current_speed"] = 5.0
            return
        load_factor = state["car_count"] / state["capacity"]
        speed = state["base_speed"] * (1 - min(load_factor, 1.0))
        state["current_speed"] = max(speed, 5.0)

    def enter_edge(self, edge_id):
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["car_count"] += 1
                self._recalculate_speed(edge_id)

    def leave_edge(self, edge_id):
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["car_count"] = max(
                    0, self._edge_states[edge_id]["car_count"] - 1
                )
                self._recalculate_speed(edge_id)

    def get_current_speed(self, edge_id):
        with self._lock:
            return self._edge_states.get(edge_id, {}).get("current_speed", 50.0)

    def cause_incident(self, edge_id, duration_seconds):
        print(f"🚨 Incident on edge {edge_id} for {duration_seconds}s")
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["incident"] = True
                self._recalculate_speed(edge_id)

        threading.Timer(duration_seconds, self._clear_incident, args=[edge_id]).start()

    def _clear_incident(self, edge_id):
        print(f"✅ Incident cleared on edge {edge_id}")
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["incident"] = False
                self._recalculate_speed(edge_id)


# ============================================================
# 🔹 HTTP helper
# ============================================================
def post_traffic_data(data):
    """Send traffic data to API with retries and timeout."""
    try:
        response = session.post(API_URL, json=data, timeout=3)
        if response.status_code != 200:
            print(f"⚠️ API responded with {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ API connection error: {e}")


# ============================================================
# 🔹 Car simulation
# ============================================================
def simulate_car(car_id, graph, nodes_list, traffic_manager):
    start_node = random.choice(nodes_list)
    end_node = random.choice(nodes_list)

    try:
        path = nx.shortest_path(graph, source=start_node, target=end_node, weight="length")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return

    print(f"🚗 Car {car_id}: Trip {start_node} → {end_node}")

    for i in range(len(path) - 1):
        u, v = path[i], path[i + 1]
        edge_id = (u, v, 0)
        traffic_manager.enter_edge(edge_id)

        try:
            edge_length = graph.get_edge_data(u, v, 0).get("length", 500)
            elapsed = 0
            while True:
                speed_kmh = traffic_manager.get_current_speed(edge_id)
                speed_mps = speed_kmh / 3.6
                if speed_mps <= 0:
                    speed_mps = 0.1

                # Approximate remaining travel time
                distance_left = edge_length * (
                    1
                    - (
                        elapsed
                        / (edge_length / speed_mps if elapsed > 0 else edge_length)
                    )
                )
                time_left = distance_left / speed_mps

                if time_left <= 5:
                    safe_sleep = max(0, time_left)
                    time.sleep(safe_sleep)
                    break

                traffic_data = {
                    "edge": (u, v),
                    "speed": speed_kmh,
                    "timestamp": time.time(),
                }
                post_traffic_data(traffic_data)
                time.sleep(5)
                elapsed += 5
        finally:
            traffic_manager.leave_edge(edge_id)


# ============================================================
# 🔹 Random incidents
# ============================================================
def incident_simulation(traffic_manager):
    time.sleep(30)
    all_edges = list(traffic_manager._edge_states.keys())
    if not all_edges:
        return
    incident_edge = random.choice(all_edges)
    traffic_manager.cause_incident(incident_edge, duration_seconds=60)


# ============================================================
# 🔹 Main entry point
# ============================================================
def main(num_cars, with_incident):
    graph = nx.read_graphml(GRAPH_FILE)
    numeric_attrs = ["length", "speed_kph", "eta"]
    for u, v, data in graph.edges(data=True):
        for attr in numeric_attrs:
            if attr in data:
                try:
                    data[attr] = float(data[attr])
                except ValueError:
                    print(f"⚠️ Edge ({u}, {v}) invalid {attr}='{data[attr]}'")

    nodes = list(graph.nodes)
    traffic_manager = TrafficManager(graph)

    print(f"🚦 Starting simulation for {num_cars} cars...")
    threads = []

    for i in range(num_cars):
        t = threading.Thread(target=simulate_car, args=(i + 1, graph, nodes, traffic_manager))
        threads.append(t)
        t.start()
        time.sleep(0.2)

    if with_incident:
        inc_thread = threading.Thread(target=incident_simulation, args=(traffic_manager,))
        inc_thread.start()
        threads.append(inc_thread)

    for t in threads:
        t.join()


# ============================================================
# 🔹 CLI entry
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Traffic simulator with retry & incident support.")
    parser.add_argument("-k", "--cars", type=int, default=50, help="Number of cars to simulate.")
    parser.add_argument("--incident", action="store_true", help="Simulate random traffic incident.")
    args = parser.parse_args()

    main(args.cars, args.incident)
