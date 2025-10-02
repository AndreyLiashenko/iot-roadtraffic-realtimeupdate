import requests
import networkx as nx
import random
import time
import threading
import argparse

API_URL = "http://localhost:8001/update-traffic"
GRAPH_FILE = "irpin_drive_graph.graphml"


class TrafficManager:
    """A thread-safe class for managing traffic state on graph edges."""
    def __init__(self, graph):
        self._edge_states = {}
        self._lock = threading.Lock()  # Lock for thread-safe access

        # Initialize state for each edge
        for u, v, key in graph.edges(keys=True):
            edge_id = (u, v, key)
            
            base_speed = 50.0
            capacity = 10 + random.randint(-5, 5)

            self._edge_states[edge_id] = {
                "car_count": 0,
                "base_speed": base_speed,
                "current_speed": base_speed,
                "capacity": max(1, capacity),
                "incident": False
            }

    def _recalculate_speed(self, edge_id):
        """Recalculates the speed on an edge based on its load."""
        state = self._edge_states[edge_id]
        if state["incident"]:
            state["current_speed"] = 5.0  # Speed during an incident
            return

        load_factor = state["car_count"] / state["capacity"]
        speed = state["base_speed"] * (1 - min(load_factor, 1.0))
        state["current_speed"] = max(speed, 5.0)  # Speed cannot be less than 5 km/h

    def enter_edge(self, edge_id):
        """A car enters an edge."""
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["car_count"] += 1
                self._recalculate_speed(edge_id)

    def leave_edge(self, edge_id):
        """A car leaves an edge."""
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["car_count"] = max(0, self._edge_states[edge_id]["car_count"] - 1)
                self._recalculate_speed(edge_id)

    def get_current_speed(self, edge_id):
        """Gets the current speed on an edge."""
        with self._lock:
            return self._edge_states.get(edge_id, {}).get("current_speed", 50.0)

    def cause_incident(self, edge_id, duration_seconds):
        """Creates an incident (traffic jam) on an edge for a specific duration."""
        print(f"🚨 ATTENTION! Incident on edge {edge_id} for {duration_seconds} seconds.")
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["incident"] = True
                self._recalculate_speed(edge_id)
        
        threading.Timer(duration_seconds, self._clear_incident, args=[edge_id]).start()

    def _clear_incident(self, edge_id):
        print(f"✅ Incident on edge {edge_id} has been cleared.")
        with self._lock:
            if edge_id in self._edge_states:
                self._edge_states[edge_id]["incident"] = False
                self._recalculate_speed(edge_id)


def simulate_car(car_id, graph, nodes_list, traffic_manager):
    """Simulates the movement of a single car interacting with the TrafficManager."""
    start_node = random.choice(nodes_list)
    end_node = random.choice(nodes_list)
    
    try:
        path = nx.shortest_path(graph, source=start_node, target=end_node, weight='length')
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return

    print(f"🚗 Car {car_id}: Starting trip from {start_node} to {end_node}.")

    for i in range(len(path) - 1):
        u, v = path[i], path[i+1]
        edge_id = (u, v, 0)
        
        traffic_manager.enter_edge(edge_id)
        try:
            edge_length_meters = graph.get_edge_data(u, v, 0).get('length', 500)
            
            elapsed_time = 0
            while True:
                current_speed_kmh = traffic_manager.get_current_speed(edge_id)
                current_speed_mps = current_speed_kmh / 3.6
                
                distance_left = edge_length_meters * (1 - (elapsed_time / (edge_length_meters / (current_speed_mps if current_speed_mps > 0 else 0.1) if elapsed_time > 0 else 1)))
                time_to_travel_left = distance_left / current_speed_mps if current_speed_mps > 0 else float('inf')

                if time_to_travel_left <= 5:
                    time.sleep(max(0, time_to_travel_left))
                    break

                traffic_data = {"edge": (u, v), "speed": current_speed_kmh, "timestamp": time.time()}
                try:
                    requests.post(API_URL, json=traffic_data)
                except requests.exceptions.ConnectionError:
                    print("❌ API connection error.")
                    break
                
                time.sleep(5)
                elapsed_time += 5
        finally:
            traffic_manager.leave_edge(edge_id)


def incident_simulation(traffic_manager):
    """A separate thread that creates random incidents."""
    time.sleep(30) # Wait for some traffic to build up
    all_edges = list(traffic_manager._edge_states.keys())
    if not all_edges: return

    incident_edge = random.choice(all_edges)
    incident_duration = 60  # seconds
    traffic_manager.cause_incident(incident_edge, incident_duration)


def main(num_cars, with_incident):
    graph = nx.read_graphml(GRAPH_FILE)

    numeric_attrs = ["length", "speed_kph", "eta"]

    for u, v, data in graph.edges(data=True):
        for attr in numeric_attrs:
            if attr in data:
                try:
                    data[attr] = float(data[attr])
                except ValueError:
                    print(f"⚠️ Попередження: ребро ({u}, {v}) має некоректне значення {attr}='{data[attr]}'")

    nodes_list = list(graph.nodes)

    traffic_manager = TrafficManager(graph)

    threads = []
    print(f"Запуск симуляції для {num_cars} автомобілів...")

    for i in range(num_cars):
        thread = threading.Thread(
            target=simulate_car,
            args=(i + 1, graph, nodes_list, traffic_manager)
        )
        threads.append(thread)
        thread.start()

    if with_incident:
        incident_thread = threading.Thread(
            target=incident_simulation,
            args=(traffic_manager, graph)
        )
        incident_thread.start()
        threads.append(incident_thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A realistic traffic simulator with a state manager.")
    parser.add_argument("-k", "--cars", type=int, default=50, help="Number of cars to simulate.")
    parser.add_argument("--incident", action='store_true', help="Simulate a random incident during the simulation.")
    args = parser.parse_args()
    
    main(args.cars, args.incident)