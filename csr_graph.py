import numpy as np

class CSRGraph:
    def __init__(self, mapper):
        self.mapper = mapper
        self.graph = mapper.graph

        self.edge_speeds = {eid: 50.0 for eid in self.mapper.id_edge_map.keys()}

        print(f"✅ CSRGraph initialized with {len(self.edge_speeds)} edges, {len(self.graph.nodes)} nodes.")

    def update_speed(self, edge_id, speed):
        if edge_id in self.edge_speeds:
            self.edge_speeds[edge_id] = speed

    def get_speed(self, edge_id):
        return self.edge_speeds.get(edge_id, 50.0)

    def reset_speeds(self, default=50.0):
        for eid in self.edge_speeds:
            self.edge_speeds[eid] = default

    def get_graph_data(self):
        nodes = [{"id": n, "lat": d.get("y"), "lon": d.get("x")} for n, d in self.graph.nodes(data=True)]
        edges = []

        for edge_id, (u, v, k) in self.mapper.id_edge_map.items():
            edges.append({
                "id": edge_id,
                "source": u,
                "target": v,
                "speed": self.edge_speeds.get(edge_id, 50.0)
            })
        return {"nodes": nodes, "edges": edges}
