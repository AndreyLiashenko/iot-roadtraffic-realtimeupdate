import networkx as nx

class EdgeMapper:
    def __init__(self, graph_file: str):
        print(f"📦 Loading graph from {graph_file} ...")
        self.graph = nx.read_graphml(graph_file)
        
        self.edge_id_map = {}   # (u, v, key) → edge_id
        self.id_edge_map = {}   # edge_id → (u, v, key)

        edge_counter = 0
        for u, v, k in self.graph.edges(keys=True):
            edge_id = f"e{edge_counter}"
            self.edge_id_map[(u, v, k)] = edge_id
            self.id_edge_map[edge_id] = (u, v, k)
            edge_counter += 1

        print(f"✅ EdgeMapper initialized: {len(self.edge_id_map)} edges mapped.")

    def get_edge_id(self, u, v, k=0):
        return self.edge_id_map.get((u, v, k))

    def get_edge_tuple(self, edge_id):
        return self.id_edge_map.get(edge_id)

    def get_all_edges(self):
        return list(self.edge_id_map.keys())
