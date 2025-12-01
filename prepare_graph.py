import osmnx as ox
import networkx as nx

def prepare_and_save_graph():
    """
    Завантажує дорожню мережу, додає ШВИДКОСТІ та ЧАС проїзду,
    і зберігає у форматі GraphML.
    """

    place_name = "Irpin, Ukraine"
    output_filename = "irpin_drive_graph.graphml"

    print(f"🌍 Завантаження дорожньої мережі для: '{place_name}'...")

    graph = ox.graph_from_place(place_name, network_type='drive')
    
    print(f"✅ Топологія завантажена. Вузлів: {len(graph.nodes)}, Ребер: {len(graph.edges)}")
    print("🚗 Обробка лімітів швидкості (add_edge_speeds)...")
    graph = ox.add_edge_speeds(graph)

    print("⏱️ Розрахунок часу проїзду (add_edge_travel_times)...")
    graph = ox.add_edge_travel_times(graph)

    print(f"💾 Збереження у '{output_filename}'...")
    ox.save_graphml(graph, filepath=output_filename)

    print("\n🔍 Перевірка даних перших 3-х ребер:")
    for i, (u, v, data) in enumerate(graph.edges(data=True)):
        if i >= 3: break
        print(f"   Edge {u}->{v}: speed_kph={data.get('speed_kph')}, length={data.get('length')}")

    print(f"\n🎉 Граф успішно оновлено та збережено!")

if __name__ == "__main__":
    prepare_and_save_graph()