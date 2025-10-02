import osmnx as ox
import networkx as nx

def prepare_and_save_graph():
    """
    Завантажує дорожню мережу для заданого району міста,
    спрощує її та зберігає у файл формату GraphML.
    """

    place_name = "Irpin, Ukraine"
    output_filename = "irpin_drive_graph.graphml"

    print(f"Завантаження дорожньої мережі для: '{place_name}'...")

    graph = ox.graph_from_place(place_name, network_type='drive')

    print(f"Мережа завантажена. Кількість вузлів: {len(graph.nodes)}, кількість ребер: {len(graph.edges)}")

    # Зберігаємо фінальний граф у файл
    ox.save_graphml(graph, filepath=output_filename)

    print(f"Граф успішно збережено у файл: '{output_filename}'")


if __name__ == "__main__":
    prepare_and_save_graph()