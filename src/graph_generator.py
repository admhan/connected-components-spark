import random


def generate_graph(num_nodes, num_edges, output_path):
    edges = set()

    while len(edges) < num_edges:
        u = random.randint(0, num_nodes - 1)
        v = random.randint(0, num_nodes - 1)
        if u != v:
            edges.add((u, v))

    with open(output_path, "w") as f:
        for u, v in edges:
            f.write(f"{u} {v}\n")


if __name__ == "__main__":
    generate_graph(50, 100, "data/small_graph.txt")
    generate_graph(500, 1500, "data/medium_graph.txt")
    generate_graph(2000, 6000, "data/large_graph.txt")
