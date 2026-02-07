from pyspark import SparkContext
import time


def load_edges(sc, path):
    """
    Load an edge list from a text file.
    Format: u v
    """
    return sc.textFile(path) \
             .map(lambda line: line.split()) \
             .map(lambda x: (x[0], x[1]))


def initialize_pairs(edges):
    """
    Symmetrize the graph.
    """
    return edges.flatMap(lambda e: [(e[0], e[1]), (e[1], e[0])])


def ccf_iteration(pairs):
    """
    One iteration of the Connected Component Finder algorithm.
    """
    neighbors = pairs.groupByKey()

    def propagate(node_neighbors):
        node, neighs = node_neighbors
        neighs = list(neighs)
        min_id = min([node] + neighs)

        emitted = []
        if min_id < node:
            emitted.append((node, min_id))
            for n in neighs:
                if n != min_id:
                    emitted.append((n, min_id))
        return emitted

    return neighbors.flatMap(propagate)


def compute_ccf(sc, edge_path, max_iter=50):
    edges = load_edges(sc, edge_path)
    pairs = initialize_pairs(edges).distinct()

    iteration = 0
    changed = True
    start = time.time()

    while changed and iteration < max_iter:
        iteration += 1

        new_pairs = ccf_iteration(pairs).distinct()
        diff = new_pairs.subtract(pairs)
        changed = not diff.isEmpty()

        pairs = new_pairs
        print(f"Iteration {iteration} finished")

    print(f"Converged in {iteration} iterations")
    print(f"Execution time: {time.time() - start:.2f}s")

    return pairs


if __name__ == "__main__":
    sc = SparkContext(appName="CCF_Basic")
    result = compute_ccf(sc, "data/small_graph.txt")
    result.collect()
    sc.stop()
