from pyspark import SparkContext
import time


def load_edges(sc, path):
    return sc.textFile(path) \
             .map(lambda line: line.split()) \
             .map(lambda x: (x[0], x[1]))


def initialize_pairs(edges):
    return edges.flatMap(lambda e: [(e[0], e[1]), (e[1], e[0])]).distinct()


def ccf_iteration_optimized(pairs):
    neighbors = pairs.groupByKey().cache()

    def propagate(node_neighbors):
        node, neighs = node_neighbors
        neighs = list(neighs)
        min_id = min([node] + neighs)

        if min_id < node:
            return [(node, min_id)] + [(n, min_id) for n in neighs if n != min_id]
        return []

    return neighbors.flatMap(propagate)


def compute_ccf_optimized(sc, edge_path, max_iter=50):
    edges = load_edges(sc, edge_path)
    pairs = initialize_pairs(edges).cache()

    iteration = 0
    changed = True
    start = time.time()

    while changed and iteration < max_iter:
        iteration += 1

        new_pairs = ccf_iteration_optimized(pairs).distinct().cache()
        diff = new_pairs.subtract(pairs)
        changed = not diff.isEmpty()

        pairs.unpersist()
        pairs = new_pairs

        print(f"[Optimized] Iteration {iteration} finished")

    print(f"[Optimized] Converged in {iteration} iterations")
    print(f"[Optimized] Execution time: {time.time() - start:.2f}s")

    return pairs


if __name__ == "__main__":
    sc = SparkContext(appName="CCF_Optimized")
    result = compute_ccf_optimized(sc, "data/small_graph.txt")
    result.collect()
    sc.stop()
