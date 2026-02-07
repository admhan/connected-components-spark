"""Implémentation optimisée de l'algorithme CCF avec persistance des RDD."""

from __future__ import annotations

import time
from pyspark import SparkContext
from pyspark.rdd import RDD


EdgeRDD = RDD[tuple[str, str]]


def load_edges(sc: SparkContext, path: str) -> EdgeRDD:
    """Charge une liste d'arêtes depuis un fichier texte."""
    return (
        sc.textFile(path)
        .map(lambda line: line.split())
        .map(lambda tokens: (tokens[0], tokens[1]))
    )


def initialize_pairs(edges: EdgeRDD) -> EdgeRDD:
    """Crée un graphe non orienté en doublant chaque arête puis déduplique."""
    return edges.flatMap(lambda edge: [(edge[0], edge[1]), (edge[1], edge[0])]).distinct()


def ccf_iteration_optimized(pairs: EdgeRDD) -> EdgeRDD:
    """Exécute une itération CCF en réutilisant les données mises en cache."""
    neighbors = pairs.groupByKey().cache()

    def propagate(node_neighbors: tuple[str, list[str]]) -> list[tuple[str, str]]:
        """Diffuse l'étiquette minimale d'une composante candidate."""
        node, neighs_iter = node_neighbors
        neighs = list(neighs_iter)
        min_id = min([node] + neighs)

        if min_id < node:
            return [(node, min_id)] + [
                (neighbor, min_id) for neighbor in neighs if neighbor != min_id
            ]
        return []

    return neighbors.flatMap(propagate)


def compute_ccf_optimized(
    sc: SparkContext,
    edge_path: str,
    max_iter: int = 50,
) -> EdgeRDD:
    """Calcule les composantes connexes avec la variante optimisée du CCF."""
    edges = load_edges(sc, edge_path)
    pairs = initialize_pairs(edges).cache()

    iteration = 0
    changed = True
    start_time = time.time()

    while changed and iteration < max_iter:
        iteration += 1

        # Les nouvelles paires sont mises en cache pour éviter des recomputations.
        new_pairs = ccf_iteration_optimized(pairs).distinct().cache()
        diff = new_pairs.subtract(pairs)
        changed = not diff.isEmpty()

        # Libération explicite de l'ancienne version pour limiter l'empreinte mémoire.
        pairs.unpersist()
        pairs = new_pairs

        print(f"[Optimized] Iteration {iteration} finished")

    print(f"[Optimized] Converged in {iteration} iterations")
    print(f"[Optimized] Execution time: {time.time() - start_time:.2f}s")

    return pairs


if __name__ == "__main__":
    spark_context = SparkContext(appName="CCF_Optimized")
    result = compute_ccf_optimized(spark_context, "data/small_graph.txt")
    result.collect()
    spark_context.stop()
