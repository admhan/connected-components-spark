"""Implémentation de base de l'algorithme CCF avec des RDD Spark."""

from __future__ import annotations

import time
from pyspark import SparkContext
from pyspark.rdd import RDD


EdgeRDD = RDD[tuple[str, str]]


def load_edges(sc: SparkContext, path: str) -> EdgeRDD:
    """Charge une liste d'arêtes orientées depuis un fichier texte.

    Chaque ligne doit suivre le format ``u v`` où ``u`` et ``v`` sont
    les identifiants de deux sommets.
    """
    return (
        sc.textFile(path)
        .map(lambda line: line.split())
        .map(lambda tokens: (tokens[0], tokens[1]))
    )


def initialize_pairs(edges: EdgeRDD) -> EdgeRDD:
    """Symétrise le graphe en ajoutant l'arête inverse pour chaque arête.

    La sortie contient les couples ``(u, v)`` et ``(v, u)`` afin de traiter
    le graphe comme non orienté.
    """
    return edges.flatMap(lambda edge: [(edge[0], edge[1]), (edge[1], edge[0])])


def ccf_iteration(pairs: EdgeRDD) -> EdgeRDD:
    """Exécute une itération de propagation du minimum local (CCF)."""
    neighbors = pairs.groupByKey()

    def propagate(node_neighbors: tuple[str, list[str]]) -> list[tuple[str, str]]:
        """Propage l'identifiant minimal observé dans un voisinage."""
        node, neighs_iter = node_neighbors
        neighs = list(neighs_iter)
        min_id = min([node] + neighs)

        emitted: list[tuple[str, str]] = []
        # Si un voisin possède un identifiant plus petit, on relie le sommet
        # courant et ses voisins à cette étiquette minimale.
        if min_id < node:
            emitted.append((node, min_id))
            for neighbor in neighs:
                if neighbor != min_id:
                    emitted.append((neighbor, min_id))
        return emitted

    return neighbors.flatMap(propagate)


def compute_ccf(sc: SparkContext, edge_path: str, max_iter: int = 50) -> EdgeRDD:
    """Calcule les composantes connexes avec la version basique du CCF."""
    edges = load_edges(sc, edge_path)
    pairs = initialize_pairs(edges).distinct()

    iteration = 0
    changed = True
    start_time = time.time()

    while changed and iteration < max_iter:
        iteration += 1

        new_pairs = ccf_iteration(pairs).distinct()
        # Le calcul converge quand aucune nouvelle paire n'est produite.
        diff = new_pairs.subtract(pairs)
        changed = not diff.isEmpty()

        pairs = new_pairs
        print(f"Iteration {iteration} finished")

    print(f"Converged in {iteration} iterations")
    print(f"Execution time: {time.time() - start_time:.2f}s")

    return pairs


if __name__ == "__main__":
    spark_context = SparkContext(appName="CCF_Basic")
    result = compute_ccf(spark_context, "data/small_graph.txt")
    result.collect()
    spark_context.stop()
