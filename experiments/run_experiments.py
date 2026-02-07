"""Script d'évaluation expérimentale des versions basique et optimisée du CCF."""

from __future__ import annotations

import csv
import time
from pyspark import SparkContext

from src.ccf_basic import compute_ccf
from src.ccf_optimized import compute_ccf_optimized


GRAPHS = [
    ("small", "data/small_graph.txt"),
    ("medium", "data/medium_graph.txt"),
    ("large", "data/large_graph.txt"),
]


def run_experiments() -> None:
    """Exécute les expériences de scalabilité et exporte ``results.csv``."""
    spark_context = SparkContext(appName="CCF_Experiments")
    results: list[list[float | str]] = []

    for graph_name, graph_path in GRAPHS:
        print(f"Running BASIC on {graph_name}")
        start_time = time.time()
        compute_ccf(spark_context, graph_path)
        basic_time = time.time() - start_time

        print(f"Running OPTIMIZED on {graph_name}")
        start_time = time.time()
        compute_ccf_optimized(spark_context, graph_path)
        optimized_time = time.time() - start_time

        results.append([graph_name, basic_time, optimized_time])

    spark_context.stop()

    with open("experiments/results.csv", "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["graph", "time_basic", "time_optimized"])
        writer.writerows(results)


if __name__ == "__main__":
    run_experiments()
