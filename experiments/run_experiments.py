from pyspark import SparkContext
from src.ccf_basic import compute_ccf
from src.ccf_optimized import compute_ccf_optimized
import time
import csv


GRAPHS = [
    ("small", "data/small_graph.txt"),
    ("medium", "data/medium_graph.txt"),
    ("large", "data/large_graph.txt")
]


def run_experiments():
    sc = SparkContext(appName="CCF_Experiments")
    results = []

    for name, path in GRAPHS:
        print(f"Running BASIC on {name}")
        start = time.time()
        compute_ccf(sc, path)
        t_basic = time.time() - start

        print(f"Running OPTIMIZED on {name}")
        start = time.time()
        compute_ccf_optimized(sc, path)
        t_opt = time.time() - start

        results.append([name, t_basic, t_opt])

    sc.stop()

    with open("experiments/results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["graph", "time_basic", "time_optimized"])
        writer.writerows(results)


if __name__ == "__main__":
    run_experiments()
