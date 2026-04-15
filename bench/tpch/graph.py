#!/usr/bin/env python3
"""Generate benchmark graphs from results.csv."""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).parent
RESULTS = HERE / "results.csv"
OUT_DIR = HERE / "graphs"


def load_results(path):
    """Load results.csv → {query: {mode: [ms, ...]}}"""
    data = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["ms"] == "NaN":
                continue
            data[row["query"]][row["mode"]].append(float(row["ms"]))
    return data


def median(vals):
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n else 0


def plot_comparison_bar(data, out):
    """Side-by-side bar chart: native vs branch median ms per query."""
    queries = sorted(data.keys(), key=lambda q: int(q.lstrip("q")))
    queries = [q for q in queries if data[q]["native"] and data[q]["branch"]]

    native = [median(data[q]["native"]) for q in queries]
    branch = [median(data[q]["branch"]) for q in queries]

    x = np.arange(len(queries))
    w = 0.35

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(x - w / 2, native, w, label="Native (main)", color="#4c72b0")
    ax.bar(x + w / 2, branch, w, label="Branch (bench)", color="#dd8452")

    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("Median Execution Time (ms)")
    ax.set_title("TPC-H Query Latency: Native vs. Branch")
    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries], rotation=45, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out / "comparison.png", dpi=150)
    plt.close(fig)
    print(f"  -> {out / 'comparison.png'}")


def plot_overhead(data, out):
    """Bar chart of overhead ratio (branch / native) per query."""
    queries = sorted(data.keys(), key=lambda q: int(q.lstrip("q")))
    queries = [q for q in queries if data[q]["native"] and data[q]["branch"]]

    ratios = []
    for q in queries:
        n = median(data[q]["native"])
        b = median(data[q]["branch"])
        ratios.append(b / n if n > 0 else 0)

    fig, ax = plt.subplots(figsize=(14, 5))
    colors = ["#55a868" if r <= 1.05 else "#c44e52" for r in ratios]
    ax.bar(range(len(queries)), ratios, color=colors)
    ax.axhline(y=1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.6)

    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("Overhead Ratio (branch / native)")
    ax.set_title("Branch Layer Overhead per Query")
    ax.set_xticks(range(len(queries)))
    ax.set_xticklabels([q.upper() for q in queries], rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out / "overhead.png", dpi=150)
    plt.close(fig)
    print(f"  -> {out / 'overhead.png'}")


def main():
    results_path = Path(sys.argv[1]) if len(sys.argv) > 1 else RESULTS
    if not results_path.exists():
        print(f"error: {results_path} not found. Run ./run.sh first.", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(exist_ok=True)
    data = load_results(results_path)

    print(f"Loaded {sum(len(v) for m in data.values() for v in m.values())} data points "
          f"across {len(data)} queries")

    plot_comparison_bar(data, OUT_DIR)
    plot_overhead(data, OUT_DIR)
    print("Done.")


if __name__ == "__main__":
    main()
