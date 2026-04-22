#!/usr/bin/env python3
"""Generate graphs for the delta scaling benchmark."""

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
    """Load results.csv → {delta_size: {mode: [ms, ...]}}"""
    data = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for row in csv.DictReader(f):
            if row["ms"] == "NaN":
                continue
            data[int(row["delta_size"])][row["mode"]].append(float(row["ms"]))
    return data


def median(vals):
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n else 0


def plot_latency_vs_delta(data, out):
    """Line chart: query latency vs delta table size."""
    sizes = sorted(data.keys())
    sizes = [s for s in sizes if data[s]["native"] and data[s]["branch"]]

    native = [median(data[s]["native"]) for s in sizes]
    branch = [median(data[s]["branch"]) for s in sizes]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(sizes, native, "o-", label="Native (main)", color="#4c72b0", linewidth=2)
    ax.plot(sizes, branch, "s-", label="Branch (view)", color="#dd8452", linewidth=2)

    ax.set_xlabel("Delta Table Size (rows)")
    ax.set_ylabel("Median Query Latency (ms)")
    ax.set_title("Query Latency vs. Delta Table Size")
    ax.set_xscale("symlog", linthresh=100)
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out / "latency_vs_delta.png", dpi=150)
    plt.close(fig)
    print(f"  -> {out / 'latency_vs_delta.png'}")


def plot_overhead_vs_delta(data, out):
    """Line chart: overhead ratio vs delta table size."""
    sizes = sorted(data.keys())
    sizes = [s for s in sizes if data[s]["native"] and data[s]["branch"]]

    ratios = []
    for s in sizes:
        n = median(data[s]["native"])
        b = median(data[s]["branch"])
        ratios.append(b / n if n > 0 else 0)

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = ["#55a868" if r <= 1.1 else "#c44e52" for r in ratios]
    ax.bar(range(len(sizes)), ratios, color=colors)
    ax.axhline(y=1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.6)

    ax.set_xlabel("Delta Table Size (rows)")
    ax.set_ylabel("Overhead Ratio (branch / native)")
    ax.set_title("Branch Layer Overhead vs. Delta Size")
    ax.set_xticks(range(len(sizes)))
    ax.set_xticklabels([f"{s:,}" for s in sizes], rotation=45, ha="right")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out / "overhead_vs_delta.png", dpi=150)
    plt.close(fig)
    print(f"  -> {out / 'overhead_vs_delta.png'}")


def main():
    results_path = Path(sys.argv[1]) if len(sys.argv) > 1 else RESULTS
    if not results_path.exists():
        print(f"error: {results_path} not found. Run ./run.sh first.", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(exist_ok=True)
    data = load_results(results_path)

    print(
        f"Loaded {sum(len(v) for m in data.values() for v in m.values())} data points "
        f"across {len(data)} delta sizes"
    )

    plot_latency_vs_delta(data, OUT_DIR)
    plot_overhead_vs_delta(data, OUT_DIR)
    print("Done.")


if __name__ == "__main__":
    main()
