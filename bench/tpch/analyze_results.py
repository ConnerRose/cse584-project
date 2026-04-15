#!/usr/bin/env python3
"""Analyze TPC-H benchmark CSV outputs and generate paper-ready figures."""

from __future__ import annotations

import csv
import math
from pathlib import Path
from statistics import median, stdev
from typing import Dict, List, Tuple

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError as exc:
    raise SystemExit(
        "matplotlib is required. Install with: pip install matplotlib"
    ) from exc

HERE = Path(__file__).resolve().parent
RESULTS_CSV = HERE / "results.csv"
CREATION_CSV = HERE / "creation_results.csv"
WRITE_CSV = HERE / "write_results.csv"
STORAGE_CSV = HERE / "storage_results.csv"
MULTIBRANCH_CSV = HERE / "multibranch_results.csv"
FIG_DIR = HERE / "figures"

MODE_LABELS = {"native": "Native (public)", "copy": "Full Copy", "branch": "Branch Ext."}
MODE_ORDER = ["native", "copy", "branch"]


def read_csv(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def parse_float(v: str) -> float:
    if v is None:
        return math.nan
    v = v.strip()
    if not v or v.lower() == "nan":
        return math.nan
    return float(v)


def summarize(vals: List[float]) -> Tuple[float, float]:
    clean = [v for v in vals if not math.isnan(v)]
    if not clean:
        return math.nan, math.nan
    if len(clean) == 1:
        return clean[0], 0.0
    return median(clean), stdev(clean)


def summarize_creation(rows: List[dict]) -> Dict[str, Tuple[float, float]]:
    groups: Dict[str, List[float]] = {}
    for r in rows:
        ms = parse_float(r["ms"])
        if not math.isnan(ms):
            groups.setdefault(r["mode"], []).append(ms)
    return {m: summarize(v) for m, v in groups.items()}


def summarize_reads(
    rows: List[dict],
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    groups: Dict[Tuple[str, str], List[float]] = {}
    for r in rows:
        ms = parse_float(r["ms"])
        if not math.isnan(ms):
            groups.setdefault((r["query"], r["mode"]), []).append(ms)
    return {k: summarize(v) for k, v in groups.items()}


def available_queries(stats: Dict[Tuple[str, str], Tuple[float, float]]) -> List[str]:
    qs = sorted({q for q, _ in stats}, key=lambda x: int(x[1:]))
    return qs


# ── Figure 1: Creation time ────────────────────────────────────────────────────

def plot_creation(stats: Dict[str, Tuple[float, float]]) -> None:
    modes = [m for m in ["mvcc", "copy", "branch"] if m in stats]
    labels = [MODE_LABELS.get(m, m) for m in modes]
    meds = [stats[m][0] for m in modes]
    errs = [stats[m][1] for m in modes]

    fig, axes = plt.subplots(1, 2, figsize=(10, 3.5))

    # Left: all three on same axis (log scale to show MVCC vs the rest)
    ax = axes[0]
    bars = ax.bar(labels, meds, yerr=errs, capsize=4, width=0.5)
    ax.set_yscale("log")
    ax.set_ylabel("Time (ms, log scale)")
    ax.set_title("Branch/Snapshot Creation Time")
    for bar, val in zip(bars, meds):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val * 1.5,
            f"{val:,.0f}ms",
            ha="center", va="bottom", fontsize=8,
        )

    # Right: copy vs branch only (linear scale, MVCC excluded as it is not equivalent)
    ax2 = axes[1]
    non_mvcc = [(m, l) for m, l in zip(modes, labels) if m != "mvcc"]
    nm = [m for m, _ in non_mvcc]
    nl = [l for _, l in non_mvcc]
    bars2 = ax2.bar(nl, [stats[m][0] for m in nm],
                    yerr=[stats[m][1] for m in nm], capsize=4, width=0.4)
    ax2.set_ylabel("Time (ms)")
    ax2.set_title("Copy vs Branch Creation (linear; branch = view + delta, O(1))")
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}s"))
    for bar, m in zip(bars2, nm):
        val = stats[m][0]
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 200,
            f"{val/1000:.1f}s",
            ha="center", va="bottom", fontsize=9,
        )

    fig.suptitle("Creation Time: MVCC Snapshot vs Full Copy vs Branch Extension", y=1.01)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure1_creation_time.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


# ── Figure 2: Read latency, all queries ───────────────────────────────────────

def plot_reads_all(
    stats: Dict[Tuple[str, str], Tuple[float, float]],
    queries: List[str],
) -> None:
    n = len(queries)
    width = 0.25
    x = list(range(n))

    fig, ax = plt.subplots(figsize=(max(10, n * 0.9), 4.5))
    for idx, mode in enumerate(MODE_ORDER):
        meds = [stats.get((q, mode), (math.nan, math.nan))[0] for q in queries]
        errs = [stats.get((q, mode), (math.nan, math.nan))[1] for q in queries]
        xpos = [v + (idx - 1) * width for v in x]
        ax.bar(xpos, meds, width=width, yerr=errs, capsize=2,
               label=MODE_LABELS.get(mode, mode))

    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries], fontsize=8)
    ax.set_ylabel("Median execution time (ms)")
    ax.set_title("Read Latency: Native vs Full Copy vs Branch Extension (TPC-H, SF=1)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure2_read_latency_all.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


# ── Figure 3: Branch overhead ratio vs native ────────────────────────────────

def plot_overhead_ratio(
    stats: Dict[Tuple[str, str], Tuple[float, float]],
    queries: List[str],
) -> None:
    fig, ax = plt.subplots(figsize=(max(10, len(queries) * 0.9), 4))
    x = list(range(len(queries)))
    width = 0.35

    for idx, mode in enumerate(["copy", "branch"]):
        ratios = []
        for q in queries:
            nat, _ = stats.get((q, "native"), (math.nan, math.nan))
            mod, _ = stats.get((q, mode), (math.nan, math.nan))
            if math.isnan(nat) or math.isnan(mod) or nat == 0:
                ratios.append(math.nan)
            else:
                ratios.append(mod / nat)
        xpos = [v + (idx - 0.5) * width for v in x]
        ax.bar(xpos, ratios, width=width, label=MODE_LABELS.get(mode, mode))

    ax.axhline(1.0, color="black", linestyle="--", linewidth=0.8, label="Native baseline (1.0x)")
    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries], fontsize=8)
    ax.set_ylabel("Overhead ratio vs native")
    ax.set_title("Read Overhead Ratio: Full Copy and Branch Extension vs Native")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure3_overhead_ratio.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


# ── Figure 4: Write throughput (total time by delta%) ────────────────────────

def summarize_writes(
    rows: List[dict],
) -> Dict[Tuple[str, int], Tuple[float, float]]:
    groups: Dict[Tuple[str, int], List[float]] = {}
    for r in rows:
        ms = parse_float(r["ms"])
        if not math.isnan(ms):
            groups.setdefault((r["mode"], int(r["delta_pct"])), []).append(ms)
    return {k: summarize(v) for k, v in groups.items()}


def plot_writes(stats: Dict[Tuple[str, int], Tuple[float, float]]) -> None:
    if not stats:
        return
    deltas = sorted({d for _, d in stats})
    modes = ["copy", "branch"]
    width = 0.35
    x = list(range(len(deltas)))

    fig, ax = plt.subplots(figsize=(6, 4))
    for idx, mode in enumerate(modes):
        meds = [stats.get((mode, d), (math.nan, math.nan))[0] for d in deltas]
        errs = [stats.get((mode, d), (math.nan, math.nan))[1] for d in deltas]
        xpos = [v + (idx - 0.5) * width for v in x]
        bars = ax.bar(xpos, meds, width=width, yerr=errs, capsize=4,
                      label=MODE_LABELS.get(mode, mode))
        for bar, val in zip(bars, meds):
            if not math.isnan(val):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    val + max(meds) * 0.01,
                    f"{val/1000:.1f}s",
                    ha="center", va="bottom", fontsize=8,
                )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{d}% delta" for d in deltas])
    ax.set_ylabel("Total write time (ms)")
    ax.set_title("Write Throughput: Full Copy vs Branch Extension\n(UPDATE + DELETE + INSERT on lineitem)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure4_write_throughput.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


# ── Figure 5: Storage footprint ───────────────────────────────────────────────

def plot_storage(rows: List[dict]) -> None:
    if not rows:
        return

    by_label = {r["label"]: int(r["total_bytes"]) for r in rows}
    base = by_label.get("public_lineitem", 0)
    if base == 0:
        return

    GB = 1024 ** 3

    labels = ["Native\n(public)", "Full Copy", "Branch\n(view + delta log)"]
    sizes_gb = [
        base / GB,
        by_label.get("copy_lineitem", 0) / GB,
        (by_label.get("branch_work_lineitem", 0) + by_label.get("branch_delta_lineitem", 0)) / GB,
    ]
    colors = ["steelblue", "darkorange", "green"]

    fig, ax = plt.subplots(figsize=(7, 4))

    # View has no heap; show delta stacked on zero-height "work" when 0
    work_gb = by_label.get("branch_work_lineitem", 0) / GB
    delta_gb = by_label.get("branch_delta_lineitem", 0) / GB

    bar_vals = [base / GB, by_label.get("copy_lineitem", 0) / GB, work_gb]
    bars = ax.bar(labels, bar_vals, color=colors, width=0.5)
    ax.bar(
        ["Branch\n(view + delta log)"],
        [delta_gb],
        bottom=[work_gb],
        color="limegreen",
        width=0.5,
        label=f"Delta log ({delta_gb:.3f} GB)",
    )

    for bar, val in zip(bars, bar_vals):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 0.01,
                f"{val:.2f} GB", ha="center", va="bottom", fontsize=9)
    total_branch = work_gb + delta_gb
    ax.text(bars[2].get_x() + bars[2].get_width() / 2, total_branch + 0.01,
            f"{total_branch:.2f} GB total", ha="center", va="bottom", fontsize=9)

    ax.set_ylabel("Storage (GB)")
    ax.set_title("Storage Footprint After Write Workload (view has no heap; delta only)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure5_storage.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def print_write_storage(
    write_stats: Dict[Tuple[str, int], Tuple[float, float]],
    storage_rows: List[dict],
) -> None:
    if write_stats:
        print("\nWrite summary (total ms for UPDATE+DELETE+INSERT):")
        print(f"  {'mode':8s}  {'delta_pct':>10s}  {'median_ms':>10s}  {'stddev_ms':>10s}")
        for (mode, d), (med, std) in sorted(write_stats.items()):
            print(f"  {mode:8s}  {d:>10d}  {med:>10.1f}  {std:>10.3f}")

    if storage_rows:
        print("\nStorage footprint (GB):")
        for r in storage_rows:
            gb = int(r["total_bytes"]) / (1024 ** 3)
            print(f"  {r['label']:30s}  {gb:.3f} GB")


# ── Console output ─────────────────────────────────────────────────────────────

def summarize_multibranch(rows: List[dict]) -> Dict[Tuple[str, int], Tuple[float, float]]:
    groups: Dict[Tuple[str, int], List[float]] = {}
    for r in rows:
        ms = parse_float(r["value_ms"])
        if not math.isnan(ms):
            groups.setdefault((r["metric"], int(r["num_branches"])), []).append(ms)
    return {k: summarize(v) for k, v in groups.items()}


def plot_multibranch(stats: Dict[Tuple[str, int], Tuple[float, float]]) -> None:
    if not stats:
        return
    branches = sorted({n for _, n in stats})
    fig, axes = plt.subplots(2, 1, figsize=(7, 6), sharex=True)
    ax0, ax1 = axes
    meds0 = [stats.get(("create_total", n), (math.nan, math.nan))[0] for n in branches]
    meds1 = [stats.get(("read_q6_on_mb1", n), (math.nan, math.nan))[0] for n in branches]
    ax0.plot(branches, meds0, marker="o", color="C0")
    ax0.set_ylabel("Median ms")
    ax0.set_title("Cumulative create time (N independent branches from main)")
    ax1.plot(branches, meds1, marker="o", color="C1")
    ax1.set_ylabel("Median ms")
    ax1.set_xlabel("N")
    ax1.set_title("TPC-H Q6 on branch mb1 while N branches exist")
    fig.suptitle("Multi-branch scaling (view-based branches)", y=1.02)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure6_multibranch.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def print_tables(
    creation_stats: Dict[str, Tuple[float, float]],
    read_stats: Dict[Tuple[str, str], Tuple[float, float]],
    queries: List[str],
) -> None:
    print("\nCreation summary (median, stddev ms):")
    for mode in ("mvcc", "copy", "branch"):
        med, std = creation_stats.get(mode, (math.nan, math.nan))
        print(f"  {mode:7s} median={med:10.3f} stddev={std:10.3f}")

    print(f"\nRead summary (all {len(queries)} queries, median ms):")
    header = f"  {'query':6s}  {'native':>10s}  {'copy':>10s}  {'branch':>10s}  {'copy/nat':>9s}  {'branch/nat':>10s}"
    print(header)
    for q in queries:
        nat = read_stats.get((q, "native"), (math.nan, math.nan))[0]
        cop = read_stats.get((q, "copy"), (math.nan, math.nan))[0]
        bra = read_stats.get((q, "branch"), (math.nan, math.nan))[0]
        cr = f"{cop/nat:.2f}x" if not (math.isnan(cop) or math.isnan(nat) or nat == 0) else "n/a"
        br = f"{bra/nat:.2f}x" if not (math.isnan(bra) or math.isnan(nat) or nat == 0) else "n/a"
        print(f"  {q:6s}  {nat:10.1f}  {cop:10.1f}  {bra:10.1f}  {cr:>9s}  {br:>10s}")

    print("\nCapability matrix (LaTeX):")
    print(r"\begin{tabular}{lccc}")
    print(r"\hline")
    print(r"Capability & MVCC Snapshot & Full Copy & Branch Extension \\")
    print(r"\hline")
    print(r"Isolated experimental writes & $\times$ & \checkmark & \checkmark \\")
    print(r"Persistent across sessions & $\times$ & \checkmark & \checkmark \\")
    print(r"Multiple concurrent branches & $\times$ & \checkmark & \checkmark \\")
    print(r"Lightweight rollback post-commit & $\times$ & $\times$ & \checkmark \\")
    print(r"Creation complexity & $O(1)^*$ & $O(n)$ & $O(1)$ \\")
    print(r"\hline")
    print(r"\end{tabular}")
    print(r"")
    print(
        r"$^*$ MVCC snapshot: not equivalent (read-only, ephemeral). "
        r"Full copy duplicates the table. Branch uses a view + delta log."
    )


def main() -> None:
    FIG_DIR.mkdir(exist_ok=True)
    creation_rows = read_csv(CREATION_CSV)
    read_rows = read_csv(RESULTS_CSV)
    write_rows = read_csv(WRITE_CSV)
    storage_rows = read_csv(STORAGE_CSV)
    multibranch_rows = read_csv(MULTIBRANCH_CSV)

    if not creation_rows and not read_rows:
        raise SystemExit(
            f"No input CSV files found. Expected {CREATION_CSV} and/or {RESULTS_CSV}."
        )

    creation_stats = summarize_creation(creation_rows)
    read_stats = summarize_reads(read_rows)
    queries = available_queries(read_stats)
    write_stats = summarize_writes(write_rows)
    multibranch_stats = summarize_multibranch(multibranch_rows)

    if creation_stats:
        plot_creation(creation_stats)
    if read_stats and queries:
        plot_reads_all(read_stats, queries)
        plot_overhead_ratio(read_stats, queries)
    if write_stats:
        plot_writes(write_stats)
    if storage_rows:
        plot_storage(storage_rows)
    if multibranch_stats:
        plot_multibranch(multibranch_stats)

    print_tables(creation_stats, read_stats, queries)
    print_write_storage(write_stats, storage_rows)
    print(f"\nFigures written to: {FIG_DIR}")


if __name__ == "__main__":
    main()
