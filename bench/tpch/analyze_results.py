#!/usr/bin/env python3
"""Analyze TPC-H benchmark CSV outputs and generate paper-ready figures."""

from __future__ import annotations

import csv
import math
import os
from pathlib import Path
from statistics import median, stdev
from typing import Dict, List, Optional, Tuple

try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError as exc:
    raise SystemExit(
        "matplotlib is required. Install with: pip install matplotlib"
    ) from exc

HERE = Path(__file__).resolve().parent
FIG_DIR = HERE / "figures"

# When IMPL is set (e.g. IMPL=materialized or IMPL=delta), load impl-tagged CSVs.
_IMPL = os.environ.get("IMPL", "").strip()


def _csv_path(base: str) -> Path:
    """Return impl-tagged CSV path if IMPL is set, else the default."""
    if _IMPL:
        tagged = HERE / f"{base}_{_IMPL}.csv"
        if tagged.exists():
            return tagged
    return HERE / f"{base}.csv"


RESULTS_CSV     = _csv_path("results")
CREATION_CSV    = _csv_path("creation_results")
WRITE_CSV       = _csv_path("write_results")
STORAGE_CSV     = _csv_path("storage_results")
MULTIBRANCH_CSV = _csv_path("multibranch_results")

MODE_LABELS = {
    "native": "Unversioned (baseline)",
    "copy":   "Full Table Copy",
    "branch": "Branch Extension",
}
MODE_ORDER = ["native", "copy", "branch"]

# Labels used in the implementation-comparison figure.
IMPL_LABELS = {
    "materialized": "Materialized Copy",
    "delta":        "Pure Delta Log",
}


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

    # Left: all three on log scale so MVCC (sub-ms) and Copy (>15s) fit together
    ax = axes[0]
    bars = ax.bar(labels, meds, yerr=errs, capsize=4, width=0.5)
    ax.set_yscale("log")
    ax.set_ylabel("Creation time (ms, log scale)")
    ax.set_title("All methods (log scale)\n† MVCC: ephemeral, read-only")
    for bar, val in zip(bars, meds):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            val * 1.5,
            f"{val:,.0f} ms",
            ha="center", va="bottom", fontsize=8,
        )

    # Right: copy vs branch only (linear scale; MVCC excluded — not a comparable operation)
    ax2 = axes[1]
    non_mvcc = [(m, l) for m, l in zip(modes, labels) if m != "mvcc"]
    nm = [m for m, _ in non_mvcc]
    nl = [l for _, l in non_mvcc]
    bars2 = ax2.bar(nl, [stats[m][0] for m in nm],
                    yerr=[stats[m][1] for m in nm], capsize=4, width=0.4)
    ax2.set_ylabel("Creation time (s)")
    ax2.set_title("Full Copy vs. Branch Extension (linear scale)")
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}s"))
    for bar, m in zip(bars2, nm):
        val = stats[m][0]
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + 200,
            f"{val/1000:.1f}s",
            ha="center", va="bottom", fontsize=9,
        )

    fig.suptitle("Figure 1: Branch Creation Time", y=1.01)
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
    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("Median latency (ms)")
    ax.set_title("Figure 2: Read Latency — TPC-H Q1–Q16 (SF=1)")
    ax.legend(title="Access method")
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

    ax.axhline(1.0, color="black", linestyle="--", linewidth=0.8,
               label="1.0× baseline")
    ax.set_xticks(x)
    ax.set_xticklabels([q.upper() for q in queries], fontsize=8)
    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("Slowdown vs. baseline (1.0× = no overhead)")
    ax.set_title("Figure 3: Read Overhead Ratio vs. Unversioned Baseline")
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
    ax.set_xlabel("Write workload (% of rows modified via UPDATE+DELETE+INSERT)")
    ax.set_ylabel("Total DML time (ms)")
    ax.set_title("Figure 4: Write Throughput — Full Copy vs. Branch Extension")
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

    labels = ["Unversioned\n(baseline)", "Full Table Copy", "Branch Extension\n(work copy + delta log)"]
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
        ["Branch Extension\n(work copy + delta log)"],
        [delta_gb],
        bottom=[work_gb],
        color="limegreen",
        width=0.5,
        label=f"Delta log — changes only ({delta_gb:.3f} GB)",
    )

    for bar, val in zip(bars, bar_vals):
        if val > 0.001:
            ax.text(bar.get_x() + bar.get_width() / 2, val + 0.01,
                    f"{val:.2f} GB", ha="center", va="bottom", fontsize=9)
    total_branch = work_gb + delta_gb
    ax.text(bars[2].get_x() + bars[2].get_width() / 2, total_branch + 0.01,
            f"{total_branch:.3f} GB total", ha="center", va="bottom", fontsize=9)

    ax.set_ylabel("Storage (GB)")
    ax.set_title("Figure 5: Storage Footprint per Branch")
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
    # Support both old column name (value_ms) and new (value)
    groups: Dict[Tuple[str, int], List[float]] = {}
    for r in rows:
        raw = r.get("value") or r.get("value_ms")
        val = parse_float(raw)
        if not math.isnan(val):
            groups.setdefault((r["metric"], int(r["num_branches"])), []).append(val)
    return {k: summarize(v) for k, v in groups.items()}


def plot_multibranch(stats: Dict[Tuple[str, int], Tuple[float, float]]) -> None:
    """Two-panel multi-branch scaling figure.

    Panel 1 (left):  Per-branch creation time (time to create the Nth branch)
                     is roughly constant regardless of how many branches already
                     exist, demonstrating O(1) marginal cost.
    Panel 2 (right): Q6 read latency on branch mb1 while N branches exist
                     is flat, proving branches are fully isolated.
    """
    if not stats:
        return

    branches = sorted({n for _, n in stats})
    if not branches:
        return

    def get(metric: str, n: int) -> Tuple[float, float]:
        return stats.get((metric, n), (math.nan, math.nan))

    last_med    = [get("create_last_ms",  n)[0] for n in branches]
    last_err    = [get("create_last_ms",  n)[1] for n in branches]
    read_med    = [get("read_q6_ms",      n)[0] for n in branches]
    read_err    = [get("read_q6_ms",      n)[1] for n in branches]

    # Backward compat with view-based CSV
    if all(math.isnan(v) for v in read_med):
        read_med = [get("read_q6_on_mb1", n)[0] for n in branches]
        read_err = [get("read_q6_on_mb1", n)[1] for n in branches]

    # Fall back for per-branch cost: derive from cumulative if create_last_ms missing
    has_last = not all(math.isnan(v) for v in last_med)
    if not has_last:
        create_med = [get("create_total_ms", n)[0] for n in branches]
        last_med = [c / n if not math.isnan(c) else math.nan
                    for c, n in zip(create_med, branches)]
        last_err = [0.0] * len(branches)
        has_last = not all(math.isnan(v) for v in last_med)

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    ax0, ax1 = axes

    # ── Panel 1: per-branch creation time (should be constant) ────────────────
    color_last = "#059669"  # green

    if has_last:
        ax0.errorbar(branches, last_med, yerr=last_err,
                     marker="^", color=color_last, linewidth=2, capsize=4,
                     label="Per-branch creation time")
        avg_last = [v for v in last_med if not math.isnan(v)]
        if avg_last:
            overall_avg = sum(avg_last) / len(avg_last)
            ax0.axhline(overall_avg, color=color_last, linestyle=":",
                        linewidth=1.5, alpha=0.6,
                        label=f"Mean ≈ {overall_avg/1000:.1f}s")
        ax0.legend(fontsize=8)

    ax0.set_xlabel("Number of existing branches (N)", fontsize=10)
    ax0.set_ylabel("Creation time for Nth branch (ms)", fontsize=10)
    ax0.set_title("Branch creation cost is constant\n(O(1) marginal cost)",
                  fontsize=11, fontweight="bold")
    ax0.set_xticks(branches)
    ax0.set_ylim(bottom=0)

    # ── Panel 2: read latency on mb1 (should be flat) ─────────────────────────
    color_read = "#7C3AED"  # purple

    ax1.errorbar(branches, read_med, yerr=read_err,
                 marker="o", color=color_read, linewidth=2, capsize=4,
                 label="Branch mb1 (Q6)")

    valid_reads = [v for v in read_med if not math.isnan(v)]
    if valid_reads:
        ax1.set_ylim(bottom=0, top=max(valid_reads) * 1.4)

    ax1.set_xlabel("Number of existing branches (N)", fontsize=10)
    ax1.set_ylabel("Q6 latency on mb1 (ms)", fontsize=10)
    ax1.set_title("Read latency is branch-isolated\n(unaffected by branch count)",
                  fontsize=11, fontweight="bold")
    ax1.set_xticks(branches)
    ax1.legend(fontsize=8)

    # ── Shared formatting ──────────────────────────────────────────────────────
    for ax in [ax0, ax1]:
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Figure 6: Multi-Branch Scaling (TPC-H lineitem, SF=1)",
                 fontsize=12, fontweight="bold", y=1.02)
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
    print(r"Creation complexity & $O(1)^\dagger$ & $O(n)$ & $O(n)$ \\")
    print(r"Rollback complexity & n/a & $O(n)$ & $O(1)$ \\")
    print(r"\hline")
    print(r"\end{tabular}")
    print(r"")
    print(
        r"$^\dagger$ MVCC snapshot: ephemeral and read-only — not a persistent writable branch."
    )


# ── Figure 7: Materialized vs Delta — head-to-head trade-off ──────────────────

def _load_impl(impl: str) -> dict:
    """Load all CSVs for a named implementation run."""
    def p(base: str) -> Path:
        tagged = HERE / f"{base}_{impl}.csv"
        return tagged if tagged.exists() else HERE / f"{base}.csv"
    return {
        "creation": read_csv(p("creation_results")),
        "reads":    read_csv(p("results")),
        "writes":   read_csv(p("write_results")),
        "storage":  read_csv(p("storage_results")),
    }


def plot_impl_comparison() -> None:
    """Four-panel figure contrasting materialized vs pure-delta branch implementations.

    Tells the narrative:
      - Delta wins on creation (O(1)) and writes (no trigger overhead)
      - Materialized wins on reads (plain table scan, no reconstruction)
      - Neither is ideal — motivates a dynamic hybrid
    """
    impls = ["materialized", "delta"]
    data = {impl: _load_impl(impl) for impl in impls}

    # Check we actually have results for both implementations
    has_both = all(data[i]["creation"] or data[i]["reads"] for i in impls)
    if not has_both:
        return

    colors = {"materialized": "#2563EB", "delta": "#DC2626"}
    labels = [IMPL_LABELS.get(i, i) for i in impls]

    fig, axes = plt.subplots(1, 4, figsize=(16, 4.5))
    ax_create, ax_read, ax_write, ax_storage = axes

    # ── Panel 1: Branch creation time ─────────────────────────────────────────
    create_vals, create_errs = [], []
    for impl in impls:
        stats = summarize_creation(data[impl]["creation"])
        med, err = stats.get("branch", (math.nan, math.nan))
        create_vals.append(med)
        create_errs.append(err)

    bars = ax_create.bar(labels, create_vals, yerr=create_errs, capsize=4,
                         color=[colors[i] for i in impls], width=0.5)
    for bar, val in zip(bars, create_vals):
        if not math.isnan(val):
            ax_create.text(bar.get_x() + bar.get_width() / 2, val + max(v for v in create_vals if not math.isnan(v)) * 0.02,
                           f"{val/1000:.1f}s", ha="center", va="bottom", fontsize=9)
    ax_create.set_ylabel("Branch creation time (ms)")
    ax_create.set_title("Creation Time\n(lower is better)", fontweight="bold")

    # ── Panel 2: Read latency (median over all queries, branch mode) ──────────
    read_vals, read_errs = [], []
    for impl in impls:
        stats = summarize_reads(data[impl]["reads"])
        queries = available_queries(stats)
        branch_times = [stats.get((q, "branch"), (math.nan, math.nan))[0]
                        for q in queries]
        clean = [v for v in branch_times if not math.isnan(v)]
        if clean:
            read_vals.append(median(clean))
            read_errs.append(stdev(clean) if len(clean) > 1 else 0.0)
        else:
            read_vals.append(math.nan)
            read_errs.append(0.0)

    bars = ax_read.bar(labels, read_vals, yerr=read_errs, capsize=4,
                       color=[colors[i] for i in impls], width=0.5)
    for bar, val in zip(bars, read_vals):
        if not math.isnan(val):
            ax_read.text(bar.get_x() + bar.get_width() / 2, val + max(v for v in read_vals if not math.isnan(v)) * 0.02,
                         f"{val:.0f}ms", ha="center", va="bottom", fontsize=9)
    ax_read.set_ylabel("Median read latency (ms)")
    ax_read.set_title("Read Latency\n(lower is better)", fontweight="bold")

    # ── Panel 3: Write time at 10% delta ──────────────────────────────────────
    write_vals, write_errs = [], []
    for impl in impls:
        stats = summarize_writes(data[impl]["writes"])
        med, err = stats.get(("branch", 10), (math.nan, math.nan))
        write_vals.append(med)
        write_errs.append(err)

    bars = ax_write.bar(labels, write_vals, yerr=write_errs, capsize=4,
                        color=[colors[i] for i in impls], width=0.5)
    for bar, val in zip(bars, write_vals):
        if not math.isnan(val):
            ax_write.text(bar.get_x() + bar.get_width() / 2, val + max(v for v in write_vals if not math.isnan(v)) * 0.02,
                          f"{val/1000:.1f}s", ha="center", va="bottom", fontsize=9)
    ax_write.set_ylabel("Total write time (ms)")
    ax_write.set_title("Write Latency @ 10% delta\n(lower is better)", fontweight="bold")

    # ── Panel 4: Storage footprint (work copy + delta log) ────────────────────
    GB = 1024 ** 3
    storage_vals = []
    for impl in impls:
        rows = data[impl]["storage"]
        by_label = {r["label"]: int(r["total_bytes"]) for r in rows}
        total = (by_label.get("branch_work_lineitem", 0) +
                 by_label.get("branch_delta_lineitem", 0)) / GB
        storage_vals.append(total if total > 0 else math.nan)

    bars = ax_storage.bar(labels, storage_vals, capsize=4,
                          color=[colors[i] for i in impls], width=0.5)
    for bar, val in zip(bars, storage_vals):
        if not math.isnan(val):
            ax_storage.text(bar.get_x() + bar.get_width() / 2, val + max(v for v in storage_vals if not math.isnan(v)) * 0.02,
                            f"{val:.2f} GB", ha="center", va="bottom", fontsize=9)
    ax_storage.set_ylabel("Branch storage (GB)")
    ax_storage.set_title("Storage Footprint\n(lower is better)", fontweight="bold")

    # ── Shared formatting ──────────────────────────────────────────────────────
    for ax in axes:
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linestyle="--", alpha=0.4)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle(
        "Figure 7: Materialized Copy vs. Pure Delta Log — Trade-off Summary\n"
        "Neither approach dominates; a dynamic hybrid is needed.",
        fontsize=12, fontweight="bold", y=1.03,
    )
    fig.tight_layout()
    fig.savefig(FIG_DIR / "figure7_impl_comparison.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("==> figure7_impl_comparison.png written")


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

    # Generate the cross-impl comparison figure whenever both runs are present.
    plot_impl_comparison()

    print_tables(creation_stats, read_stats, queries)
    print_write_storage(write_stats, storage_rows)
    print(f"\nFigures written to: {FIG_DIR}")


if __name__ == "__main__":
    main()
