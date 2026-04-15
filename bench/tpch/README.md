# TPC-H Benchmark Suite (MVCC vs Copy vs Branch)

This directory contains the benchmark used to show how far PostgreSQL MVCC
can go for versioning, where it fails, and where the branch extension helps.

The suite keeps a single benchmark table (`lineitem`) and compares three
approaches:

- `native` / `mvcc`: PostgreSQL base table via MVCC snapshot semantics
- `copy`: full table copy baseline (`copy_baseline.lineitem`)
- `branch`: branch extension working copy (`branch_work_bench.lineitem`)

## Prerequisites

- PostgreSQL 17
- extension built/installed from repo root: `make && make install`
- tools on PATH: `git`, `make`, `cc`, `python3`
- optional for charts: `pip install matplotlib`
- disk: roughly 5+ GB free at SF=1

## Setup

```bash
cd bench/tpch

# 1) Generate TPC-H data and queries (SF defaults to 1).
./setup.sh

# 2) Create schema and load data.
psql postgres -f schema.sql
psql postgres -f load.sql

# 3) Install extension + benchmark objects (branch + copy baselines).
psql postgres -f init_branch.sql
```

## What Each Script Does

- `run.sh`:
  - creation timing microbenchmark (`mvcc`, `copy`, `branch`)
  - read benchmark over TPC-H query files (`native`, `copy`, `branch`)
  - outputs:
    - `creation_results.csv`
    - `results.csv`
- `run_all.sh`: one-command orchestrator for read/write/storage (+ optional wall tests)
- `wall_tests.sql`: five MVCC limitation demonstrations (qualitative + helper stats)
- `bench_write.sql`: write workload on sampled `lineitem` keys (1%/10% deltas)
- `bench_storage.sql`: relation-size and dead-tuple metrics
- `analyze_results.py`: plots charts and prints a LaTeX capability matrix

## Running Benchmarks

### 1) Creation + Read Benchmark

```bash
# all queries, 3 iterations
./run.sh

# custom run
ITERS=5 QUERIES="1 6 14" ./run.sh
```

Note: `run.sh` checks `SHOW autovacuum;` and exits unless it is `off`
(or `ALLOW_AUTOVACUUM_ON=1` is set).

### Fast Path: Run Everything

```bash
# includes run.sh + write benchmark + storage snapshot + figure generation
./run_all.sh

# include wall tests too
RUN_WALL_TESTS=1 ./run_all.sh
```

### 2) MVCC Wall Demonstrations

```bash
psql postgres -f wall_tests.sql
```

### 3) Write Benchmark

```bash
psql postgres -v mode=branch -v delta_pct=1  -f bench_write.sql
psql postgres -v mode=copy   -v delta_pct=1  -f bench_write.sql
psql postgres -v mode=mvcc   -v delta_pct=1  -f bench_write.sql

psql postgres -v mode=branch -v delta_pct=10 -f bench_write.sql
psql postgres -v mode=copy   -v delta_pct=10 -f bench_write.sql
psql postgres -v mode=mvcc   -v delta_pct=10 -f bench_write.sql
```

### 4) Storage Snapshot

```bash
psql postgres -f bench_storage.sql
```

### 5) Generate Figures

```bash
python3 analyze_results.py
```

Figures are written to `figures/`.

## Methodology Notes for Paper

- Dataset: TPC-H `lineitem` (SF configurable; SF=1 ~6M rows)
- Query timing uses server-side `\timing` and repeated runs
- reported stats: median and standard deviation
- `lineitem` has a composite PK (`l_orderkey`, `l_linenumber`)
- branch benchmark does not rely on `apply_branch`; it focuses on branch
  creation/switching/read/write/rollback behavior

## Caveats

- Q15 creates/drops a view and may be sensitive to `search_path`
- Branch creation is eager snapshotting (`O(n)`), so creation-time comparisons
  should be interpreted with semantic differences in mind (`mvcc` creation is
  fast but does not provide persistent writable branching)
