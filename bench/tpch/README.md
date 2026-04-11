# TPC-H Benchmark for the Branch Extension

Measures the overhead of the branch layer by running the 22 TPC-H queries
natively and through `branch.run()` on an empty child branch.

## Prerequisites

- A working PostgreSQL 17 install with the `branch` extension built and
  installed (run `make && make install` from the repo root first).
- `git`, `make`, `cc` on PATH.
- ~5 GB free disk (1 GB raw `.tbl` files + ~3 GB in Postgres at SF=1).

## Setup

```bash
cd bench/tpch

# 1. Clone tpch-kit, build dbgen/qgen, generate SF=1 data and queries.
#    Override with: SF=0.1 ./setup.sh for a smaller quick-test run.
./setup.sh

# 2. Create the schema, load the data, and install the branch extension.
psql postgres -f schema.sql
psql postgres -f load.sql
psql postgres -f init_branch.sql
```

## Running the benchmark

```bash
./run.sh                    # all 22 queries, 3 iterations each
ITERS=5 ./run.sh            # more iterations
QUERIES="1 6 14" ./run.sh   # only specific queries
```

Results are written to `results.csv` (one row per query / mode / iteration)
and a median-per-query summary is printed.

## Benchmark design

The `branches` metadata table ties each branch to a single `base_table`, so
we can't cover all 8 TPC-H tables under one branch. We register `main` on
**LINEITEM** (the biggest table at SF=1, ~6M rows) and create a child
branch `bench`. Creating the child eagerly materializes a full copy of
LINEITEM (with its PK and indexes) into schema `branch_work_bench`.

For each query we compare:

- **native** — `switch_branch('main')` then run the query. `search_path` is
  `public`, so LINEITEM resolves to `public.lineitem`.
- **branch** — `switch_branch('bench')` then run the same query.
  `search_path` becomes `branch_work_bench, public`, so LINEITEM resolves
  to the working copy while the other 7 tables still come from `public`.

Both runs execute the query inside
`DO $$ BEGIN PERFORM 1 FROM (<query>) t; END $$;` so that rows are consumed
server-side and result-transfer time is removed from the measurement.

The delta is the overhead of reading through a second, schema-resolved copy
of LINEITEM with an empty delta log. Since the working copy has identical
structure and indexes to `public.lineitem`, we expect the overhead to be
near zero — any measurable gap comes from cold cache effects, stats
differences, or planner choices on the cloned table.

## Caveats

- Creating `bench` is a one-time ~several-second cost (clones LINEITEM +
  indexes). This cost is not included in the per-query measurements.
- The benchmark currently only exercises read overhead. Write overhead (the
  cost of the AFTER ROW trigger appending deltas) is not measured here.
- TPC-H Q15 creates and drops a view — it works, but `search_path` affects
  where the view is created. Inspect `queries/q15.sql` if it misbehaves.
