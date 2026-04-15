# OrpheusDB Simulator Benchmark for TPC-H

This benchmark implements an **OrpheusDB-style table versioning system** in pure PostgreSQL, allowing us to compare it directly with your branch extension.

## Overview

**OrpheusDB Data Model**: Each versioned dataset (CVD) uses three tables:
- **`lineitem_data`**: stores actual records with unique row IDs
- **`lineitem_index`**: many-to-many mapping of `(record_id, version_id)`
- **`lineitem_version`**: version metadata (parent, author, timestamp, etc.)

**Versioning Strategy**: Full table materialization
- When creating a child version, all records from the parent are copied into the index
- Queries access the current version via the `lineitem` view, which joins data through the index

## Comparison with Your Branch Extension

| Aspect | Your Extension | OrpheusDB Simulator |
|--------|---|---|
| Implementation | PostgreSQL extension (C) | Pure SQL functions |
| Data Storage | Delta logs + base table | Full record copies in index |
| Version Creation | Lazy (empty delta log) | Eager (full table materialization) |
| Branching | Git-like with logical redo | Reference-based versioning |
| Query Integration | Transparent query rewriting | Index-based filtering |

## Prerequisites

- PostgreSQL 17
- TPC-H data generated (run `cd bench/tpch && ./setup.sh` first)
- ~2-3 GB additional disk space for versioned lineitem tables

## Setup

```bash
cd bench/orpheus_sim

# 1. Create schema, load data, initialize versions
./setup.sh

# 2. Verify installation
psql -c "SELECT * FROM list_versions();" postgres

# 3. Check record counts
psql -c "SELECT vid, num_records FROM lineitem_version;" postgres
```

## Running the Benchmark

```bash
./run.sh                    # all 22 queries, 3 iterations each
ITERS=5 ./run.sh            # more iterations
QUERIES="1 6 14" ./run.sh   # only specific queries
DB=mydb ./run.sh            # use different database
```

Results are written to `results_sim.csv` with columns: `query,mode,iter,ms`

## Benchmark Design

### Measurement Model

For each query:
- **native**: runs against version 0 (base version, ~6M lineitem rows)
- **sim**: runs against version 1 (child version, fully materialized copy of all rows)

The delta = `sim_time - native_time` represents the overhead of index-based record lookup versus direct table access.

### Overhead Sources

1. **Index join**: `lineitem` view must join `lineitem_data` through `lineitem_index`
2. **Table size**: `lineitem_data` contains ALL records; index filtering is the overhead
3. **Query planning**: planner must choose between different join strategies

### Expected Results

- **Overhead**: 10-50% for most queries (depending on selectivity and cache effects)
- **Variation**: some queries benefit from the additional index (better partitioning for planner)
- **Scaling**: overhead grows with table size due to index lookup cost

## Interpretation

Compare these results with your branch extension benchmark (`bench/tpch/results.csv`):

```bash
# Side-by-side comparison
paste bench/tpch/results.csv bench/orpheus_sim/results_sim.csv | \
  awk -F, 'NR>1 {
    if ($1==$5) {
      branch = $4;
      sim = $9;
      if (branch != "NaN" && sim != "NaN") {
        overhead = (sim - branch) / branch * 100;
        printf "%s: branch=%.1fms sim=%.1fms (overhead=%+.1f%%)\n", 
               $1, branch, sim, overhead;
      }
    }
  }' | sort -t: -k1 -V
```

### Interpretation Guide

- **Branch overhead < Sim overhead**: Your approach is more efficient
- **Branch overhead > Sim overhead**: OrpheusDB approach is more efficient
- **Similar overhead**: Both approaches have comparable performance

## Versioning API

The simulator provides SQL functions for version management:

```sql
-- Switch to a version
SELECT switch_version(1);

-- Create a new child version
SELECT create_child_version(0, 'new experiment', true);

-- List all versions
SELECT * FROM list_versions();

-- Get version statistics
SELECT * FROM version_stats(1);

-- Materialize a version (for export)
SELECT * FROM materialize_version(1);
```

## Limitations

1. **No modifications**: The simulator only supports full materialization for version creation. Write operations (insert/update/delete) are not implemented.
2. **Single table**: Only LINEITEM is versioned; other TPC-H tables are not.
3. **No optimization**: Unlike OrpheusDB, there's no delta compression or partition optimization.
4. **Session-based**: Version switching is per-session (via `session_state` table); connections see independent versions.

## References

- OrpheusDB: https://github.com/orpheus-db/implementation
- OrpheusDB Paper: https://par.nsf.gov/servlets/purl/10110945
- TPC-H Specification: http://www.tpc.org/tpch/

## Next Steps

1. Run both benchmarks with the same hardware/settings
2. Compare overhead metrics
3. Analyze query execution plans (`EXPLAIN ANALYZE`) for insights
4. Consider which approach (delta logs vs. materialization) better suits your use case
