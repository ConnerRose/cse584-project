# Benchmarking Suite: Branch Extension vs OrpheusDB

This directory contains a comprehensive benchmarking setup to compare your PostgreSQL branch extension with an OrpheusDB-style table versioning system using TPC-H queries.

## Directory Structure

```
bench/
├── tpch/                    # Branch extension benchmark (existing)
│   ├── README.md           # Details on branch extension approach
│   ├── setup.sh            # Generate TPC-H data & setup
│   ├── schema.sql          # Standard TPC-H schema + branch extension init
│   ├── load.sql            # Load TPC-H data
│   ├── init_branch.sql     # Create branch versions
│   ├── run.sh              # Run benchmark
│   └── results.csv         # Output from benchmark
│
├── orpheus_sim/             # OrpheusDB simulator benchmark (new)
│   ├── README.md           # Details on OrpheusDB simulator approach
│   ├── setup.sh            # Setup benchmark
│   ├── schema.sql          # OrpheusDB data model (data + index + version tables)
│   ├── load.sql            # Load TPC-H data
│   ├── init_sim.sql        # Initialize versioning
│   ├── versioning_functions.sql  # SQL functions for version management
│   ├── run.sh              # Run benchmark
│   └── results_sim.csv     # Output from benchmark
│
├── setup_all.sh            # Automated setup for both benchmarks
├── compare.sh              # Compare results from both benchmarks
└── README.md               # This file
```

## Quick Start

### 1. Set Up Everything at Once

```bash
cd bench
./setup_all.sh
```

This will:
- Generate TPC-H data if not present
- Create branch extension benchmark
- Create orpheus_sim benchmark

### 2. Run Individual Benchmarks

```bash
# Branch extension benchmark
cd bench/tpch
./run.sh                    # All 22 queries, 3 iterations

# OrpheusDB simulator benchmark  
cd bench/orpheus_sim
./run.sh                    # All 22 queries, 3 iterations
```

### 3. Compare Results

```bash
cd bench
./compare.sh
```

This produces:
- `comparison_raw.csv`: Detailed per-query timing and overhead calculations
- `comparison_summary.txt`: Statistical summary and interpretation

## Implementation Approaches

### Your Branch Extension (tpch/)

- **Architecture**: PostgreSQL C extension
- **Strategy**: Logical redo logging (delta logs)
- **Branch Creation**: Lazy (empty delta log)
- **Query Integration**: Transparent rewriting
- **Data Storage**: Base table + delta logs per branch

**Benchmark Measures**: 
- Native query on main branch (no versioning overhead)
- Same query on empty child branch
- Delta = overhead of query rewriting and delta log application

### OrpheusDB Simulator (orpheus_sim/)

- **Architecture**: Pure PostgreSQL SQL
- **Strategy**: Index-based record versioning
- **Branch Creation**: Eager (full table materialization)
- **Query Integration**: Index-based filtering
- **Data Storage**: Record table + many-to-many index mapping + version metadata

**Benchmark Measures**:
- Native query directly on record table (no versioning)
- Same query through version 1 index
- Delta = overhead of index join and record lookup

## Understanding the Comparison

Both benchmarks follow the same measurement model but implement different underlying data models:

| Metric | Branch Extension | OrpheusDB Simulator |
|--------|---|---|
| **Query**: q1 on TPC-H | Time(native) = 1234.5ms | Time(native) = 1240.3ms |
| | Time(branch) = 1245.2ms | Time(sim) = 1305.7ms |
| | Overhead = 0.9% | Overhead = 5.3% |

**Interpretation**: If your branch extension has lower overhead across most queries, it's more efficient than the OrpheusDB approach.

## Key Design Decisions

### Why Full Materialization for OrpheusDB Sim?

The original OrpheusDB uses full materialization when creating versions:
- All records from parent are copied to child in the index
- Provides a fair comparison baseline
- Highlights the cost difference between delta logs vs. eager copying

### Why Not Use Real OrpheusDB?

1. **Python 2.7 dependency**: Outdated and incompatible with modern macOS
2. **API complexity**: Would require extensive Python wrapping
3. **Fair comparison**: Pure SQL implementation at same abstraction level as your extension

### Why Focus on LINEITEM?

- Largest TPC-H table (~6M rows at SF=1)
- Represents real-world workloads on big tables
- Demonstrates scalability of both approaches
- Other tables remain non-versioned for comparison fairness

## Detailed Results Analysis

After running both benchmarks, analyze results with:

```bash
# View raw comparison
cat comparison_raw.csv

# View statistical summary
cat comparison_summary.txt

# Query-specific comparison
awk -F, 'NR>1 {
  diff = $7 - $6;
  printf "Q%2s: Branch=%+.1f%% | Sim=%+.1f%% | Diff=%+.1f%%\n", 
         $1, $6, $7, diff
}' comparison_raw.csv | sort
```

## Performance Insights

### What to Expect

1. **Absolute timing**: Both should have low overhead on modern hardware (~1-10%)
2. **Variance**: Some queries benefit from additional indexes or better partitioning
3. **Scaling**: Overhead may increase with table size

### Interpreting Results

- **Branch overhead < 2%**: Excellent efficiency
- **Branch overhead 2-10%**: Good performance, acceptable for most use cases
- **Branch overhead > 10%**: May indicate opportunity for optimization
- **Simulator overhead > Branch overhead**: Your approach is more efficient

## Extending the Benchmark

### Add More Iterations

```bash
ITERS=10 ./run.sh
```

### Run Specific Queries

```bash
QUERIES="1 6 14" ./run.sh
```

### Test Different Scale Factors

```bash
cd bench/tpch
SF=0.1 ./setup.sh      # Small dataset for quick testing
# Re-run benchmarks...
```

## Troubleshooting

### "Data not found" error during load

Make sure TPC-H data has been generated:
```bash
cd bench/tpch
./setup.sh
```

### "Version not found" errors

Verify orpheus_sim initialization:
```bash
psql -c "SELECT * FROM list_versions();" postgres
```

### Timing results show "NaN"

Query likely failed. Check:
1. TPC-H tables exist: `psql -c "SELECT COUNT(*) FROM lineitem;" postgres`
2. Query files exist: `ls bench/tpch/queries/q*.sql`
3. Version tables exist: `psql -c "SELECT * FROM lineitem_version;" postgres`

## References

- **Branch Extension**: Your PostgreSQL extension implementation
- **OrpheusDB Paper**: [Orpheus: Harvesting Version Control Histories](https://par.nsf.gov/servlets/purl/10110945)
- **TPC-H**: [TPC-H Benchmark Specification](http://www.tpc.org/tpch/)
- **PostgreSQL**: [PostgreSQL 17 Documentation](https://www.postgresql.org/docs/17/)

## Next Steps

1. Run both benchmarks: `./setup_all.sh && cd tpch && ./run.sh && cd ../orpheus_sim && ./run.sh`
2. Compare results: `cd .. && ./compare.sh`
3. Analyze performance differences and optimize as needed
4. Document findings in your paper/thesis

## Support

For questions about the benchmarks or setup issues, refer to:
- [tpch/README.md](tpch/README.md) - Details on branch extension approach
- [orpheus_sim/README.md](orpheus_sim/README.md) - Details on OrpheusDB simulator approach
