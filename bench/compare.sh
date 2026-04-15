#!/usr/bin/env bash
# Compare Branch Extension vs OrpheusDB Simulator
#
# Runs both benchmarks and produces side-by-side comparison of overhead metrics.
# Outputs:
#   - comparison_raw.csv: raw timing data from both systems
#   - comparison_summary.txt: statistical summary and analysis

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$HERE/tpch"
SIM_DIR="$HERE/orpheus_sim"
DB="${DB:-postgres}"

COMPARE_RAW="$HERE/comparison_raw.csv"
COMPARE_SUMMARY="$HERE/comparison_summary.txt"

# Check if both benchmarks have been run
if [ ! -f "$TPCH_DIR/results.csv" ]; then
    echo "error: $TPCH_DIR/results.csv not found" >&2
    echo "Run 'cd $TPCH_DIR && ./run.sh' first" >&2
    exit 1
fi

if [ ! -f "$SIM_DIR/results_sim.csv" ]; then
    echo "error: $SIM_DIR/results_sim.csv not found" >&2
    echo "Run 'cd $SIM_DIR && ./run.sh' first" >&2
    exit 1
fi

echo "Comparing Branch Extension vs OrpheusDB Simulator..."
echo ""

# Create raw comparison file with header
echo "query,branch_native_ms,branch_sim_ms,sim_native_ms,sim_sim_ms" > "$COMPARE_RAW"

# Parse and merge results
python3 - "$TPCH_DIR/results.csv" "$SIM_DIR/results_sim.csv" "$COMPARE_RAW" <<'EOF'
import sys
import csv
from collections import defaultdict

branch_file = sys.argv[1]
sim_file = sys.argv[2]
output_file = sys.argv[3]

# Parse branch extension results
branch_data = defaultdict(lambda: {'native': [], 'branch': []})
with open(branch_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        query = int(row['query'])
        mode = row['mode']
        ms = row['ms']
        if ms != 'NaN':
            branch_data[query][mode].append(float(ms))

# Parse orpheus_sim results  
sim_data = defaultdict(lambda: {'native': [], 'sim': []})
with open(sim_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        query = int(row['query'])
        mode = row['mode']
        ms = row['ms']
        if ms != 'NaN':
            sim_data[query][mode].append(float(ms))

# Calculate medians and write comparison
with open(output_file, 'w') as f:
    f.write("query,branch_native_ms,branch_sim_ms,sim_native_ms,sim_sim_ms,branch_overhead_pct,sim_overhead_pct\n")
    
    for query in sorted(set(branch_data.keys()) & set(sim_data.keys())):
        branch_native = sorted(branch_data[query]['native'])[len(branch_data[query]['native'])//2] if branch_data[query]['native'] else None
        branch_child = sorted(branch_data[query]['branch'])[len(branch_data[query]['branch'])//2] if branch_data[query]['branch'] else None
        sim_native = sorted(sim_data[query]['native'])[len(sim_data[query]['native'])//2] if sim_data[query]['native'] else None
        sim_child = sorted(sim_data[query]['sim'])[len(sim_data[query]['sim'])//2] if sim_data[query]['sim'] else None
        
        branch_overhead = ((branch_child - branch_native) / branch_native * 100) if (branch_native and branch_child) else None
        sim_overhead = ((sim_child - sim_native) / sim_native * 100) if (sim_native and sim_child) else None
        
        row = [str(query)]
        row.append(f"{branch_native:.1f}" if branch_native else "NaN")
        row.append(f"{branch_child:.1f}" if branch_child else "NaN")
        row.append(f"{sim_native:.1f}" if sim_native else "NaN")
        row.append(f"{sim_child:.1f}" if sim_child else "NaN")
        row.append(f"{branch_overhead:.1f}" if branch_overhead else "NaN")
        row.append(f"{sim_overhead:.1f}" if sim_overhead else "NaN")
        
        f.write(",".join(row) + "\n")

print("Comparison written to " + output_file)
EOF

# Generate summary statistics
python3 - "$COMPARE_RAW" "$COMPARE_SUMMARY" <<'EOF'
import sys
import csv
import statistics

input_file = sys.argv[1]
output_file = sys.argv[2]

branch_overheads = []
sim_overheads = []

with open(input_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            b_ovh = float(row['branch_overhead_pct'])
            s_ovh = float(row['sim_overhead_pct'])
            if b_ovh > 0 and s_ovh > 0:  # Only consider positive overheads
                branch_overheads.append(b_ovh)
                sim_overheads.append(s_ovh)
        except:
            pass

with open(output_file, 'w') as f:
    f.write("=" * 70 + "\n")
    f.write("BENCHMARK COMPARISON: Branch Extension vs OrpheusDB Simulator\n")
    f.write("=" * 70 + "\n\n")
    
    if branch_overheads and sim_overheads:
        f.write("Branch Extension Overhead:\n")
        f.write(f"  Median: {statistics.median(branch_overheads):.2f}%\n")
        f.write(f"  Mean:   {statistics.mean(branch_overheads):.2f}%\n")
        f.write(f"  Min:    {min(branch_overheads):.2f}%\n")
        f.write(f"  Max:    {max(branch_overheads):.2f}%\n")
        f.write(f"  StdDev: {statistics.stdev(branch_overheads) if len(branch_overheads) > 1 else 0:.2f}%\n")
        f.write(f"  Count:  {len(branch_overheads)} queries\n\n")
        
        f.write("OrpheusDB Simulator Overhead:\n")
        f.write(f"  Median: {statistics.median(sim_overheads):.2f}%\n")
        f.write(f"  Mean:   {statistics.mean(sim_overheads):.2f}%\n")
        f.write(f"  Min:    {min(sim_overheads):.2f}%\n")
        f.write(f"  Max:    {max(sim_overheads):.2f}%\n")
        f.write(f"  StdDev: {statistics.stdev(sim_overheads) if len(sim_overheads) > 1 else 0:.2f}%\n")
        f.write(f"  Count:  {len(sim_overheads)} queries\n\n")
        
        diff = statistics.median(sim_overheads) - statistics.median(branch_overheads)
        f.write("Analysis:\n")
        f.write(f"  Overhead Difference (Sim - Branch): {diff:+.2f}%\n")
        if diff > 0:
            f.write(f"  Conclusion: Branch extension is {abs(diff):.1f}% more efficient\n")
        elif diff < 0:
            f.write(f"  Conclusion: OrpheusDB simulator is {abs(diff):.1f}% more efficient\n")
        else:
            f.write(f"  Conclusion: Both approaches have comparable performance\n")
    else:
        f.write("ERROR: Insufficient data for comparison\n")

print("Summary written to " + output_file)
EOF

# Display summary
echo ""
cat "$COMPARE_SUMMARY"

echo ""
echo "Detailed comparison written to: $COMPARE_RAW"
echo ""
