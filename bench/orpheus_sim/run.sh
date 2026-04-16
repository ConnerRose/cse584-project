#!/usr/bin/env bash
# Benchmark runner for orpheus_sim (OrpheusDB simulator)
#
# For each TPC-H query, times it under two configurations:
#
#   1. native — query version 0 (base version, search_path = public)
#   2. sim    — query version 1 (child version, search_path = public)
#
# The delta between the two is the overhead of the OrpheusDB-style versioning
# for a read-only workload with full table materialization (inheritance).
#
# Results go to results_sim.csv

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QUERIES_DIR="$(cd "$HERE/../tpch/queries" && pwd)"
RESULTS="$HERE/results_sim.csv"

DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
QUERIES="${QUERIES:-$(seq 1 22)}"

if [ ! -d "$QUERIES_DIR" ]; then
    echo "error: $QUERIES_DIR not found. Run bench/tpch/setup.sh first." >&2
    exit 1
fi

# time_query <version_id> <query_file>
#   Switches to <version_id>, enables \timing, runs the query file via \i,
#   sums all "Time:" lines into total ms. Prints "NaN" on failure.
time_query() {
    local vid="$1"
    local qfile="$2"
    local out rc
    out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF
SELECT switch_version($vid);
\timing on
\i $qfile
EOF
)
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "NaN"
        return
    fi
    echo "$out" | awk '/^Time:/ { gsub("ms",""); s += $2 } END { if (s>0) printf "%.3f\n", s; else print "NaN" }'
}

echo "query,mode,iter,ms" > "$RESULTS"

for i in $QUERIES; do
    qfile="$QUERIES_DIR/q$i.sql"
    if [ ! -f "$qfile" ]; then
        echo "skip q$i (missing $qfile)" >&2
        continue
    fi

    echo "==> q$i"
    for iter in $(seq 1 "$ITERS"); do
        native_ms=$(time_query 0 "$qfile")
        echo "q$i,native,$iter,$native_ms" >> "$RESULTS"

        sim_ms=$(time_query 1 "$qfile")
        echo "q$i,sim,$iter,$sim_ms" >> "$RESULTS"

        printf "    iter %d: native=%sms sim=%sms\n" "$iter" "$native_ms" "$sim_ms"
    done
done

echo ""
echo "==> Results written to $RESULTS"echo ""

# Summary — median per (query, mode), using sort instead of gawk asort
echo "Summary (median ms per query):"
printf "%-6s %12s %12s %12s\n" "query" "native" "sim" "overhead"
for i in $QUERIES; do
    q="q$i"
    nmed=$(awk -F, -v q="$q" '$1==q && $2=="native" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    smed=$(awk -F, -v q="$q" '$1==q && $2=="sim" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    if [ -z "$nmed" ] || [ -z "$smed" ]; then
        printf "%-6s %12s %12s %12s\n" "$q" "${nmed:-NaN}" "${smed:-NaN}" "n/a"
    else
        ovh=$(awk -v n="$nmed" -v s="$smed" 'BEGIN { if (n>0) printf "%.2fx", s/n; else print "n/a" }')
        printf "%-6s %12s %12s %12s\n" "$q" "$nmed" "$smed" "$ovh"
    fi
done

# Generate graphs
echo ""
echo "==> Generating graphs..."
python3 "$HERE/graph.py" "$RESULTS"