#!/usr/bin/env bash
# Delta scaling benchmark.
#
# Tests the hypothesis that view-based query performance degrades as
# the delta table grows. For each delta size, we:
#   1. Truncate the delta table
#   2. Insert N random delta rows (UPDATE ops on existing lineitem PKs)
#   3. Run a TPC-H query through the branch view
#   4. Record the latency
#
# This informs whether a hybrid approach (view for small deltas,
# materialize above a threshold) would be beneficial.
#
# Usage:
#   ./run.sh
#   QUERY=6 ./run.sh
#   ITERS=5 ./run.sh
#   SIZES="0 100 1000 10000 50000 100000 500000" ./run.sh

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$HERE/../tpch"
RESULTS="$HERE/results.csv"

DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
QUERY="${QUERY:-6}"
SIZES="${SIZES:-0 100 1000 5000 10000 50000 100000 500000}"

QFILE="$TPCH_DIR/queries/q${QUERY}.sql"
if [ ! -f "$QFILE" ]; then
    echo "error: $QFILE not found. Run ../tpch/setup.sh first." >&2
    exit 1
fi

# time_query <branch> <query_file>
time_query() {
    local branch="$1"
    local qfile="$2"
    local out rc
    out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF
SELECT branch.switch_branch('$branch');
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

# Setup: ensure branch extension is loaded with a 'bench' branch on lineitem
echo "==> Initializing branch..."
psql -q -X "$DB" <<'SQL'
DROP SCHEMA IF EXISTS branch_work_bench CASCADE;
DROP EXTENSION IF EXISTS branch CASCADE;
CREATE EXTENSION branch;
INSERT INTO branch.branches (name, base_table) VALUES ('main', 'lineitem');
SELECT branch.create_branch('bench', 'main');
SQL

echo "==> Running delta scaling benchmark (Q${QUERY}, iters=${ITERS})"
echo "==> Delta sizes: $SIZES"
echo ""

echo "delta_size,mode,iter,ms" > "$RESULTS"

for sz in $SIZES; do
    echo "--- delta_size = $sz ---"

    # Populate delta table with $sz UPDATE deltas on random existing rows
    psql -q -X "$DB" <<EOF
TRUNCATE branch.branch_delta_bench;
INSERT INTO branch.branch_delta_bench (_op, l_orderkey, l_partkey, l_suppkey,
    l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
    l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
    l_shipinstruct, l_shipmode, l_comment)
SELECT 'U', l_orderkey, l_partkey, l_suppkey,
    l_linenumber, l_quantity + 1, l_extendedprice, l_discount, l_tax,
    l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
    l_shipinstruct, l_shipmode, l_comment
FROM lineitem
ORDER BY random()
LIMIT $sz;
ANALYZE branch.branch_delta_bench;
EOF

    for iter in $(seq 1 "$ITERS"); do
        if [ $((iter % 2)) -eq 1 ]; then
            native_ms=$(time_query main  "$QFILE")
            branch_ms=$(time_query bench "$QFILE")
        else
            branch_ms=$(time_query bench "$QFILE")
            native_ms=$(time_query main  "$QFILE")
        fi

        echo "$sz,native,$iter,$native_ms"  >> "$RESULTS"
        echo "$sz,branch,$iter,$branch_ms"  >> "$RESULTS"
        printf "  iter %d: native=%sms  branch=%sms\n" "$iter" "$native_ms" "$branch_ms"
    done
done

echo ""
echo "==> Results written to $RESULTS"
echo ""

# Summary
echo "Summary (median ms):"
printf "%-12s %12s %12s %12s\n" "delta_size" "native" "branch" "overhead"
for sz in $SIZES; do
    nmed=$(awk -F, -v s="$sz" '$1==s && $2=="native" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    bmed=$(awk -F, -v s="$sz" '$1==s && $2=="branch" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    if [ -z "$nmed" ] || [ -z "$bmed" ]; then
        printf "%-12s %12s %12s %12s\n" "$sz" "${nmed:-NaN}" "${bmed:-NaN}" "n/a"
    else
        ovh=$(awk -v n="$nmed" -v b="$bmed" 'BEGIN { if (n>0) printf "%.2fx", b/n; else print "n/a" }')
        printf "%-12s %12s %12s %12s\n" "$sz" "$nmed" "$bmed" "$ovh"
    fi
done

# Generate graphs
echo ""
echo "==> Generating graphs..."
python3 "$HERE/graph.py" "$RESULTS"
