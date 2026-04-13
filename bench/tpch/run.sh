#!/usr/bin/env bash
# Benchmark runner.
#
# For each TPC-H query, times it under two configurations:
#
#   1. native — switch to 'main' branch (search_path = public), run the query
#   2. branch — switch to 'bench' branch (search_path = branch_work_bench,
#               public), run the same query. LINEITEM is resolved from the
#               working copy in branch_work_bench; all other tables still
#               come from public.
#
# The delta between the two is the overhead of the branch layer for a
# read-only workload against a pre-materialized working copy with an empty
# delta log. Results go to results.csv.
#
# Usage:
#   ./run.sh
#   ITERS=5 ./run.sh
#   QUERIES="1 6 14" ./run.sh
#   DB=mydb ./run.sh

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QUERIES_DIR="$HERE/queries"
RESULTS="$HERE/results.csv"

DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
QUERIES="${QUERIES:-$(seq 1 22)}"

if [ ! -d "$QUERIES_DIR" ]; then
    echo "error: $QUERIES_DIR not found. Run ./setup.sh first." >&2
    exit 1
fi

# time_query <branch> <query_file>
#   Switches to <branch>, enables \timing, runs the query file via \i,
#   sums all "Time:" lines into total ms. Prints "NaN" on failure.
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

echo "query,mode,iter,ms" > "$RESULTS"

for i in $QUERIES; do
    qfile="$QUERIES_DIR/q$i.sql"
    if [ ! -f "$qfile" ]; then
        echo "skip q$i (missing $qfile)" >&2
        continue
    fi

    echo "==> q$i"
    for iter in $(seq 1 "$ITERS"); do
        native_ms=$(time_query main   "$qfile")
        echo "q$i,native,$iter,$native_ms" >> "$RESULTS"

        branch_ms=$(time_query bench  "$qfile")
        echo "q$i,branch,$iter,$branch_ms" >> "$RESULTS"

        printf "    iter %d: native=%sms branch=%sms\n" "$iter" "$native_ms" "$branch_ms"
    done
done

echo ""
echo "==> Results written to $RESULTS"
echo ""

# Summary — median per (query, mode), using sort instead of gawk asort
echo "Summary (median ms per query):"
printf "%-6s %12s %12s %12s\n" "query" "native" "branch" "overhead"
for i in $QUERIES; do
    q="q$i"
    nmed=$(awk -F, -v q="$q" '$1==q && $2=="native" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    bmed=$(awk -F, -v q="$q" '$1==q && $2=="branch" { print $4 }' "$RESULTS" \
           | grep -v NaN | sort -n \
           | awk '{a[NR]=$0} END{ if (NR>0) print a[int((NR+1)/2)] }')
    if [ -z "$nmed" ] || [ -z "$bmed" ]; then
        printf "%-6s %12s %12s %12s\n" "$q" "${nmed:-NaN}" "${bmed:-NaN}" "n/a"
    else
        ovh=$(awk -v n="$nmed" -v b="$bmed" 'BEGIN { if (n>0) printf "%.2fx", b/n; else print "n/a" }')
        printf "%-6s %12s %12s %12s\n" "$q" "$nmed" "$bmed" "$ovh"
    fi
done
