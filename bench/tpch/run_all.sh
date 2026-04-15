#!/usr/bin/env bash

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
RUN_WALL_TESTS="${RUN_WALL_TESTS:-0}"
RUN_MULTIBRANCH="${RUN_MULTIBRANCH:-0}"
ALLOW_AUTOVACUUM_ON="${ALLOW_AUTOVACUUM_ON:-0}"

WRITE_RESULTS="$HERE/write_results.csv"
STORAGE_RESULTS="$HERE/storage_results.csv"

# shellcheck source=/dev/null
source "$HERE/preflight.sh"
if command -v preflight_check >/dev/null 2>&1; then
    preflight_check "$DB" || {
        echo "preflight failed — fix the database state (see message above) or set DB=..." >&2
        exit 1
    }
fi

echo "==> [1/5] creation + read benchmark"
ALLOW_AUTOVACUUM_ON="$ALLOW_AUTOVACUUM_ON" DB="$DB" ITERS="$ITERS" "$HERE/run.sh"

echo "==> [2/5] write benchmark"
echo "mode,delta_pct,iter,ms" > "$WRITE_RESULTS"
for delta in 1 10; do
    for mode in mvcc copy branch; do
        for iter in $(seq 1 "$ITERS"); do
            out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 -v mode="$mode" -v delta_pct="$delta" "$DB" -f "$HERE/bench_write.sql" 2>&1)
            ms=$(echo "$out" | awk '/^Time:/ { gsub("ms",""); s += $2 } END { if (s>0) printf "%.3f\n", s; else print "NaN" }')
            echo "$mode,$delta,$iter,$ms" >> "$WRITE_RESULTS"
            printf "    mode=%s delta=%s iter=%d ms=%s\n" "$mode" "$delta" "$iter" "$ms"
        done
    done
done
echo "==> write results: $WRITE_RESULTS"

psql -q -X -v ON_ERROR_STOP=1 "$DB" -c "SET statement_timeout = 0; ANALYZE public.lineitem; ANALYZE copy_baseline.lineitem; ANALYZE branch.branch_delta_bench;" 2>/dev/null || true

echo "==> [3/5] storage snapshot"
echo "label,total_bytes,heap_bytes,index_bytes" > "$STORAGE_RESULTS"
psql -q -X -A -t -F, -v ON_ERROR_STOP=1 "$DB" -c "
WITH rels AS (
    SELECT 'public_lineitem'::text AS label, 'public.lineitem'::regclass AS oid
    UNION ALL SELECT 'copy_lineitem', 'copy_baseline.lineitem'::regclass
    UNION ALL SELECT 'branch_work_lineitem', 'branch_work_bench.lineitem'::regclass
    UNION ALL SELECT 'branch_delta_lineitem', 'branch.branch_delta_bench'::regclass
)
SELECT r.label,
       CASE WHEN c.relkind = 'v' THEN 0::bigint ELSE pg_total_relation_size(r.oid) END,
       CASE WHEN c.relkind = 'v' THEN 0::bigint ELSE pg_relation_size(r.oid) END,
       CASE WHEN c.relkind = 'v' THEN 0::bigint ELSE pg_indexes_size(r.oid) END
FROM rels r
JOIN pg_class c ON c.oid = r.oid::oid
ORDER BY label;
" >> "$STORAGE_RESULTS"
echo "==> storage results: $STORAGE_RESULTS"

if [ "$RUN_MULTIBRANCH" = "1" ]; then
    echo "==> [4/5] multi-branch scaling (longer)"
    chmod +x "$HERE/bench_multibranch.sh"
    MULTIBRANCH_CSV="$HERE/multibranch_results.csv" DB="$DB" "$HERE/bench_multibranch.sh"
else
    echo "==> [4/5] skipping multi-branch (set RUN_MULTIBRANCH=1 to run)"
fi

if [ "$RUN_WALL_TESTS" = "1" ]; then
    echo "==> [5/5] mvcc wall demonstrations"
    psql -v ON_ERROR_STOP=1 "$DB" -f "$HERE/wall_tests.sql"
else
    echo "==> [5/5] skipping wall tests (set RUN_WALL_TESTS=1 to run)"
fi

echo "==> generating figures"
python3 "$HERE/analyze_results.py"

echo "==> done"
