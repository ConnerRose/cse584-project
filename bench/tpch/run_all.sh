#!/usr/bin/env bash

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

# Kill any leftover connections from previous interrupted runs before starting.
psql "${DB:-postgres}" -q -c \
    "SELECT pg_terminate_backend(pid)
     FROM pg_stat_activity
     WHERE pid <> pg_backend_pid()
       AND usename = current_user
       AND datname = current_database();" 2>/dev/null || true

# Kill any lingering backend connections from this user on Ctrl+C or error exit.
cleanup() {
    echo "" >&2
    echo "==> interrupted — terminating open postgres connections..." >&2
    psql "${DB:-postgres}" -q -c \
        "SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE pid <> pg_backend_pid()
           AND usename = current_user
           AND datname = current_database();" 2>/dev/null || true
    echo "==> done" >&2
}
trap cleanup INT TERM
DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
RUN_WALL_TESTS="${RUN_WALL_TESTS:-0}"
RUN_MULTIBRANCH="${RUN_MULTIBRANCH:-0}"
ALLOW_AUTOVACUUM_ON="${ALLOW_AUTOVACUUM_ON:-0}"
SKIP_READ="${SKIP_READ:-0}"
# IMPL tags output CSVs so results from different code versions don't collide.
# Usage: IMPL=materialized ./run_all.sh   (first pass, with materialized C code)
#        IMPL=delta        ./run_all.sh   (second pass, with pure-delta C code)
IMPL="${IMPL:-}"

if [ -n "$IMPL" ]; then
    WRITE_RESULTS="$HERE/write_results_${IMPL}.csv"
    STORAGE_RESULTS="$HERE/storage_results_${IMPL}.csv"
    READ_RESULTS="$HERE/results_${IMPL}.csv"
    CREATION_RESULTS_FILE="$HERE/creation_results_${IMPL}.csv"
else
    WRITE_RESULTS="$HERE/write_results.csv"
    STORAGE_RESULTS="$HERE/storage_results.csv"
    READ_RESULTS="$HERE/results.csv"
    CREATION_RESULTS_FILE="$HERE/creation_results.csv"
fi

# shellcheck source=/dev/null
source "$HERE/preflight.sh"
if command -v preflight_check >/dev/null 2>&1; then
    preflight_check "$DB" || {
        echo "preflight failed — fix the database state (see message above) or set DB=..." >&2
        exit 1
    }
fi

if [ "$SKIP_READ" = "1" ]; then
    echo "==> [1/5] skipping creation + read benchmark (SKIP_READ=1)"
else
    echo "==> [1/5] creation + read benchmark"
    ALLOW_AUTOVACUUM_ON="$ALLOW_AUTOVACUUM_ON" DB="$DB" ITERS="$ITERS" \
        RESULTS="$READ_RESULTS" CREATION_RESULTS="$CREATION_RESULTS_FILE" \
        "$HERE/run.sh"
fi

echo "==> [2/5] write benchmark${IMPL:+ (impl=$IMPL)}"
echo "mode,delta_pct,iter,ms" > "$WRITE_RESULTS"

# Snapshot public.lineitem once so each delta level starts from clean data.
# mvcc writes directly to public.lineitem (no isolation); without a restore
# the 2nd delta level would run on already-modified data.
printf "    snapshotting public.lineitem for write benchmark...\n"
psql -q -X -v ON_ERROR_STOP=1 "$DB" -c \
    "DROP TABLE IF EXISTS public._lineitem_snap;
     CREATE TABLE public._lineitem_snap (LIKE public.lineitem INCLUDING ALL);
     INSERT INTO public._lineitem_snap SELECT * FROM public.lineitem;"

for delta in 1 10; do
    # Restore public.lineitem to the clean snapshot before each delta level
    # so mvcc (which mutates it directly) doesn't corrupt later iterations.
    printf "    restoring public.lineitem for delta=%s%%...\n" "$delta"
    psql -q -X -v ON_ERROR_STOP=1 "$DB" -c \
        "TRUNCATE public.lineitem;
         INSERT INTO public.lineitem SELECT * FROM public._lineitem_snap;
         ANALYZE public.lineitem;"
    for mode in branch copy mvcc; do
        for iter in $(seq 1 "$ITERS"); do
            out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 -v mode="$mode" -v delta_pct="$delta" "$DB" -f "$HERE/bench_write.sql" 2>&1)
            ms=$(echo "$out" | awk '/^Time:/ { gsub(/ms/,""); s += $2 } END { if (s>0) printf "%.3f\n", s; else print "NaN" }')
            echo "$mode,$delta,$iter,$ms" >> "$WRITE_RESULTS"
            printf "    mode=%s delta=%s iter=%d ms=%s\n" "$mode" "$delta" "$iter" "$ms"
        done
    done
done

psql -q -X "$DB" -c "DROP TABLE IF EXISTS public._lineitem_snap;" 2>/dev/null || true
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
       pg_total_relation_size(r.oid),
       pg_relation_size(r.oid),
       pg_indexes_size(r.oid)
FROM rels r
JOIN pg_class c ON c.oid = r.oid::oid
ORDER BY label;
" >> "$STORAGE_RESULTS"
echo "==> storage results: $STORAGE_RESULTS"

if [ "$RUN_MULTIBRANCH" = "1" ]; then
    echo "==> [4/5] multi-branch scaling (longer)"
    chmod +x "$HERE/bench_multibranch.sh"
    _mb_csv="${IMPL:+$HERE/multibranch_results_${IMPL}.csv}"
    MULTIBRANCH_CSV="${_mb_csv:-$HERE/multibranch_results.csv}" DB="$DB" "$HERE/bench_multibranch.sh"
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
IMPL="$IMPL" python3 "$HERE/analyze_results.py"

echo "==> done"
