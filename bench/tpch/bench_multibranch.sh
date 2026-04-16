#!/usr/bin/env bash
# Multi-branch scaling benchmark.
#
# For each N in 1,2,4,8 creates N independent branches from main and measures:
#   (After creation, each mb{i} is materialized from public.lineitem so storage
#    and Q6 reflect real data — lazy branches start empty.)
#   create_total_ms   -- wall time to create all N branches (cumulative)
#   create_last_ms    -- wall time to create only the Nth branch (per-branch cost)
#   storage_mb        -- total disk space consumed by all N branch work schemas + delta tables
#   read_q6_ms        -- TPC-H Q6 latency on branch mb1 (isolation: does N affect reads?)
#
# CSV columns: num_branches, metric, iter, value
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DB="${DB:-postgres}"
OUT="${MULTIBRANCH_CSV:-$HERE/multibranch_results.csv}"
echo "num_branches,metric,iter,value" > "$OUT"

drop_mb_branches() {
    local max="$1"
    local i
    psql -q -X -v ON_ERROR_STOP=1 "$DB" -c "SELECT branch.switch_branch('main');" 2>/dev/null || true
    for i in $(seq 1 "$max"); do
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP SCHEMA IF EXISTS branch_work_mb${i} CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP TABLE IF EXISTS branch.branch_delta_mb${i} CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP FUNCTION IF EXISTS branch._capture_mb${i}() CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP FUNCTION IF EXISTS branch._lazy_mb${i}() CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DELETE FROM branch.branches WHERE name = 'mb$i'" 2>/dev/null || true
    done
}

for n in 1 2 4 8; do
    for iter in 1 2 3; do
        drop_mb_branches 16

        # --- Cumulative creation time: create all N branches and sum wall times ---
        cre_all_sql="SELECT branch.switch_branch('main');"$'\n'
        for i in $(seq 1 "$n"); do
            cre_all_sql+="SELECT branch.create_branch('mb${i}', 'main');"$'\n'
        done

        total_ms=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF | awk '/^Time:/ { gsub(/ms/,""); s += $2 } END { printf "%.3f\n", s+0 }'
\timing on
${cre_all_sql}
EOF
)
        echo "$n,create_total_ms,$iter,$total_ms" >> "$OUT"
        printf "  n=%2d iter=%d create_total=%.1fms\n" "$n" "$iter" "$total_ms"

        # --- Per-branch cost: time to create only the Nth branch (others already exist) ---
        last_ms=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF | awk '/^Time:/ { gsub(/ms/,""); s += $2 } END { printf "%.3f\n", s+0 }'
\timing on
SELECT branch.create_branch('mb${n}_extra', 'main');
EOF
)
        # Clean up the extra branch
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP SCHEMA IF EXISTS branch_work_mb${n}_extra CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP TABLE IF EXISTS branch.branch_delta_mb${n}_extra CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP FUNCTION IF EXISTS branch._capture_mb${n}_extra() CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP FUNCTION IF EXISTS branch._lazy_mb${n}_extra() CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DELETE FROM branch.branches WHERE name = 'mb${n}_extra'" 2>/dev/null || true

        echo "$n,create_last_ms,$iter,$last_ms" >> "$OUT"
        printf "  n=%2d iter=%d create_last=%.1fms\n" "$n" "$iter" "$last_ms"

        # Lazy branches start with empty working copies.  Materialize each
        # mb{i}.lineitem from public before storage/read (same pattern as run.sh).
        printf "    materializing mb1..mb%d (full lineitem copy each — may take many minutes)...\n" "$n"
        for i in $(seq 1 "$n"); do
            printf "      mb%d: copying from public.lineitem...\n" "$i"
            psql -q -X -v ON_ERROR_STOP=1 "$DB" <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM branch_work_mb${i}.lineitem LIMIT 1) THEN
    ALTER TABLE branch_work_mb${i}.lineitem DISABLE TRIGGER _lazy_materialize;
    ALTER TABLE branch_work_mb${i}.lineitem DISABLE TRIGGER _capture;
    INSERT INTO branch_work_mb${i}.lineitem SELECT * FROM public.lineitem;
    ALTER TABLE branch_work_mb${i}.lineitem ENABLE TRIGGER _capture;
    ALTER TABLE branch_work_mb${i}.lineitem ENABLE TRIGGER _lazy_materialize;
    UPDATE branch.branches SET materialized = true WHERE name = 'mb${i}';
  END IF;
END;
\$\$;
ANALYZE branch_work_mb${i}.lineitem;
EOF
            printf "      mb%d: done.\n" "$i"
        done

        # --- Storage: total disk space of all N branch work schemas + delta tables ---
        storage_mb=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" <<EOF
SELECT ROUND(
    COALESCE(SUM(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))), 0)
    ::numeric / (1024 * 1024), 2
)
FROM pg_tables
WHERE schemaname LIKE 'branch_work_mb%'
   OR (schemaname = 'branch' AND tablename LIKE 'branch_delta_mb%');
EOF
)
        echo "$n,storage_mb,$iter,$storage_mb" >> "$OUT"
        printf "  n=%2d iter=%d storage=%.1fMB\n" "$n" "$iter" "$storage_mb"

        # --- Read isolation: Q6 on mb1 while N branches exist ---
        read_ms=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF | awk '/^Time:/ { gsub(/ms/,""); s += $2 } END { printf "%.3f\n", s+0 }'
SET statement_timeout = '120s';
SELECT branch.switch_branch('mb1');
\timing on
\i $HERE/queries/q6.sql
EOF
)
        echo "$n,read_q6_ms,$iter,$read_ms" >> "$OUT"
        printf "  n=%2d iter=%d read_q6=%.1fms\n" "$n" "$iter" "$read_ms"
    done
done

drop_mb_branches 8
echo "multibranch results: $OUT"
