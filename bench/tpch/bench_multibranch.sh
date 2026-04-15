#!/usr/bin/env bash
# Multi-branch scaling: for each N in 1,2,4,8 create N independent branches from main,
# measure cumulative creation time and Q6 read latency on branch mb1.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DB="${DB:-postgres}"
OUT="${MULTIBRANCH_CSV:-$HERE/multibranch_results.csv}"
echo "num_branches,metric,iter,value_ms" > "$OUT"

drop_mb_branches() {
    local max="$1"
    local i
    psql -q -X -v ON_ERROR_STOP=1 "$DB" -c "SELECT branch.switch_branch('main');" || true
    for i in $(seq 1 "$max"); do
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP SCHEMA IF EXISTS branch_work_mb${i} CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP TABLE IF EXISTS branch.branch_delta_mb${i} CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DROP FUNCTION IF EXISTS branch._capture_mb${i}() CASCADE" 2>/dev/null || true
        psql -q -X -v ON_ERROR_STOP=0 "$DB" -c "DELETE FROM branch.branches WHERE name = 'mb$i'" 2>/dev/null || true
    done
}

for n in 1 2 4 8; do
    for iter in 1 2 3; do
        drop_mb_branches 8

        cre_sql=$'SELECT branch.switch_branch(\'main\');'
        for i in $(seq 1 "$n"); do
            cre_sql+=$'\n'"SELECT branch.create_branch('mb${i}', 'main');"
        done

        total_ms=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF | awk '/^Time:/ { gsub("ms",""); s += \$2 } END { printf "%.3f\n", s+0 }'
\\timing on
${cre_sql}
EOF
)
        echo "$n,create_total,$iter,$total_ms" >> "$OUT"

        read_ms=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF | awk '/^Time:/ { gsub("ms",""); s += \$2 } END { printf "%.3f\n", s+0 }'
SET statement_timeout = '120s';
SELECT branch.switch_branch('mb1');
\\timing on
\\i $HERE/queries/q6.sql
EOF
)
        echo "$n,read_q6_on_mb1,$iter,$read_ms" >> "$OUT"
    done
done

drop_mb_branches 8
echo "multibranch results: $OUT"
