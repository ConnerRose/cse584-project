#!/usr/bin/env bash
# Benchmark runner.
#
# Extends the original TPC-H read benchmark with:
#   1) creation-time microbenchmark (mvcc vs copy vs branch)
#   2) third read mode "copy" (copy_baseline.lineitem)
#   3) median + standard deviation reporting
#
# Usage:
#   ./run.sh
#   ITERS=5 ./run.sh
#   QUERIES="1 6 14" ./run.sh
#   DB=mydb ./run.sh

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
QUERIES_DIR="$HERE/queries"
RESULTS="$HERE/results.csv"
CREATION_RESULTS="$HERE/creation_results.csv"

DB="${DB:-postgres}"
ITERS="${ITERS:-3}"
# Default: skip Q17–Q22 (known pathological / very slow on stock PostgreSQL).
QUERIES="${QUERIES:-$(seq 1 16)}"
ALLOW_AUTOVACUUM_ON="${ALLOW_AUTOVACUUM_ON:-0}"

if [ ! -d "$QUERIES_DIR" ]; then
    echo "error: $QUERIES_DIR not found. Run ./setup.sh first." >&2
    exit 1
fi

extract_time_ms() {
    awk '/^Time:/ { gsub("ms",""); s += $2 } END { if (s>0) printf "%.3f\n", s; else print "NaN" }'
}

stats_line() {
    awk '
    {
        x[NR]=$1
        s+=$1
    }
    END {
        if (NR == 0) {
            print "NaN,NaN"
            exit
        }
        n=NR
        mean=s/n
        for (i=1; i<=n; i++) {
            d=x[i]-mean
            ss += d*d
        }
        std = (n > 1) ? sqrt(ss / (n-1)) : 0
        med = x[int((n+1)/2)]
        printf "%.3f,%.3f\n", med, std
    }'
}

# Best-effort guard for cleaner storage/bloat tests.
autovacuum_state=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" -c "SHOW autovacuum;" 2>/dev/null || echo "unknown")
if [ "$autovacuum_state" != "off" ] && [ "$ALLOW_AUTOVACUUM_ON" != "1" ]; then
    echo "error: autovacuum is '$autovacuum_state'."
    echo "set autovacuum=off for cleaner benchmarking, or run with ALLOW_AUTOVACUUM_ON=1."
    exit 1
fi

prep_for_mode() {
    local mode="$1"
    local qfile="$2"

    if [ "$mode" = "native" ]; then
        psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" >/dev/null 2>&1 <<EOF
SELECT branch.switch_branch('main');
SELECT 1 FROM pg_catalog.pg_class LIMIT 1;
\i $qfile
EOF
    elif [ "$mode" = "branch" ]; then
        psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" >/dev/null 2>&1 <<EOF
SELECT branch.switch_branch('bench');
SELECT 1 FROM pg_catalog.pg_class LIMIT 1;
\i $qfile
EOF
    else
        psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" >/dev/null 2>&1 <<EOF
SET search_path = copy_baseline, public;
SELECT 1 FROM pg_catalog.pg_class LIMIT 1;
\i $qfile
EOF
    fi
}

# time_query <mode> <query_file>
time_query() {
    local mode="$1"
    local qfile="$2"
    local out rc

    if [ "$mode" = "native" ]; then
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF
SET statement_timeout = '120s';
SELECT branch.switch_branch('main');
\timing on
\i $qfile
EOF
)
    elif [ "$mode" = "branch" ]; then
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF
SET statement_timeout = '120s';
SELECT branch.switch_branch('bench');
\timing on
\i $qfile
EOF
)
    else
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<EOF
SET statement_timeout = '120s';
SET search_path = copy_baseline, public;
\timing on
\i $qfile
EOF
)
    fi
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "NaN"
        return
    fi
    echo "$out" | extract_time_ms
}

time_creation() {
    local mode="$1"
    local out rc

    if [ "$mode" = "mvcc" ]; then
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<'EOF'
\timing on
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT 1;
ROLLBACK;
EOF
)
    elif [ "$mode" = "copy" ]; then
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<'EOF'
DROP TABLE IF EXISTS copy_baseline.lineitem;
\timing on
CREATE TABLE copy_baseline.lineitem (LIKE public.lineitem INCLUDING ALL);
INSERT INTO copy_baseline.lineitem SELECT * FROM public.lineitem;
ANALYZE copy_baseline.lineitem;
EOF
)
    else
        out=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$DB" 2>&1 <<'EOF'
DROP SCHEMA IF EXISTS branch_work_bench CASCADE;
DROP TABLE IF EXISTS branch.branch_delta_bench;
DROP FUNCTION IF EXISTS branch._capture_bench() CASCADE;
DELETE FROM branch.branches WHERE name = 'bench';
\timing on
SELECT branch.create_branch('bench', 'main');
ANALYZE branch.branch_delta_bench;
EOF
)
    fi
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "NaN"
        return
    fi
    echo "$out" | extract_time_ms
}

echo "mode,iter,ms" > "$CREATION_RESULTS"
for iter in $(seq 1 "$ITERS"); do
    mvcc_ms=$(time_creation mvcc)
    copy_ms=$(time_creation copy)
    branch_ms=$(time_creation branch)
    echo "mvcc,$iter,$mvcc_ms" >> "$CREATION_RESULTS"
    echo "copy,$iter,$copy_ms" >> "$CREATION_RESULTS"
    echo "branch,$iter,$branch_ms" >> "$CREATION_RESULTS"
    printf "creation iter %d: mvcc=%sms copy=%sms branch=%sms\n" "$iter" "$mvcc_ms" "$copy_ms" "$branch_ms"
done

echo "query,mode,iter,ms" > "$RESULTS"

for i in $QUERIES; do
    qfile="$QUERIES_DIR/q$i.sql"
    if [ ! -f "$qfile" ]; then
        echo "skip q$i (missing $qfile)" >&2
        continue
    fi

    echo "==> q$i"
    prep_for_mode native "$qfile"
    prep_for_mode branch "$qfile"
    prep_for_mode copy "$qfile"
    for iter in $(seq 1 "$ITERS"); do
        native_ms=$(time_query native "$qfile")
        branch_ms=$(time_query branch "$qfile")
        copy_ms=$(time_query copy "$qfile")
        echo "q$i,native,$iter,$native_ms" >> "$RESULTS"
        echo "q$i,branch,$iter,$branch_ms" >> "$RESULTS"
        echo "q$i,copy,$iter,$copy_ms" >> "$RESULTS"
        printf "    iter %d: native=%sms branch=%sms copy=%sms\n" "$iter" "$native_ms" "$branch_ms" "$copy_ms"
    done
done

echo ""
echo "==> Results written to $RESULTS"
echo "==> Creation results written to $CREATION_RESULTS"
echo ""

echo "Creation summary (median/stddev ms):"
printf "%-8s %12s %12s\n" "mode" "median" "stddev"
for mode in mvcc copy branch; do
    s=$(awk -F, -v m="$mode" '$1==m && $3!="NaN" { print $3 }' "$CREATION_RESULTS" | sort -n | stats_line)
    med=${s%%,*}
    std=${s##*,}
    printf "%-8s %12s %12s\n" "$mode" "$med" "$std"
done

echo ""
echo "Read summary (median/stddev ms per query):"
printf "%-6s %12s %12s %12s\n" "query" "native" "branch" "copy"
for i in $QUERIES; do
    q="q$i"
    n=$(awk -F, -v q="$q" '$1==q && $2=="native" && $4!="NaN" { print $4 }' "$RESULTS" | sort -n | stats_line)
    b=$(awk -F, -v q="$q" '$1==q && $2=="branch" && $4!="NaN" { print $4 }' "$RESULTS" | sort -n | stats_line)
    c=$(awk -F, -v q="$q" '$1==q && $2=="copy" && $4!="NaN" { print $4 }' "$RESULTS" | sort -n | stats_line)
    nmed=${n%%,*}
    bmed=${b%%,*}
    cmed=${c%%,*}
    printf "%-6s %12s %12s %12s\n" "$q" "$nmed" "$bmed" "$cmed"
done
