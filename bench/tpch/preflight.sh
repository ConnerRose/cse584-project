#!/usr/bin/env bash
# Optional checks before benchmarks. Source from run_all.sh or run standalone:
#   source ./preflight.sh && preflight_check "$DB"

preflight_check() {
    local db="${1:-postgres}"
    if ! command -v psql >/dev/null 2>&1; then
        echo "error: psql not found in PATH" >&2
        return 1
    fi
    if ! psql -q -X -v ON_ERROR_STOP=1 "$db" -c "SELECT 1" >/dev/null 2>&1; then
        echo "error: cannot connect to database '$db'" >&2
        return 1
    fi
    local n
    n=$(psql -q -X -A -t -v ON_ERROR_STOP=1 "$db" -c "SELECT count(*)::bigint FROM public.lineitem" 2>/dev/null | tr -d '[:space:]' || echo "0")
    if [ "${n:-0}" -eq 0 ] 2>/dev/null; then
        echo "error: public.lineitem is empty or missing. Run from bench/tpch:" >&2
        echo "  psql \$DB -f schema.sql && psql \$DB -f load.sql && psql \$DB -f init_branch.sql" >&2
        return 1
    fi
    return 0
}
