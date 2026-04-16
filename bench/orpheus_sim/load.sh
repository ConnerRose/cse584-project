#!/usr/bin/env bash
# Convenience wrapper for loading TPC-H data into orpheus_sim
#
# This script:
# 1. Copies .tbl files to /tmp (PostgreSQL sandboxing workaround on macOS)
# 2. Substitutes the path into load.sql
# 3. Executes it via psql
#
# Usage:
#   ./load.sh
#   DB=mydb ./load.sh

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DATA_DIR="$HERE/../tpch/data"
DB="${DB:-postgres}"

if [ ! -d "$TPCH_DATA_DIR" ]; then
    echo "error: TPC-H data not found at $TPCH_DATA_DIR" >&2
    echo "Run: cd ../tpch && ./setup.sh" >&2
    exit 1
fi

# On macOS, PostgreSQL has filesystem sandboxing; copy to /tmp for access
echo "==> Copying TPC-H data to /tmp..."
cp "$TPCH_DATA_DIR"/*.tbl /tmp/ 2>/dev/null || true

# Substitute path (using /tmp) and pipe directly to psql
echo "==> Loading TPC-H data into orpheus_sim..."
sed "s|@TPCH_DATA_PATH@|/tmp|g" "$HERE/load.sql" | psql -q "$DB"
