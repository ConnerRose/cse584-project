#!/usr/bin/env bash
# Setup script for orpheus_sim TPC-H benchmark
#
# Ensures TPC-H data exists. If not, generates it.
# This script only manages data generation; see README for SQL pipeline.
#
# Usage:
#   ./setup.sh
#   SF=0.1 ./setup.sh

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$HERE/../tpch"
TPCH_DATA_DIR="$TPCH_DIR/data"

# Ensure TPC-H data exists; if not, run tpch/setup.sh
if [ ! -d "$TPCH_DATA_DIR" ]; then
    echo "==> TPC-H data not found. Running tpch/setup.sh..."
    (cd "$TPCH_DIR" && ./setup.sh)
fi

echo "==> TPC-H data ready at: $TPCH_DATA_DIR"
echo ""
echo "Next steps (run these in order):"
echo "    psql postgres -f schema.sql"
echo "    psql postgres -f load.sql"
echo "    psql postgres -f init_sim.sql"
echo "    ./run.sh"
