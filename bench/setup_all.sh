#!/usr/bin/env bash
# Quick setup script for benchmarking
# Sets up TPC-H data and both benchmark implementations

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$HERE/tpch"
SIM_DIR="$HERE/orpheus_sim"

DB="${DB:-postgres}"

echo "==========================================================="
echo "  TPC-H Benchmark Setup"
echo "  Branch Extension vs OrpheusDB Simulator"
echo "==========================================================="
echo ""

# Step 1: Generate TPC-H data if needed
if [ ! -d "$TPCH_DIR/data" ] || [ -z "$(ls -A $TPCH_DIR/data 2>/dev/null)" ]; then
    echo "Step 1: Generating TPC-H data..."
    cd "$TPCH_DIR"
    ./setup.sh
    cd "$HERE"
    echo "✓ TPC-H data generated"
else
    echo "Step 1: TPC-H data already exists"
fi

echo ""

# Step 2: Setup branch extension benchmark
if [ -f "$TPCH_DIR/schema.sql" ]; then
    echo "Step 2: Setting up branch extension benchmark..."
    echo "  - Creating schema: psql -f $TPCH_DIR/schema.sql"
    psql -q "$DB" -f "$TPCH_DIR/schema.sql"
    echo "  - Loading data: psql -f $TPCH_DIR/load.sql"
    psql -q "$DB" -f "$TPCH_DIR/load.sql"
    echo "  - Initializing: psql -f $TPCH_DIR/init_branch.sql"
    psql -q "$DB" -f "$TPCH_DIR/init_branch.sql"
    echo "✓ Branch extension benchmark ready"
else
    echo "Step 2: Branch extension schema not found, skipping"
fi

echo ""

# Step 3: Setup orpheus_sim benchmark
echo "Step 3: Setting up orpheus_sim benchmark..."
cd "$SIM_DIR"
./setup.sh
cd "$HERE"
echo "✓ OrpheusDB simulator benchmark ready"

echo ""
echo "==========================================================="
echo "  Setup Complete!"
echo "==========================================================="
echo ""
echo "Next steps:"
echo ""
echo "  1. Run TPC-H benchmark with branch extension:"
echo "     cd $TPCH_DIR && ./run.sh"
echo ""
echo "  2. Run TPC-H benchmark with orpheus_sim:"
echo "     cd $SIM_DIR && ./run.sh"
echo ""
echo "  3. Compare results:"
echo "     cd $HERE && ./compare.sh"
echo ""
