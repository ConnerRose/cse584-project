#!/usr/bin/env bash
# Setup script for benchmarking
# Orchestrates setup of both TPC-H benchmark implementations by calling their individual setup scripts

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPCH_DIR="$HERE/tpch"
SIM_DIR="$HERE/orpheus_sim"

echo "==========================================================="
echo "  TPC-H Benchmark Setup"
echo "  Branch Extension vs OrpheusDB Simulator"
echo "==========================================================="
echo ""

# Step 1: Setup TPC-H benchmark (includes data generation)
echo "Step 1: Setting up TPC-H benchmark (branch extension)..."
cd "$TPCH_DIR"
./setup.sh || {
    echo "error: failed to setup TPC-H benchmark" >&2
    exit 1
}
cd "$HERE"
echo "✓ TPC-H benchmark ready"

echo ""

# Step 2: Setup OrpheusDB simulator benchmark
echo "Step 2: Setting up OrpheusDB simulator benchmark..."
cd "$SIM_DIR"
./setup.sh || {
    echo "error: failed to setup OrpheusDB simulator benchmark" >&2
    exit 1
}
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
