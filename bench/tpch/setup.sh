#!/usr/bin/env bash
# Setup script for TPC-H benchmark.
#
# Clones tpch-kit, builds dbgen/qgen, generates SF=1 data and the 22 queries.
# Safe to re-run: skips work that is already done.
#
# Usage:
#   ./setup.sh          # SF=1 (default)
#   SF=0.1 ./setup.sh   # smaller dataset for quick testing

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
KIT_DIR="$HERE/tpch-kit"
DBGEN_DIR="$KIT_DIR/dbgen"
DATA_DIR="$HERE/data"
QUERIES_DIR="$HERE/queries"
SF="${SF:-1}"

# Detect MACHINE flag for tpch-kit
UNAME="$(uname -s)"
case "$UNAME" in
    Darwin) MACHINE=MACOS ;;
    Linux)  MACHINE=LINUX ;;
    *)      echo "unsupported OS: $UNAME" >&2; exit 1 ;;
esac

# 1. Clone tpch-kit if needed
if [ ! -d "$KIT_DIR" ]; then
    echo "==> Cloning tpch-kit..."
    git clone --depth 1 https://github.com/gregrahn/tpch-kit.git "$KIT_DIR"
fi

# 2. Build dbgen and qgen
if [ ! -x "$DBGEN_DIR/dbgen" ] || [ ! -x "$DBGEN_DIR/qgen" ]; then
    echo "==> Building dbgen/qgen (MACHINE=$MACHINE DATABASE=POSTGRESQL)..."
    (cd "$DBGEN_DIR" && make -s \
        MACHINE="$MACHINE" \
        DATABASE=POSTGRESQL \
        WORKLOAD=TPCH \
        CC=cc)
fi

# 3. Generate data at scale factor
mkdir -p "$DATA_DIR"
if ! ls "$DATA_DIR"/*.tbl >/dev/null 2>&1; then
    echo "==> Generating TPC-H data at SF=$SF (this may take a minute)..."
    (cd "$DBGEN_DIR" && ./dbgen -vf -s "$SF")
    mv "$DBGEN_DIR"/*.tbl "$DATA_DIR/"

    # Strip trailing '|' from each line so COPY with DELIMITER '|' loads cleanly
    echo "==> Stripping trailing delimiters..."
    for f in "$DATA_DIR"/*.tbl; do
        sed -i.bak 's/|$//' "$f"
        rm -f "$f.bak"
    done
fi

# 4. Generate queries using qgen
mkdir -p "$QUERIES_DIR"
if ! ls "$QUERIES_DIR"/q*.sql >/dev/null 2>&1; then
    echo "==> Generating queries..."
    export DSS_QUERY="$DBGEN_DIR/queries"
    for i in $(seq 1 22); do
        # Strip qgen cruft: "limit -1" and non-PostgreSQL interval precision "(3)"
        (cd "$DBGEN_DIR" && ./qgen -s "$SF" "$i") \
            | sed -e 's/limit -1//' -e 's/day ([0-9]*)/day/' \
            > "$QUERIES_DIR/q$i.sql"
    done

    # Fix qgen putting "limit N;" as a separate statement after the ";"
    python3 -c "
import re, glob
for f in glob.glob('$QUERIES_DIR/q*.sql'):
    with open(f) as fh:
        text = fh.read()
    m = re.search(r';\s*\n(limit\s+\d+)\s*;', text)
    if m:
        lim = m.group(1)
        text = re.sub(r';\s*\n' + re.escape(lim) + r'\s*;', '\n' + lim + ';', text)
        with open(f, 'w') as fh:
            fh.write(text)
"
fi

echo "==> Done."
echo "    Data:    $DATA_DIR"
echo "    Queries: $QUERIES_DIR"
echo ""
echo "Next steps:"
echo "    psql postgres -f schema.sql"
echo "    psql postgres -f load.sql"
echo "    psql postgres -f init_branch.sql"
echo "    ./run.sh"
