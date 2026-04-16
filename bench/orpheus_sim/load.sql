-- Load TPC-H data into orpheus_sim schema
-- This script loads data from tpch-kit generated .tbl files
-- and populates both standard tables and versioned lineitem tables
--
-- IMPORTANT: Before running, substitute @TPCH_DATA_PATH@ with the actual path to tpch/data.
-- Example usage:
--   sed 's|@TPCH_DATA_PATH@|/path/to/tpch/data|g' load.sql | psql postgres
--
-- Or use the convenience wrapper: bash load.sh

\set ON_ERROR_STOP on

-- Load non-versioned TPC-H tables from CSV files
COPY region (r_regionkey, r_name, r_comment) 
  FROM '@TPCH_DATA_PATH@/region.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY nation (n_nationkey, n_name, n_regionkey, n_comment) 
  FROM '@TPCH_DATA_PATH@/nation.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, 
           p_container, p_retailprice, p_comment)
  FROM '@TPCH_DATA_PATH@/part.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
  FROM '@TPCH_DATA_PATH@/supplier.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
  FROM '@TPCH_DATA_PATH@/partsupp.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, 
               c_mktsegment, c_comment)
  FROM '@TPCH_DATA_PATH@/customer.tbl'
  WITH (FORMAT csv, DELIMITER '|');

COPY orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
             o_orderpriority, o_clerk, o_shippriority, o_comment)
  FROM '@TPCH_DATA_PATH@/orders.tbl'
  WITH (FORMAT csv, DELIMITER '|');

-- Load LINEITEM into versioned schema
-- Insert into lineitem_data, then create index entries for version 0
COPY lineitem_data (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
                    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
                    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, 
                    l_shipmode, l_comment)
  FROM '@TPCH_DATA_PATH@/lineitem.tbl'
  WITH (FORMAT csv, DELIMITER '|');

-- Create index entries for all records in version 0 (the base version)
INSERT INTO lineitem_index (rid, vid)
SELECT rid, 0 FROM lineitem_data;

-- Update version 0 record count
UPDATE lineitem_version 
SET num_records = (SELECT COUNT(*) FROM lineitem_index WHERE vid = 0)
WHERE vid = 0;

-- Analyze tables for query planning
ANALYZE;

VACUUM FULL;
