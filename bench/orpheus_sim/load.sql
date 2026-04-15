-- Load TPC-H data into orpheus_sim schema
-- This script loads data from tpch-kit generated .tbl files
-- and populates both standard tables and versioned lineitem tables

\set ON_ERROR_STOP on

-- Load non-versioned TPC-H tables from CSV files
-- Data files should be in ../tpch/data/ directory
-- This SQL file should be run from the bench/orpheus_sim directory

-- Get the absolute path to TPC-H data
\set tpch_data `find ../tpch/data -maxdepth 1 -name "region.tbl" | head -1 | sed 's|/region.tbl||'`

-- Check if data directory exists and has files
-- If the COPY fails, the data directory is not in the expected location

COPY region (r_regionkey, r_name, r_comment) 
  FROM :'tpch_data'/region.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY nation (n_nationkey, n_name, n_regionkey, n_comment) 
  FROM :'tpch_data'/nation.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, 
           p_container, p_retailprice, p_comment)
  FROM :'tpch_data'/part.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
  FROM :'tpch_data'/supplier.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
  FROM :'tpch_data'/partsupp.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, 
               c_mktsegment, c_comment)
  FROM :'tpch_data'/customer.tbl
  WITH (FORMAT csv, DELIMITER '|');

COPY orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
             o_orderpriority, o_clerk, o_shippriority, o_comment)
  FROM :'tpch_data'/orders.tbl
  WITH (FORMAT csv, DELIMITER '|');

-- Load LINEITEM into versioned schema
-- Insert into lineitem_data, then create index entries for version 0
COPY lineitem_data (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
                    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
                    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, 
                    l_shipmode, l_comment)
  FROM :'tpch_data'/lineitem.tbl
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
