-- Load TPC-H .tbl files into the schema. Run from the bench/tpch/ directory:
--   psql postgres -f load.sql
--
-- Loads largest-first to surface errors early.

\set ON_ERROR_STOP on

\copy region   FROM 'data/region.tbl'   WITH (DELIMITER '|');
\copy nation   FROM 'data/nation.tbl'   WITH (DELIMITER '|');
\copy supplier FROM 'data/supplier.tbl' WITH (DELIMITER '|');
\copy customer FROM 'data/customer.tbl' WITH (DELIMITER '|');
\copy part     FROM 'data/part.tbl'     WITH (DELIMITER '|');
\copy partsupp FROM 'data/partsupp.tbl' WITH (DELIMITER '|');
\copy orders   FROM 'data/orders.tbl'   WITH (DELIMITER '|');
\copy lineitem FROM 'data/lineitem.tbl' WITH (DELIMITER '|');

ANALYZE;

SELECT 'region'   AS table, count(*) FROM region
UNION ALL SELECT 'nation',   count(*) FROM nation
UNION ALL SELECT 'supplier', count(*) FROM supplier
UNION ALL SELECT 'customer', count(*) FROM customer
UNION ALL SELECT 'part',     count(*) FROM part
UNION ALL SELECT 'partsupp', count(*) FROM partsupp
UNION ALL SELECT 'orders',   count(*) FROM orders
UNION ALL SELECT 'lineitem', count(*) FROM lineitem;
