-- Update the lineitem view to query based on current session version
-- This must run AFTER versioning_functions.sql so get_current_version() exists

\set ON_ERROR_STOP on

-- Drop the old hardcoded view
DROP VIEW IF EXISTS lineitem CASCADE;

-- Create a new view that uses the session state function
CREATE OR REPLACE VIEW lineitem AS
SELECT 
    d.l_orderkey,
    d.l_partkey,
    d.l_suppkey,
    d.l_linenumber,
    d.l_quantity,
    d.l_extendedprice,
    d.l_discount,
    d.l_tax,
    d.l_returnflag,
    d.l_linestatus,
    d.l_shipdate,
    d.l_commitdate,
    d.l_receiptdate,
    d.l_shipinstruct,
    d.l_shipmode,
    d.l_comment
FROM lineitem_data d
INNER JOIN lineitem_index i ON d.rid = i.rid
WHERE i.vid = get_current_version();
