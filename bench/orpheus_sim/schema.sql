-- OrpheusDB Simulator Schema for TPC-H Benchmark
-- Implements the OrpheusDB data model with three tables per CVD:
--   - lineitem_data: stores records with row IDs
--   - lineitem_index: maps (record_id, version_id) pairs
--   - lineitem_version: metadata for each version
--
-- The simulator focuses on LINEITEM (the largest TPC-H table at SF=1, ~6M rows)
-- Other TPC-H tables (region, nation, part, supplier, etc.) are not versioned.

DROP TABLE IF EXISTS lineitem, orders, partsupp, customer, supplier, part, nation, region CASCADE;
DROP TABLE IF EXISTS lineitem_index, lineitem_data, lineitem_version CASCADE;

-- ============================================================================
-- Standard TPC-H tables (non-versioned)
-- ============================================================================

CREATE TABLE region (
    r_regionkey  INTEGER       NOT NULL,
    r_name       CHAR(25)      NOT NULL,
    r_comment    VARCHAR(152),
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE nation (
    n_nationkey  INTEGER       NOT NULL,
    n_name       CHAR(25)      NOT NULL,
    n_regionkey  INTEGER        NOT NULL,
    n_comment    VARCHAR(152),
    PRIMARY KEY (n_nationkey)
);

CREATE TABLE part (
    p_partkey     INTEGER        NOT NULL,
    p_name        VARCHAR(55)    NOT NULL,
    p_mfgr        CHAR(25)       NOT NULL,
    p_brand       CHAR(10)       NOT NULL,
    p_type        VARCHAR(25)    NOT NULL,
    p_size        INTEGER        NOT NULL,
    p_container   CHAR(10)       NOT NULL,
    p_retailprice DECIMAL(15,2)  NOT NULL,
    p_comment     VARCHAR(23)    NOT NULL,
    PRIMARY KEY (p_partkey)
);

CREATE TABLE supplier (
    s_suppkey     INTEGER        NOT NULL,
    s_name        CHAR(25)       NOT NULL,
    s_address     VARCHAR(40)    NOT NULL,
    s_nationkey   INTEGER        NOT NULL,
    s_phone       CHAR(15)       NOT NULL,
    s_acctbal     DECIMAL(15,2)  NOT NULL,
    s_comment     VARCHAR(101)   NOT NULL,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE partsupp (
    ps_partkey     INTEGER        NOT NULL,
    ps_suppkey     INTEGER        NOT NULL,
    ps_availqty    INTEGER        NOT NULL,
    ps_supplycost  DECIMAL(15,2)  NOT NULL,
    ps_comment     VARCHAR(199)   NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE customer (
    c_custkey     INTEGER        NOT NULL,
    c_name        VARCHAR(25)    NOT NULL,
    c_address     VARCHAR(40)    NOT NULL,
    c_nationkey   INTEGER        NOT NULL,
    c_phone       CHAR(15)       NOT NULL,
    c_acctbal     DECIMAL(15,2)  NOT NULL,
    c_mktsegment  CHAR(10)       NOT NULL,
    c_comment     VARCHAR(117)   NOT NULL,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE orders (
    o_orderkey      INTEGER        NOT NULL,
    o_custkey       INTEGER        NOT NULL,
    o_orderstatus   CHAR(1)        NOT NULL,
    o_totalprice    DECIMAL(15,2)  NOT NULL,
    o_orderdate     DATE           NOT NULL,
    o_orderpriority CHAR(15)       NOT NULL,
    o_clerk         CHAR(15)       NOT NULL,
    o_shippriority  INTEGER        NOT NULL,
    o_comment       VARCHAR(79)    NOT NULL,
    PRIMARY KEY (o_orderkey)
);

-- ============================================================================
-- OrpheusDB Simulator: LINEITEM Versioning Infrastructure
-- ============================================================================

-- lineitem_data: stores actual lineitem records with row IDs
CREATE TABLE lineitem_data (
    rid                BIGSERIAL    PRIMARY KEY,  -- unique record ID
    l_orderkey         INTEGER        NOT NULL,
    l_partkey          INTEGER        NOT NULL,
    l_suppkey          INTEGER        NOT NULL,
    l_linenumber       INTEGER        NOT NULL,
    l_quantity         DECIMAL(15,2)  NOT NULL,
    l_extendedprice    DECIMAL(15,2)  NOT NULL,
    l_discount         DECIMAL(15,2)  NOT NULL,
    l_tax              DECIMAL(15,2)  NOT NULL,
    l_returnflag       CHAR(1)        NOT NULL,
    l_linestatus       CHAR(1)        NOT NULL,
    l_shipdate         DATE           NOT NULL,
    l_commitdate       DATE           NOT NULL,
    l_receiptdate      DATE           NOT NULL,
    l_shipinstruct     CHAR(25)       NOT NULL,
    l_shipmode         CHAR(10)       NOT NULL,
    l_comment          VARCHAR(44)    NOT NULL
);

-- lineitem_index: maps record IDs to version IDs (many-to-many)
CREATE TABLE lineitem_index (
    rid                BIGINT       NOT NULL,
    vid                INTEGER      NOT NULL,
    PRIMARY KEY (rid, vid),
    FOREIGN KEY (rid) REFERENCES lineitem_data(rid) ON DELETE CASCADE,
    FOREIGN KEY (vid) REFERENCES lineitem_version(vid) ON DELETE CASCADE
);

-- lineitem_version: metadata for each version
CREATE TABLE lineitem_version (
    vid                INTEGER      PRIMARY KEY,
    parent_vid         INTEGER      REFERENCES lineitem_version(vid) ON DELETE SET NULL,
    author             TEXT         DEFAULT 'system',
    commit_msg         TEXT,
    created_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    num_records        INTEGER      DEFAULT 0
);

-- Create indexes for performance
CREATE INDEX idx_lineitem_index_vid ON lineitem_index(vid);
CREATE INDEX idx_lineitem_index_rid ON lineitem_index(rid);
CREATE INDEX idx_lineitem_data_orderkey ON lineitem_data(l_orderkey);

-- ============================================================================
-- View: lineitem (used by queries)
-- Resolves to the current active version based on session state
-- ============================================================================

-- Temporary session table to track current version
CREATE TEMP TABLE session_state (
    current_vid INTEGER
) ON COMMIT PRESERVE;

-- Initialize with version 0
INSERT INTO lineitem_version (vid, author, commit_msg) VALUES (0, 'system', 'base');

-- Create a view that materializes the current version on demand
-- This view is updated when switching versions
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
WHERE i.vid = COALESCE((SELECT current_vid FROM session_state LIMIT 1), 0);
