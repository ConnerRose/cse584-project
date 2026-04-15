-- OrpheusDB Simulator: Versioning Functions and Procedures
-- Provides functions to switch versions, create versions, and manage the version DAG

\set ON_ERROR_STOP on

-- ============================================================================
-- Session State Management
-- ============================================================================

-- Get current active version
CREATE OR REPLACE FUNCTION get_current_version()
RETURNS INTEGER AS $$
DECLARE
    vid INTEGER;
BEGIN
    SELECT COALESCE(current_vid, 0) INTO vid FROM session_state LIMIT 1;
    RETURN vid;
END;
$$ LANGUAGE plpgsql STABLE;

-- Switch to a specific version
-- This updates the session state so queries use the specified version
CREATE OR REPLACE FUNCTION switch_version(v_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    count INTEGER;
BEGIN
    -- Verify version exists
    SELECT COUNT(*) INTO count FROM lineitem_version WHERE vid = v_id;
    IF count = 0 THEN
        RETURN format('ERROR: Version %s does not exist', v_id);
    END IF;
    
    -- Update session state
    DELETE FROM session_state;
    INSERT INTO session_state (current_vid) VALUES (v_id);
    
    RETURN format('Switched to version %s', v_id);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Version Creation and Management
-- ============================================================================

-- Create a new child version (branch)
-- If inherit_data is true, copy all records from parent to child
CREATE OR REPLACE FUNCTION create_child_version(
    parent_v INTEGER,
    msg TEXT,
    inherit_data BOOLEAN DEFAULT true
)
RETURNS INTEGER AS $$
DECLARE
    new_vid INTEGER;
    record_count INTEGER;
BEGIN
    -- Find next available version ID
    SELECT COALESCE(MAX(vid), -1) + 1 INTO new_vid FROM lineitem_version;
    
    -- Create new version entry
    INSERT INTO lineitem_version (vid, parent_vid, author, commit_msg, num_records)
    VALUES (new_vid, parent_v, 'benchmark', msg, 0);
    
    -- If inherit_data, copy all records from parent version
    IF inherit_data THEN
        INSERT INTO lineitem_index (rid, vid)
        SELECT rid, new_vid FROM lineitem_index WHERE vid = parent_v;
        
        SELECT COUNT(*) INTO record_count FROM lineitem_index WHERE vid = new_vid;
        UPDATE lineitem_version SET num_records = record_count WHERE vid = new_vid;
    END IF;
    
    RETURN new_vid;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Materialization and Checkout
-- ============================================================================

-- Materialize a specific version as a temporary table
-- Useful for inspection or export
CREATE OR REPLACE FUNCTION materialize_version(v_id INTEGER)
RETURNS TABLE(
    l_orderkey INTEGER,
    l_partkey INTEGER,
    l_suppkey INTEGER,
    l_linenumber INTEGER,
    l_quantity DECIMAL,
    l_extendedprice DECIMAL,
    l_discount DECIMAL,
    l_tax DECIMAL,
    l_returnflag CHAR,
    l_linestatus CHAR,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR,
    l_shipmode CHAR,
    l_comment VARCHAR
) AS $$
BEGIN
    RETURN QUERY
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
    WHERE i.vid = v_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================================
-- Version Information
-- ============================================================================

-- List all versions with their metadata
CREATE OR REPLACE FUNCTION list_versions()
RETURNS TABLE(
    vid INTEGER,
    parent_vid INTEGER,
    author TEXT,
    commit_msg TEXT,
    created_at TIMESTAMP,
    num_records INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM lineitem_version ORDER BY vid;
END;
$$ LANGUAGE plpgsql STABLE;

-- Get version statistics
CREATE OR REPLACE FUNCTION version_stats(v_id INTEGER)
RETURNS TABLE(
    total_records BIGINT,
    unique_records BIGINT,
    ancestors INTEGER
) AS $$
DECLARE
    ancestor_count INTEGER;
BEGIN
    -- Count ancestors (path to root)
    WITH RECURSIVE ancestry AS (
        SELECT vid, parent_vid FROM lineitem_version WHERE vid = v_id
        UNION ALL
        SELECT lv.vid, lv.parent_vid 
        FROM lineitem_version lv
        JOIN ancestry a ON a.parent_vid = lv.vid
    )
    SELECT COUNT(*) - 1 INTO ancestor_count FROM ancestry;
    
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM lineitem_index WHERE vid = v_id)::BIGINT,
        (SELECT COUNT(*) FROM lineitem_data WHERE rid IN (
            SELECT rid FROM lineitem_index WHERE vid = v_id
        ))::BIGINT,
        ancestor_count;
END;
$$ LANGUAGE plpgsql STABLE;
