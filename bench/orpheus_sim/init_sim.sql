-- Initialize orpheus_sim versioning for benchmark
-- Creates version 1 as a child of version 0 (the base)
-- Version 1 inherits all records from version 0 (full materialization)

\set ON_ERROR_STOP on

-- Initialize session state (start with version 0)
DELETE FROM session_state;
INSERT INTO session_state (current_vid) VALUES (0);

-- Load versioning functions
\i versioning_functions.sql

-- Update the lineitem view to be dynamic based on session state
\i update_view.sql

-- Create version 1 as a child of version 0, inheriting all data
SELECT create_child_version(0, 'benchmark child branch', true);

-- Verify initialization
SELECT * FROM list_versions();
