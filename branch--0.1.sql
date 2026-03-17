-- Branch extension SQL definitions

-- Create the branch schema to hold all metadata and delta tables
CREATE SCHEMA IF NOT EXISTS branch;

-- Metadata table: tracks all branches and their parentage
CREATE TABLE IF NOT EXISTS branch.branches (
    branch_id   SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    parent_id   INTEGER REFERENCES branch.branches(branch_id),
    base_table  TEXT NOT NULL,
    delta_table TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Session-level GUC to track the active branch
-- (Set via SET branch.active_branch = 'name')

-- Create a new branch from an existing one
CREATE OR REPLACE FUNCTION branch.create_branch(
    new_branch TEXT,
    from_branch TEXT DEFAULT 'main'
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_create'
LANGUAGE C STRICT;

-- Switch the current session to a different branch
CREATE OR REPLACE FUNCTION branch.switch_branch(
    target_branch TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_switch'
LANGUAGE C STRICT;

-- Apply a branch's delta log to the base table, then clear the deltas
CREATE OR REPLACE FUNCTION branch.apply_branch(
    branch_name TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_apply'
LANGUAGE C STRICT;

-- Discard all changes in a branch's delta table
CREATE OR REPLACE FUNCTION branch.rollback_branch(
    branch_name TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_rollback'
LANGUAGE C STRICT;

-- Preview the current branch state (base table + deltas applied)
CREATE OR REPLACE FUNCTION branch.preview()
RETURNS SETOF RECORD AS 'MODULE_PATHNAME', 'branch_preview'
LANGUAGE C STRICT;

-- Insert a row on the current branch
CREATE OR REPLACE FUNCTION branch.binsert(
    data JSONB
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_binsert'
LANGUAGE C STRICT;

-- Delete a row on the current branch by primary key
CREATE OR REPLACE FUNCTION branch.bdelete(
    pk_value INTEGER
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_bdelete'
LANGUAGE C STRICT;

-- Update a row on the current branch by primary key with JSONB changes
CREATE OR REPLACE FUNCTION branch.bupdate(
    pk_value INTEGER,
    data JSONB
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_bupdate'
LANGUAGE C STRICT;

-- Return the name of the currently active branch
CREATE OR REPLACE FUNCTION branch.current_branch()
RETURNS TEXT AS 'MODULE_PATHNAME', 'branch_current'
LANGUAGE C STRICT;
