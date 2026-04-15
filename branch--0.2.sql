-- Branch extension SQL definitions (v0.2: view-based branches)

CREATE SCHEMA IF NOT EXISTS branch;

CREATE TABLE IF NOT EXISTS branch.branches (
    branch_id   SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    parent_id   INTEGER REFERENCES branch.branches(branch_id),
    base_table  TEXT NOT NULL,
    delta_table TEXT,
    materialized BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION branch.create_branch(
    new_branch TEXT,
    from_branch TEXT DEFAULT 'main'
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_create'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.switch_branch(
    target_branch TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_switch'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.apply_branch(
    branch_name TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_apply'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.rollback_branch(
    branch_name TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_rollback'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.materialize_branch(
    branch_name TEXT
) RETURNS VOID AS 'MODULE_PATHNAME', 'branch_materialize'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.preview()
RETURNS SETOF RECORD AS 'MODULE_PATHNAME', 'branch_preview'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION branch.current_branch()
RETURNS TEXT AS 'MODULE_PATHNAME', 'branch_current'
LANGUAGE C STRICT;
