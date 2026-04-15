-- Upgrade branch extension from 0.1 to 0.2 (view-based branches + materialize)

ALTER TABLE branch.branches
    ADD COLUMN IF NOT EXISTS materialized BOOLEAN NOT NULL DEFAULT false;

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
