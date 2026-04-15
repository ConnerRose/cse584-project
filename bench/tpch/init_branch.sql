-- Install the branch extension and set up the benchmark branch.
--
-- Design:
--   - Register 'main' on LINEITEM (~6M rows at SF=1).
--   - 'bench' is a view-based child branch (no eager copy): empty delta log,
--     reads resolve to public.lineitem via the view. Creation is O(1).

\set ON_ERROR_STOP on

-- Drop any leftover work schemas from previous runs.
-- The extension does not own these (created via SPI), so DROP EXTENSION CASCADE
-- does not remove them automatically.
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT nspname FROM pg_namespace
        WHERE nspname LIKE 'branch_work_%' OR nspname = 'copy_baseline'
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', r.nspname);
    END LOOP;
END;
$$;

DROP EXTENSION IF EXISTS branch CASCADE;
CREATE EXTENSION branch;

-- Register 'main' against LINEITEM
INSERT INTO branch.branches (name, base_table) VALUES ('main', 'lineitem');

-- Create an empty child branch for benchmarking
SELECT branch.create_branch('bench', 'main');

-- Create a full-copy baseline in a separate schema so we can benchmark
-- branch-vs-copy with identical SQL query text via search_path switching.
CREATE SCHEMA IF NOT EXISTS copy_baseline;
DROP TABLE IF EXISTS copy_baseline.lineitem;
CREATE TABLE copy_baseline.lineitem (LIKE public.lineitem INCLUDING ALL);
INSERT INTO copy_baseline.lineitem SELECT * FROM public.lineitem;

ANALYZE copy_baseline.lineitem;
ANALYZE branch.branch_delta_bench;

-- Small metadata table used by wall tests to persist timestamps/notes.
CREATE TABLE IF NOT EXISTS public.bench_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
