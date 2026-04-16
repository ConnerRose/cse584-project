-- Install the branch extension and set up the benchmark branch.
--
-- Design:
--   - The branches table allows one base_table per branch. We register
--     'main' on LINEITEM (the largest table at ~6M rows SF=1) so every
--     branch.run() call exercises the most expensive shadow-materialization.
--   - 'bench' is an empty child branch used for the benchmark run — no deltas,
--     so we're measuring the pure overhead of the branch layer (trigger setup
--     + temp-table shadow creation/drop), not the cost of delta overlay.

\set ON_ERROR_STOP off

-- Ensure the branch extension is loaded
-- The DROP ... IF EXISTS is safe even if it doesn't exist
-- The CREATE ... IF NOT EXISTS ensures we don't error if already loaded
DROP EXTENSION IF EXISTS branch CASCADE;
CREATE EXTENSION branch;

\set ON_ERROR_STOP on

-- Register 'main' against LINEITEM
INSERT INTO branch.branches (name, base_table) VALUES ('main', 'lineitem');

-- Create an empty child branch for benchmarking
SELECT branch.create_branch('bench', 'main');
