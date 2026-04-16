-- Install the branch extension and set up the benchmark branch.
--
-- Design:
--   - Register 'main' on LINEITEM (the largest table at ~6M rows SF=1).
--   - 'bench' is an empty child branch — a VIEW overlaying an empty delta
--     table on public.lineitem. With no deltas, the view resolves to the
--     base table, so we're measuring the pure overhead of the branch layer
--     (view resolution + UNION ALL with empty delta), not delta replay cost.

\set ON_ERROR_STOP on

DROP EXTENSION IF EXISTS branch CASCADE;
CREATE EXTENSION branch;

-- Register 'main' against LINEITEM
INSERT INTO branch.branches (name, base_table) VALUES ('main', 'lineitem');

-- Create an empty child branch for benchmarking
SELECT branch.create_branch('bench', 'main');
