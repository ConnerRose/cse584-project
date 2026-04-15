\set ON_ERROR_STOP on

-- MVCC wall tests for database-versioning use cases.
-- Run after schema/load/init_branch:
--   psql postgres -f wall_tests.sql

\echo ''
\echo '=== Wall 1: No isolated experimental writes with MVCC snapshots ==='
\echo 'MVCC attempt: write rows inside REPEATABLE READ, then ROLLBACK.'
\echo ''

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) AS before_count_mvcc_txn FROM public.lineitem;

INSERT INTO public.lineitem (
    l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
    l_comment
)
VALUES (
    990000000, 1, 1, 1, 1,
    10.00, 0.00, 0.00, 'N', 'O',
    DATE '1998-01-01', DATE '1998-01-02', DATE '1998-01-03', 'NONE', 'BENCH',
    'mvcc-wall1-temp-row'
);

SELECT count(*) AS after_insert_mvcc_txn FROM public.lineitem;
ROLLBACK;

SELECT count(*) AS after_rollback_public FROM public.lineitem;
\echo 'Expected: count returns to original (work is lost if aborted).'

\echo ''
\echo 'Branch equivalent: write to branch working copy persists independently.'
SELECT branch.switch_branch('bench');
INSERT INTO lineitem (
    l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
    l_comment
)
VALUES (
    980000000, 1, 1, 1, 1,
    10.00, 0.00, 0.00, 'N', 'O',
    DATE '1998-01-01', DATE '1998-01-02', DATE '1998-01-03', 'NONE', 'BENCH',
    'branch-wall1-persist-row'
);
SELECT count(*) AS branch_count_after_insert FROM lineitem;
SELECT branch.switch_branch('main');
SELECT count(*) AS public_count_after_branch_insert FROM public.lineitem;
\echo 'Expected: branch count increases, public count unchanged.'

\echo ''
\echo '=== Wall 2: MVCC snapshots are ephemeral ==='
\echo 'Open transaction snapshots disappear after COMMIT/ROLLBACK.'
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT txid_current() AS mvcc_txid_snapshot;
SELECT now() AS mvcc_snapshot_time;
ROLLBACK;
\echo 'Expected: no way to switch back to this exact snapshot by name later.'

\echo ''
\echo 'Branch equivalent: persistent branch metadata + switchable by name.'
SELECT branch.current_branch() AS current_before;
SELECT branch.switch_branch('bench');
SELECT branch.current_branch() AS current_after;
SELECT branch.switch_branch('main');

\echo ''
\echo '=== Wall 3: Holding snapshot causes dead-tuple accumulation pressure ==='
\echo 'This script prints storage/dead tuple stats only.'
SELECT
  relname,
  n_live_tup,
  n_dead_tup
FROM pg_stat_user_tables
WHERE schemaname = 'public' AND relname = 'lineitem';

SELECT
  pg_size_pretty(pg_total_relation_size('public.lineitem')) AS lineitem_total_size;
\echo 'Run concurrent UPDATE workload while holding REPEATABLE READ in another session for full demonstration.'

\echo ''
\echo '=== Wall 4: No native multi-branch writable isolation in MVCC ==='
\echo 'MVCC can create concurrent snapshots, but not two persistent named writable branches.'
\echo 'Branch extension demonstration:'

-- Cleanup from prior runs so this script is idempotent.
DROP SCHEMA IF EXISTS branch_work_raise CASCADE;
DROP SCHEMA IF EXISTS branch_work_cut CASCADE;
DROP TABLE IF EXISTS branch.branch_delta_raise;
DROP TABLE IF EXISTS branch.branch_delta_cut;
DROP FUNCTION IF EXISTS branch._capture_raise() CASCADE;
DROP FUNCTION IF EXISTS branch._capture_cut() CASCADE;
DELETE FROM branch.branches WHERE name IN ('raise', 'cut');

SELECT branch.create_branch('raise', 'main');
SELECT branch.create_branch('cut', 'main');

SELECT branch.switch_branch('raise');
UPDATE lineitem
SET l_extendedprice = l_extendedprice * 1.10
WHERE l_returnflag = 'R';

SELECT branch.switch_branch('cut');
UPDATE lineitem
SET l_extendedprice = l_extendedprice * 0.80
WHERE l_returnflag = 'R';

SELECT branch.switch_branch('raise');
SELECT AVG(l_extendedprice) AS avg_price_raise
FROM lineitem
WHERE l_returnflag = 'R';

SELECT branch.switch_branch('cut');
SELECT AVG(l_extendedprice) AS avg_price_cut
FROM lineitem
WHERE l_returnflag = 'R';

SELECT branch.switch_branch('main');
\echo 'Expected: raise and cut branches diverge independently.'

\echo ''
\echo '=== Wall 5: Lightweight rollback after committed changes ==='
\echo 'Branch rollback is one command.'
SELECT branch.switch_branch('bench');
UPDATE lineitem
SET l_discount = l_discount + 0.01
WHERE l_shipmode = 'AIR';
SELECT branch.rollback_branch('bench');
SELECT branch.switch_branch('main');
\echo 'Expected: bench reset from parent with deltas cleared.'
