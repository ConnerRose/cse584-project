\set ON_ERROR_STOP on

-- Write benchmark for lineitem.
-- Parameterized variables:
--   mode      = mvcc | copy | branch
--   delta_pct = 1 | 10
--
-- Example:
--   psql postgres -v mode=branch -v delta_pct=1 -f bench_write.sql
--
-- Notes:
--   - mode=mvcc writes directly to public.lineitem (this is intentional:
--     it demonstrates the "not isolated branch" limitation).
--   - mode=copy and mode=branch write to their own lineitem copy.

\if :{?mode}
\else
\set mode branch
\endif

\if :{?delta_pct}
\else
\set delta_pct 1
\endif

DROP TABLE IF EXISTS bench_keys;
CREATE TEMP TABLE bench_keys AS
SELECT l_orderkey, l_linenumber
FROM public.lineitem
WHERE l_orderkey % 100 < :delta_pct;

\echo 'mode=' :mode ', delta_pct=' :delta_pct
SELECT count(*) AS key_count FROM bench_keys;

-- Resolve mode into psql boolean variables so \if can branch without
-- a DO block (psql does not interpolate :variables inside $$ ... $$).
SELECT :'mode' = 'branch' AS do_branch,
       :'mode' = 'copy'   AS do_copy,
       :'mode' = 'mvcc'   AS do_mvcc
\gset

\if :do_branch
SELECT branch.switch_branch('bench');
SELECT branch.rollback_branch('bench');
\elif :do_copy
SET search_path = copy_baseline, public;
TRUNCATE copy_baseline.lineitem;
INSERT INTO copy_baseline.lineitem SELECT * FROM public.lineitem;
ANALYZE copy_baseline.lineitem;
\elif :do_mvcc
SELECT branch.switch_branch('main');
\else
\warn 'Unsupported mode. Use mvcc|copy|branch.'
\quit 1
\endif

\timing on

-- 60% UPDATE-like workload: bump discount/price on sampled keys.
UPDATE lineitem l
SET l_discount = LEAST(l_discount + 0.01, 0.15),
    l_extendedprice = l_extendedprice * 1.01
FROM bench_keys k
WHERE l.l_orderkey = k.l_orderkey
  AND l.l_linenumber = k.l_linenumber;

-- 20% DELETE workload: remove a deterministic subset.
DELETE FROM lineitem l
USING bench_keys k
WHERE l.l_orderkey = k.l_orderkey
  AND l.l_linenumber = k.l_linenumber
  AND (l.l_orderkey % 5 = 0);

-- 20% INSERT workload: add deterministic synthetic rows.
INSERT INTO lineitem (
    l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
    l_comment
)
SELECT
    (SELECT MAX(l_orderkey) FROM lineitem) + row_number() OVER (),
    1, 1, 1, 1,
    10.00, 0.00, 0.00, 'N', 'O',
    DATE '1998-01-01', DATE '1998-01-02', DATE '1998-01-03',
    'NONE', 'BENCH',
    'bench-write-synth-row'
FROM bench_keys
WHERE (l_orderkey % 5 = 0);

\timing off

SELECT
    :'mode'::text AS mode,
    :delta_pct::int AS delta_pct,
    count(*) AS lineitem_rows_after
FROM lineitem;
