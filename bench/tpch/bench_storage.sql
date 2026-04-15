\set ON_ERROR_STOP on

-- Storage benchmark helper.
-- Branch "work" may be a VIEW (no heap); we report 0 bytes for the view object
-- and measure delta + metadata separately.
--
-- Usage:
--   psql postgres -f bench_storage.sql

\echo '=== relation sizes (bytes) ==='

WITH rels AS (
    SELECT 'public_lineitem'::text AS label, 'public.lineitem'::regclass AS oid
    UNION ALL
    SELECT 'copy_lineitem', 'copy_baseline.lineitem'::regclass
    UNION ALL
    SELECT 'branch_work_lineitem', 'branch_work_bench.lineitem'::regclass
    UNION ALL
    SELECT 'branch_delta_lineitem', 'branch.branch_delta_bench'::regclass
)
SELECT
    r.label,
    CASE
        WHEN c.relkind = 'v' THEN 0::bigint
        ELSE pg_total_relation_size(r.oid)
    END AS total_bytes,
    CASE
        WHEN c.relkind = 'v' THEN 0::bigint
        ELSE pg_relation_size(r.oid)
    END AS heap_bytes,
    CASE
        WHEN c.relkind = 'v' THEN 0::bigint
        ELSE pg_indexes_size(r.oid)
    END AS index_bytes
FROM rels r
JOIN pg_class c ON c.oid = r.oid::oid
ORDER BY label;

\echo ''
\echo '=== dead tuple stats ==='
SELECT
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
WHERE (schemaname, relname) IN (
    ('public', 'lineitem'),
    ('copy_baseline', 'lineitem'),
    ('branch_work_bench', 'lineitem'),
    ('branch', 'branch_delta_bench')
)
ORDER BY schemaname, relname;

\echo ''
\echo '=== summarized approach overhead ratios vs public.lineitem ==='
WITH work_sz AS (
    SELECT
        CASE
            WHEN c.relkind = 'v' THEN 0::numeric
            ELSE pg_total_relation_size(c.oid)::numeric
        END AS b
    FROM pg_class c
    WHERE c.oid = 'branch_work_bench.lineitem'::regclass
),
sizes AS (
    SELECT
      pg_total_relation_size('public.lineitem')::numeric AS base_bytes,
      COALESCE(pg_total_relation_size('copy_baseline.lineitem'), 0)::numeric AS copy_bytes,
      (
        COALESCE((SELECT b FROM work_sz), 0) +
        COALESCE(pg_total_relation_size('branch.branch_delta_bench'), 0) +
        COALESCE(pg_total_relation_size('branch.branches'), 0)
      )::numeric AS branch_bytes
)
SELECT
    base_bytes::bigint AS base_bytes,
    copy_bytes::bigint AS copy_bytes,
    branch_bytes::bigint AS branch_bytes,
    ROUND(copy_bytes / NULLIF(base_bytes, 0), 4) AS copy_over_base_ratio,
    ROUND(branch_bytes / NULLIF(base_bytes, 0), 4) AS branch_over_base_ratio
FROM sizes;
