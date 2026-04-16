-- Branch Extension Demo
-- Run with: psql postgres -f demo.sql
--
-- Architecture notes:
--   * Each branch has its own schema (branch_work_<name>) containing a
--     VIEW that overlays the branch's delta table onto the parent's data.
--   * switch_branch sets search_path so plain SQL like "SELECT * FROM users"
--     resolves to the branch's overlay view.
--   * INSTEAD OF triggers on the view intercept writes and append them to
--     the branch's delta table (copy-on-write).
--   * No data is copied at branch creation time — storage is proportional
--     only to the changes made on the branch.

-- Start fresh
DROP EXTENSION IF EXISTS branch CASCADE;
DROP SCHEMA IF EXISTS branch CASCADE;
DROP SCHEMA IF EXISTS branch_work_experiment1 CASCADE;
DROP SCHEMA IF EXISTS branch_work_experiment2 CASCADE;
DROP TABLE IF EXISTS users;
CREATE EXTENSION branch;

-- Create a base table with some data
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');

-- Register it as the main branch
INSERT INTO branch.branches (name, parent_id, base_table, delta_table)
VALUES ('main', NULL, 'users', NULL);

-- Check current branch
SELECT branch.current_branch();

-- See the base data
SELECT * FROM users;

-- Create an experimental branch (near-instant, no data copied)
SELECT branch.create_branch('experiment1', 'main');

-- Switch to it (sets search_path to branch_work_experiment1, public)
SELECT branch.switch_branch('experiment1');
SELECT branch.current_branch();

-- Plain SQL writes hit the view. INSTEAD OF triggers capture them as deltas.
INSERT INTO users (id, name) VALUES (4, 'Diana');
DELETE FROM users WHERE id = 2;
UPDATE users SET name = 'Alicia' WHERE id = 1;

-- The base table in public is unchanged
SELECT * FROM public.users;

-- The delta log captures branch changes
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

-- "SELECT * FROM users" resolves to the overlay view via search_path
SELECT * FROM users ORDER BY id;

-- Bulk operations work too — same direct SQL
UPDATE users SET name = upper(name);
SELECT * FROM users ORDER BY id;

-- Create experiment2 from experiment1 — sees experiment1's current state
SELECT branch.create_branch('experiment2', 'experiment1');
SELECT branch.switch_branch('experiment2');

-- experiment2 starts with experiment1's current state (via nested views)
SELECT * FROM users ORDER BY id;

-- Make further changes on experiment2
INSERT INTO users (id, name) VALUES (5, 'Eve');
UPDATE users SET name = 'diana' WHERE id = 4;
SELECT * FROM users ORDER BY id;

-- Switch back to experiment1 — unaffected by experiment2
SELECT branch.switch_branch('experiment1');
SELECT * FROM users ORDER BY id;

-- List all branches showing the parent chain
SELECT name, parent_id, base_table, delta_table FROM branch.branches;

-- Apply experiment1: replay its latest deltas into the base table
SELECT branch.apply_branch('experiment1');
SELECT * FROM public.users ORDER BY id;

-- Rollback experiment2's changes (truncates delta — view reflects parent again)
SELECT branch.switch_branch('experiment2');
SELECT branch.rollback_branch('experiment2');
SELECT * FROM users ORDER BY id;

-- Add more deltas on experiment1, then rollback
SELECT branch.switch_branch('experiment1');
INSERT INTO users (id, name) VALUES (5, 'Eve');
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT branch.rollback_branch('experiment1');

-- Deltas discarded, view reflects parent (base table) again
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;
SELECT * FROM users ORDER BY id;

-- Switch back to main
SELECT branch.switch_branch('main');
SELECT branch.current_branch();
