-- Branch Extension Demo
-- Run with: psql postgres -f demo.sql
--
-- Architecture notes:
--   * Each branch has its own schema (branch_work_<name>) containing a
--     materialized working copy of the base table.
--   * switch_branch sets search_path so plain SQL like "SELECT * FROM users"
--     resolves to the active branch's working copy.
--   * An AFTER ROW trigger on the working copy appends every I/U/D to the
--     branch's delta table for history / apply / audit.
--   * Child branches get an eager snapshot of the parent at creation time;
--     they do NOT see subsequent parent writes.

-- Start fresh
DROP EXTENSION IF EXISTS branch CASCADE;
DROP SCHEMA IF EXISTS branch CASCADE;
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

-- Create an experimental branch
SELECT branch.create_branch('experiment1', 'main');

-- Switch to it (sets search_path to branch_work_experiment1, public)
SELECT branch.switch_branch('experiment1');
SELECT branch.current_branch();

-- Plain SQL writes hit the working copy. The trigger captures them as deltas.
INSERT INTO users (id, name) VALUES (4, 'Diana');
DELETE FROM users WHERE id = 2;
UPDATE users SET name = 'Alicia' WHERE id = 1;

-- The base table in public is unchanged
SELECT * FROM public.users;

-- The delta log captures branch changes
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

-- "SELECT * FROM users" resolves to the working copy via search_path
SELECT * FROM users ORDER BY id;

-- Bulk operations work too — same direct SQL
UPDATE users SET name = upper(name);
SELECT * FROM users ORDER BY id;

-- Create experiment2 from experiment1 — eager snapshot at this moment
SELECT branch.create_branch('experiment2', 'experiment1');
SELECT branch.switch_branch('experiment2');

-- experiment2 starts with a copy of experiment1's current state
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

-- Rollback experiment2's changes (resets working copy to experiment1's current state)
SELECT branch.switch_branch('experiment2');
SELECT branch.rollback_branch('experiment2');
SELECT * FROM users ORDER BY id;

-- Add more deltas on experiment1, then rollback
SELECT branch.switch_branch('experiment1');
INSERT INTO users (id, name) VALUES (5, 'Eve');
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT branch.rollback_branch('experiment1'::text);

-- Deltas discarded, base table unchanged, working copy reset to main's state
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;
SELECT * FROM users ORDER BY id;

-- Switch back to main
SELECT branch.switch_branch('main');
SELECT branch.current_branch();
