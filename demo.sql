-- Branch Extension Demo
-- Run with: psql postgres -f demo.sql
--
-- Architecture (v0.2):
--   * Each branch has schema branch_work_<name> with a VIEW on the base table
--     joined with an append-only delta log in branch.branch_delta_<name>.
--   * switch_branch sets search_path so "SELECT FROM users" reads the view.
--   * INSTEAD OF triggers on the view log INSERT/UPDATE/DELETE into the delta.
--   * Rolling back clears the delta; the view then matches the parent again.
--   * Optional: branch.materialize_branch(name) replaces the view with a
--     physical table + AFTER ROW triggers (same read cost as a full copy).

DROP EXTENSION IF EXISTS branch CASCADE;
DROP SCHEMA IF EXISTS branch CASCADE;
DROP TABLE IF EXISTS users;
CREATE EXTENSION branch;

CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');

INSERT INTO branch.branches (name, parent_id, base_table, delta_table)
VALUES ('main', NULL, 'users', NULL);

SELECT branch.current_branch();

SELECT * FROM users;

SELECT branch.create_branch('experiment1', 'main');
SELECT branch.switch_branch('experiment1');
SELECT branch.current_branch();

INSERT INTO users (id, name) VALUES (4, 'Diana');
DELETE FROM users WHERE id = 2;
UPDATE users SET name = 'Alicia' WHERE id = 1;

SELECT * FROM public.users;

SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT * FROM users ORDER BY id;

UPDATE users SET name = upper(name);
SELECT * FROM users ORDER BY id;

SELECT branch.create_branch('experiment2', 'experiment1');
SELECT branch.switch_branch('experiment2');

SELECT * FROM users ORDER BY id;

INSERT INTO users (id, name) VALUES (5, 'Eve');
UPDATE users SET name = 'diana' WHERE id = 4;
SELECT * FROM users ORDER BY id;

SELECT branch.switch_branch('experiment1');
SELECT * FROM users ORDER BY id;

SELECT name, parent_id, base_table, delta_table, materialized FROM branch.branches;

SELECT branch.apply_branch('experiment1');
SELECT * FROM public.users ORDER BY id;

SELECT branch.switch_branch('experiment2');
SELECT branch.rollback_branch('experiment2');
SELECT * FROM users ORDER BY id;

SELECT branch.switch_branch('experiment1');
INSERT INTO users (id, name) VALUES (5, 'Eve');
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT branch.rollback_branch('experiment1');

SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;
SELECT * FROM users ORDER BY id;

SELECT branch.materialize_branch('experiment1');
SELECT materialized FROM branch.branches WHERE name = 'experiment1';

SELECT branch.switch_branch('main');
SELECT branch.current_branch();
