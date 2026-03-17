-- Branch Extension Demo
-- Run with: psql postgres -f demo.sql

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

-- Switch to it
SELECT branch.switch_branch('experiment1');
SELECT branch.current_branch();

-- Write to the branch using branch.run() with standard SQL
SELECT branch.run('INSERT INTO users (id, name) VALUES (4, ''Diana'')');
SELECT branch.run('DELETE FROM users WHERE id = 2');
SELECT branch.run('UPDATE users SET name = ''Alicia'' WHERE id = 1');

-- The base table is unchanged
SELECT * FROM users;

-- The delta log captures branch changes
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

-- Preview the branch: reconstructed view without modifying the base table
SELECT * FROM branch.preview() AS t(id INTEGER, name TEXT);

-- Bulk operations work too
SELECT branch.run('UPDATE users SET name = upper(name)');

-- Preview uses only the latest delta per primary key
SELECT * FROM branch.preview() AS t(id INTEGER, name TEXT);

-- List all branches
SELECT name, parent_id, base_table, delta_table FROM branch.branches;

-- Apply the branch: replay latest deltas into the base table
SELECT branch.apply_branch('experiment1');

-- Base table now reflects the branch changes
SELECT * FROM users ORDER BY id;

-- Delta table is cleared after apply
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;

-- Add more deltas, then rollback to discard them
SELECT branch.run('INSERT INTO users (id, name) VALUES (5, ''Eve'')');
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT branch.rollback_branch('experiment1');

-- Deltas discarded, base table unchanged
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;
SELECT * FROM users ORDER BY id;

-- Switch back to main
SELECT branch.switch_branch('main');
SELECT branch.current_branch();
