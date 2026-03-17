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

-- Simulate branch writes via the delta table
INSERT INTO branch.branch_delta_experiment1 (_op, id, name) VALUES ('I', 4, 'Diana');
INSERT INTO branch.branch_delta_experiment1 (_op, id, name) VALUES ('D', 2, 'Bob');
INSERT INTO branch.branch_delta_experiment1 (_op, id, name) VALUES ('U', 1, 'Alicia');

-- The base table is unchanged
SELECT * FROM users;

-- But the delta log captures branch changes
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

-- Preview the branch: reconstructed view without modifying the base table
SELECT * FROM branch.preview() AS t(id INTEGER, name TEXT);

-- List all branches
SELECT name, parent_id, base_table, delta_table FROM branch.branches;

-- Apply the branch: replay deltas into the base table
SELECT branch.apply_branch('experiment1');

-- Base table now reflects the branch changes
SELECT * FROM users ORDER BY id;

-- Delta table is cleared after apply
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;

-- Add more deltas, then rollback to discard them
INSERT INTO branch.branch_delta_experiment1 (_op, id, name) VALUES ('I', 5, 'Eve');
SELECT _seq, _op, id, name FROM branch.branch_delta_experiment1 ORDER BY _seq;

SELECT branch.rollback_branch('experiment1');

-- Deltas discarded, base table unchanged
SELECT count(*) AS remaining_deltas FROM branch.branch_delta_experiment1;
SELECT * FROM users ORDER BY id;

-- Switch back to main
SELECT branch.switch_branch('main');
SELECT branch.current_branch();
