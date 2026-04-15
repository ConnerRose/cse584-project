# Branch: Git-like Branching for PostgreSQL

A PostgreSQL extension that adds branch-aware logical redo logging to relational tables. Each branch is a **view** over the base table plus an append-only delta log (O(1) branch creation; storage grows with changes). Optional `materialize_branch()` upgrades a branch to a physical copy for maximum read throughput.

## Usage

### Setup

After installing the extension (see [INSTALL.md](INSTALL.md)), create a base table and register it as the `main` branch:

```sql
CREATE EXTENSION branch;

CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO branch.branches (name, parent_id, base_table, delta_table)
VALUES ('main', NULL, 'users', NULL);
```

### Creating a Branch

```sql
SELECT branch.create_branch('experiment1', 'main');
```

This creates a view in `branch_work_experiment1` and a delta table `branch.branch_delta_experiment1`. Writes on the branch are logged to the delta; the view reconstructs branch state without copying the base table up front.

### Switching Branches

```sql
SELECT branch.switch_branch('experiment1');
```

Sets the active branch for the current session.

### Checking the Current Branch

```sql
SELECT branch.current_branch();
```

### Listing All Branches

```sql
SELECT name, parent_id, base_table, delta_table, materialized, created_at
FROM branch.branches;
```

## Architecture

### View and delta tables

Each non-`main` branch has a view named like the base table in `branch_work_<name>` and an append-only delta table with the same schema as the base table plus two metadata columns:

| Column | Description |
|--------|-------------|
| `_op`  | Operation type: `I` (insert), `D` (delete), `U` (update) |
| `_seq` | Auto-incrementing sequence number for ordering operations |

### Source Code

| File | Description |
|------|-------------|
| `src/branch.c` | C extension implementing `branch_create`, `branch_switch`, and `branch_current` via SPI and GUC |
| `branch--0.2.sql` | SQL definitions: schema, metadata table, and function declarations |
| `branch.control` | Extension metadata for PostgreSQL |
| `Makefile` | PGXS-based build system |
