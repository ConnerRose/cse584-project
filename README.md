# Branch: Git-like Branching for PostgreSQL

A PostgreSQL extension that adds branch-aware logical redo logging to relational tables. Create, switch, and query independent branches of your data without duplicating entire tables.

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

This creates a delta table (`branch.branch_delta_experiment1`) that records changes made on the branch without copying the base data.

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
SELECT name, parent_id, base_table, delta_table, created_at
FROM branch.branches;
```

## Architecture

### Delta Tables

Each branch has an associated delta table with the same schema as the base table plus two metadata columns:

| Column | Description |
|--------|-------------|
| `_op`  | Operation type: `I` (insert), `D` (delete), `U` (update) |
| `_seq` | Auto-incrementing sequence number for ordering operations |

### Source Code

| File | Description |
|------|-------------|
| `src/branch.c` | C extension implementing `branch_create`, `branch_switch`, and `branch_current` via SPI and GUC |
| `branch--0.1.sql` | SQL definitions: schema, metadata table, and function declarations |
| `branch.control` | Extension metadata for PostgreSQL |
| `Makefile` | PGXS-based build system |
