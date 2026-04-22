# Branch: Git-like Branching for PostgreSQL

A PostgreSQL extension that adds branch-aware logical redo logging to relational tables. Create, switch, and query independent branches of your data without duplicating entire tables.

## Usage

After installing the extension (see [INSTALL.md](INSTALL.md)), create a base table and register it as the `main` branch:

```sql
CREATE EXTENSION branch;

CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');

INSERT INTO branch.branches (name, parent_id, base_table, delta_table)
VALUES ('main', NULL, 'users', NULL);
```

### Creating and Switching Branches

```sql
SELECT branch.create_branch('experiment1', 'main');
SELECT branch.switch_branch('experiment1');
```

After switching, ordinary SQL operates on the branch. Writes are captured in the branch's delta table; the base table is not modified.

```sql
INSERT INTO users (id, name) VALUES (4, 'Diana');
DELETE FROM users WHERE id = 2;
UPDATE users SET name = 'Alicia' WHERE id = 1;

-- Base table is unchanged:
SELECT * FROM public.users;

-- Branch view reflects changes:
SELECT * FROM users;
```

### Applying and Rolling Back

```sql
-- Merge branch deltas into the base table:
SELECT branch.apply_branch('experiment1');

-- Discard all branch changes:
SELECT branch.rollback_branch('experiment1');
```

### Other Operations

```sql
-- Check which branch is active:
SELECT branch.current_branch();

-- List all branches:
SELECT name, parent_id, base_table, delta_table FROM branch.branches;
```

### Demo Script

A complete walkthrough is provided in `demo.sql`:

```bash
psql postgres -f demo.sql
```

This demonstrates branch creation, writes, isolation between branches, child branches, apply, and rollback.

## Source Code

### `src/branch.c`

The core C extension, approximately 610 lines. All functions use the PostgreSQL SPI (Server Programming Interface) to execute dynamically constructed SQL. Key functions:

- **`branch_create`** (line 55): Creates a new branch from an existing one. Sets up a work schema, an empty delta table, a SQL view (LEFT JOIN anti-join overlay), INSTEAD OF triggers for write interception, and registers the branch in metadata. No data is copied.

- **`branch_switch`** (line 285): Switches the active branch by setting the `branch.active_branch` GUC and updating `search_path` so unqualified table names resolve to the branch's view.

- **`branch_apply`** (line 323): Replays the branch's delta log into the base table. Materializes the latest delta per primary key into a temp table, then applies inserts, deletes, and updates to the base table. Truncates the delta table afterward.

- **`branch_rollback`** (line 478): Discards all branch changes by truncating the delta table. The overlay view instantly reflects the parent's state again.

- **`branch_preview`** (line 523): Returns the current branch state as a set of records by querying through the overlay view (or the base table for main).

- **`branch_current`** (line 605): Returns the name of the currently active branch from the GUC variable.

### `branch--0.1.sql`

SQL definitions loaded by `CREATE EXTENSION`. Creates the `branch` schema, the `branch.branches` metadata table, and declares all six SQL-callable functions (`create_branch`, `switch_branch`, `apply_branch`, `rollback_branch`, `preview`, `current_branch`).

### `branch.control`

Extension metadata (name, version, schema) used by PostgreSQL's extension system.

### `Makefile`

PGXS-based build file. Compiles `src/branch.c` and installs the shared library and SQL files.

### `demo.sql`

End-to-end demonstration script covering all supported operations.

### `bench/`

Benchmark infrastructure:

- `bench/tpch/`: TPC-H benchmark suite (setup, schema, data loading, query runner, graph generation)
- `bench/delta_scaling/`: Measures query latency as a function of delta table size
