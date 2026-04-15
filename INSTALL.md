# Installation

## Prerequisites

- PostgreSQL 17 with development headers
- C compiler (clang or gcc)
- `pg_config` available on your PATH

### macOS (Homebrew)

```bash
brew install postgresql@17
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
```

### Ubuntu/Debian

```bash
sudo apt install postgresql-17 postgresql-server-dev-17
```

## Build and Install

```bash
make
make install
```

## Load the Extension

Connect to your database and run:

```sql
CREATE EXTENSION branch;
```

Extension version is **0.2** (view-based branches). If you previously installed **0.1** with eager copies, run `DROP EXTENSION branch CASCADE;` and `CREATE EXTENSION branch;` again after `make install`, then re-run your table setup scripts.

## Uninstall

```sql
DROP EXTENSION branch CASCADE;
DROP SCHEMA IF EXISTS branch CASCADE;
```

```bash
make uninstall
```
