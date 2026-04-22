# Installation

## Prerequisites

- PostgreSQL 17 with development headers
- C compiler (clang or gcc)
- `pg_config` available on your PATH
- make (GNU Make or compatible)

### macOS (Homebrew)

```bash
brew install postgresql@17
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
```

To start the PostgreSQL server:

```bash
brew services start postgresql@17
```

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install postgresql-17 postgresql-server-dev-17
```

The server starts automatically after installation.

## Verify pg_config

```bash
pg_config --version
```

This should print `PostgreSQL 17.x`. If not, ensure the PostgreSQL bin directory is on your PATH.

## Build and Install

From the project root directory:

```bash
make
make install
```

This compiles `src/branch.c` into a shared library using the PGXS build system and copies it along with the SQL definition files into the PostgreSQL extension directory. On Linux, `make install` may require `sudo`.

## Load the Extension

Connect to your database and run:

```sql
CREATE EXTENSION branch;
```

## Verify

```sql
SELECT branch.current_branch();
```

This should return `main`.

## Uninstall

```sql
DROP EXTENSION branch CASCADE;
```

```bash
make uninstall
```
