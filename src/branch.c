// clang-format off
#include "postgres.h"
// clang-format on
#include "access/htup_details.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include <ctype.h>
#include <string.h>

PG_MODULE_MAGIC;

/* GUC variable: the currently active branch name */
static char* active_branch = NULL;

void _PG_init(void) {
  DefineCustomStringVariable("branch.active_branch",
                             "The currently active branch name.", NULL,
                             &active_branch, "main", /* default value */
                             PGC_USERSET, /* any user can set it per-session */
                             0,           /* flags */
                             NULL,        /* check_hook */
                             NULL,        /* assign_hook */
                             NULL         /* show_hook */
  );
}

/* Return the work schema name for a branch: "branch_work_<name>". */
static char* work_schema_name(const char* branch_name) {
  return psprintf("branch_work_%s", branch_name);
}

/* Return the trigger function name for a branch: "_capture_<name>". */
static char* trigger_fn_name(const char* branch_name) {
  return psprintf("_capture_%s", branch_name);
}

/*
 * Look up a branch's base_table by name. Returns a palloc'd string.
 * Must be called within an active SPI connection.
 */
static char* lookup_base_table(const char* branch_name) {
  StringInfoData buf;
  int ret;
  char* result;

  initStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT base_table FROM branch.branches WHERE name = %s",
                   quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", branch_name)));
  }

  result =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  pfree(buf.data);
  return result;
}

/*
 * Build "d.col = b.col AND ..." from a comma-separated pk column list.
 * Both sides use the same bare identifiers (already safe from the catalog).
 */
static char* build_pk_join_two_aliases(const char* pk_cols_raw,
                                       const char* alias_a,
                                       const char* alias_b) {
  StringInfoData out;
  char* copy;
  char* saveptr = NULL;
  char* token;
  char* end;
  bool first = true;

  initStringInfo(&out);
  copy = pstrdup(pk_cols_raw);

  for (token = strtok_r(copy, ",", &saveptr); token != NULL;
       token = strtok_r(NULL, ",", &saveptr)) {
    while (*token == ' ') {
      token++;
    }
    end = token + strlen(token);
    while (end > token && isspace((unsigned char)end[-1])) {
      end--;
    }
    *end = '\0';

    if (!first) {
      appendStringInfoString(&out, " AND ");
    }
    first = false;
    appendStringInfo(&out, "%s.%s = %s.%s", alias_a, token, alias_b, token);
  }

  pfree(copy);
  return out.data;
}

static char* lookup_primary_key_columns(const char* base_table) {
  StringInfoData buf;
  int ret;
  char* pk;

  initStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(quote_ident(a.attname), ', ' "
      "  ORDER BY array_position(i.indkey::int[], a.attnum::int)) "
      "FROM pg_index i "
      "JOIN pg_attribute a ON a.attrelid = i.indrelid "
      "AND a.attnum = ANY(i.indkey) "
      "WHERE i.indrelid = 'public.%s'::regclass AND i.indisprimary",
      base_table);

  ret = SPI_execute(buf.data, true, 1);
  pfree(buf.data);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not determine primary key for \"%s\"", base_table)));
  }

  pk = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
  if (pk == NULL) {
    ereport(ERROR, (errmsg("table \"%s\" has no primary key", base_table)));
  }
  return pstrdup(pk);
}

/* ----------------------------------------------------------------
 * branch_create(new_branch TEXT, from_branch TEXT)
 *
 * Creates a view-based branch:
 *   1) work schema + delta table
 *   2) VIEW = parent rows minus (D,U) in latest delta, union I/U rows
 *   3) INSTEAD OF triggers log writes to the delta table
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_create);

Datum branch_create(PG_FUNCTION_ARGS) {
  text* new_branch_t = PG_GETARG_TEXT_PP(0);
  text* from_branch_t = PG_GETARG_TEXT_PP(1);
  char* new_branch = text_to_cstring(new_branch_t);
  char* from_branch = text_to_cstring(from_branch_t);

  int ret;
  StringInfoData buf;
  int parent_id;
  bool isnull;
  char* base_table;
  char* parent_schema;
  char* work_schema;
  char* delta_table;
  char* fn_name;
  char* columns;
  char* new_columns;
  char* old_columns;
  char* pk_cols;
  char* pk_join_db;

  SPI_connect();

  /* Look up the parent branch */
  initStringInfo(&buf);
  appendStringInfo(
      &buf, "SELECT branch_id, base_table FROM branch.branches WHERE name = %s",
      quote_literal_cstr(from_branch));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", from_branch)));
  }

  parent_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                          SPI_tuptable->tupdesc, 1, &isnull));
  base_table =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));

  /* Parent's source data lives in public if main, otherwise its work schema */
  if (strcmp(from_branch, "main") == 0) {
    parent_schema = pstrdup("public");
  } else {
    parent_schema = work_schema_name(from_branch);
  }

  work_schema = work_schema_name(new_branch);
  delta_table = psprintf("branch_delta_%s", new_branch);
  fn_name = trigger_fn_name(new_branch);

  /* 1. Create the work schema */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "CREATE SCHEMA %s", quote_identifier(work_schema));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to create work schema \"%s\"", work_schema)));
  }

  /* 2. Delta table (append-only audit log) */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TABLE branch.%s ("
                   "  _op CHAR(1) NOT NULL,"
                   "  _seq BIGSERIAL,"
                   "  LIKE public.%s INCLUDING DEFAULTS"
                   ")",
                   quote_identifier(delta_table), quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create delta table for branch \"%s\"",
                           new_branch)));
  }

  /* Column lists + primary key (for view and index) */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT "
      "  string_agg(quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('NEW.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('OLD.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position) "
      "FROM information_schema.columns "
      "WHERE table_name = %s AND table_schema = 'public'",
      quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not read columns for table \"%s\"", base_table)));
  }
  columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  new_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
  old_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));

  pk_cols = lookup_primary_key_columns(base_table);
  pk_join_db = build_pk_join_two_aliases(pk_cols, "d", "b");

  /* Index to speed DISTINCT ON / joins on delta */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE INDEX ON branch.%s (%s, _seq DESC)",
                   quote_identifier(delta_table), pk_cols);
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to create index on delta for branch \"%s\"",
                    new_branch)));
  }

  /* 3. VIEW: latest delta per PK; union visible rows */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE VIEW %s.%s AS "
                   "WITH latest AS ( "
                   "  SELECT DISTINCT ON (%s) "
                   "    _op, %s "
                   "  FROM branch.%s "
                   "  ORDER BY %s, _seq DESC "
                   ") "
                   "SELECT %s "
                   "FROM %s.%s b "
                   "WHERE NOT EXISTS ( "
                   "  SELECT 1 FROM latest d "
                   "  WHERE (%s) AND d._op IN ('D','U') "
                   ") "
                   "UNION ALL "
                   "SELECT %s FROM latest WHERE _op IN ('I','U')",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   pk_cols, columns, quote_identifier(delta_table), pk_cols,
                   columns, quote_identifier(parent_schema),
                   quote_identifier(base_table), pk_join_db, columns);
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create branch view for \"%s\"",
                            new_branch)));
  }

  /* 4a. INSTEAD OF trigger: log to delta; RETURN values required */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE FUNCTION branch.%s() RETURNS TRIGGER AS $fn$ "
                   "BEGIN "
                   "  IF TG_OP = 'INSERT' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('I', %s); "
                   "    RETURN NEW; "
                   "  ELSIF TG_OP = 'DELETE' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('D', %s); "
                   "    RETURN OLD; "
                   "  ELSE "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('U', %s); "
                   "    RETURN NEW; "
                   "  END IF; "
                   "END; "
                   "$fn$ LANGUAGE plpgsql",
                   quote_identifier(fn_name), quote_identifier(delta_table),
                   columns, new_columns, quote_identifier(delta_table), columns,
                   old_columns, quote_identifier(delta_table), columns,
                   new_columns);
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create trigger function for branch "
                           "\"%s\"",
                           new_branch)));
  }

  /* 4b. INSTEAD OF on the view */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TRIGGER _capture "
                   "INSTEAD OF INSERT OR UPDATE OR DELETE ON %s.%s "
                   "FOR EACH ROW EXECUTE FUNCTION branch.%s()",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   quote_identifier(fn_name));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to install capture trigger on \"%s.%s\"",
                           work_schema, base_table)));
  }

  /* 5. Register branch (view-based, not materialized) */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "INSERT INTO branch.branches (name, parent_id, "
                   "base_table, delta_table, materialized) "
                   "VALUES (%s, %d, %s, %s, false)",
                   quote_literal_cstr(new_branch), parent_id,
                   quote_literal_cstr(base_table),
                   quote_literal_cstr(delta_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR, (errmsg("failed to register branch \"%s\"", new_branch)));
  }

  pfree(pk_join_db);
  pfree(pk_cols);

  SPI_finish();

  elog(NOTICE, "branch \"%s\" created from \"%s\" (view-based)", new_branch,
       from_branch);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_switch(target_branch TEXT)
 *
 * Sets the session GUC branch.active_branch and updates search_path so
 * table references in user SQL resolve to the branch's working copy.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_switch);

Datum branch_switch(PG_FUNCTION_ARGS) {
  text* target_t = PG_GETARG_TEXT_PP(0);
  char* target = text_to_cstring(target_t);
  int ret;
  StringInfoData buf;
  char* new_search_path;

  SPI_connect();

  /* Verify the branch exists */
  initStringInfo(&buf);
  appendStringInfo(&buf, "SELECT 1 FROM branch.branches WHERE name = %s",
                   quote_literal_cstr(target));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", target)));
  }

  SPI_finish();

  /* Set the GUC and search_path */
  SetConfigOption("branch.active_branch", target, PGC_USERSET, PGC_S_SESSION);

  if (strcmp(target, "main") == 0) {
    new_search_path = "\"$user\", public";
  } else {
    char* ws = work_schema_name(target);
    new_search_path = psprintf("%s, public", quote_identifier(ws));
  }
  SetConfigOption("search_path", new_search_path, PGC_USERSET, PGC_S_SESSION);

  elog(NOTICE, "switched to branch \"%s\"", target);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_apply(branch_name TEXT)
 *
 * Replays the branch's delta log into the base table, applying the latest
 * op per primary key in _seq order, then truncates the delta table.
 * Leaves the branch's working copy intact (it is already post-apply).
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_apply);

Datum branch_apply(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;

  SPI_connect();

  /* Look up branch metadata */
  initStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT base_table, delta_table FROM branch.branches WHERE name = %s",
      quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", branch_name)));
  }

  base_table =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

  if (delta_table == NULL) {
    ereport(ERROR, (errmsg("branch \"%s\" has no delta table (is it main?)",
                           branch_name)));
  }
  delta_table = pstrdup(delta_table);

  /* Get column names (excluding delta metadata cols) */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT string_agg(quote_ident(column_name), ', ' "
                   "  ORDER BY ordinal_position) "
                   "FROM information_schema.columns "
                   "WHERE table_name = %s AND table_schema = 'public'",
                   quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not read columns for table \"%s\"", base_table)));
  }

  {
    char* columns =
        pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

    /* Get the primary key column names (composite-PK aware, ordered) */
    resetStringInfo(&buf);
    appendStringInfo(
        &buf,
        "SELECT string_agg(quote_ident(a.attname), ', ' "
        "  ORDER BY array_position(i.indkey::int[], a.attnum::int)) "
        "FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary",
        quote_literal_cstr(base_table));

    ret = SPI_execute(buf.data, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0) {
      ereport(ERROR, (errmsg("could not determine primary key for \"%s\"",
                             base_table)));
    }

    {
      char* pk_cols = pstrdup(
          SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

      if (pk_cols == NULL) {
        ereport(ERROR, (errmsg("table \"%s\" has no primary key", base_table)));
      }

      /* Materialize the latest delta per PK into a temp table */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "CREATE TEMP TABLE _branch_apply_latest AS "
                       "SELECT DISTINCT ON (%s) _op, %s FROM branch.%s "
                       "ORDER BY %s, _seq DESC",
                       pk_cols, columns, quote_identifier(delta_table),
                       pk_cols);
      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_UTILITY) {
        ereport(ERROR, (errmsg("failed to materialize latest deltas")));
      }

      /* Apply inserts */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO public.%s (%s) "
                       "SELECT %s FROM _branch_apply_latest WHERE _op = 'I'",
                       quote_identifier(base_table), columns, columns);
      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply inserts for branch \"%s\"",
                               branch_name)));
      }

      /* Apply deletes */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM public.%s WHERE (%s) IN "
                       "(SELECT %s FROM _branch_apply_latest WHERE _op = 'D')",
                       quote_identifier(base_table), pk_cols, pk_cols);
      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply deletes for branch \"%s\"",
                               branch_name)));
      }

      /* Apply updates: delete old rows then insert updated rows */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM public.%s WHERE (%s) IN "
                       "(SELECT %s FROM _branch_apply_latest WHERE _op = 'U')",
                       quote_identifier(base_table), pk_cols, pk_cols);
      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply updates (delete phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }

      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO public.%s (%s) "
                       "SELECT %s FROM _branch_apply_latest WHERE _op = 'U'",
                       quote_identifier(base_table), columns, columns);
      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply updates (insert phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }

      SPI_execute("DROP TABLE _branch_apply_latest", false, 0);
    }
  }

  /* Truncate the delta table */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "TRUNCATE branch.%s", quote_identifier(delta_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to truncate delta table for branch \"%s\"",
                           branch_name)));
  }

  SPI_finish();

  elog(NOTICE, "applied and cleared delta log for branch \"%s\"", branch_name);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_rollback(branch_name TEXT)
 *
 * View branch: TRUNCATE delta only (view then matches parent).
 * Materialized branch: truncate physical table, refill from parent,
 * then truncate delta (trigger disabled during refill).
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_rollback);

Datum branch_rollback(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* parent_name;
  char* parent_schema;
  char* work_schema;
  bool isnull;
  bool materialized;
  Datum mat_datum;

  SPI_connect();

  initStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT b.base_table, b.delta_table, b.materialized, p.name "
                   "FROM branch.branches b "
                   "JOIN branch.branches p ON p.branch_id = b.parent_id "
                   "WHERE b.name = %s",
                   quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist or has no parent",
                           branch_name)));
  }

  base_table =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
  mat_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3,
                            &isnull);
  materialized = (!isnull) && DatumGetBool(mat_datum);
  parent_name = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);

  if (delta_table == NULL) {
    ereport(ERROR, (errmsg("branch \"%s\" has no delta table (is it main?)",
                           branch_name)));
  }
  delta_table = pstrdup(delta_table);
  parent_name = pstrdup(parent_name);

  if (strcmp(parent_name, "main") == 0) {
    parent_schema = pstrdup("public");
  } else {
    parent_schema = work_schema_name(parent_name);
  }
  work_schema = work_schema_name(branch_name);

  if (!materialized) {
    resetStringInfo(&buf);
    appendStringInfo(&buf, "TRUNCATE branch.%s", quote_identifier(delta_table));
    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_UTILITY) {
      ereport(ERROR,
              (errmsg("failed to truncate delta table for branch \"%s\"",
                      branch_name)));
    }
    SPI_finish();
    elog(NOTICE, "rolled back view branch \"%s\" (delta cleared)", branch_name);
    PG_RETURN_VOID();
  }

  /* Materialized: refresh physical table from parent */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "ALTER TABLE %s.%s DISABLE TRIGGER _capture",
                   quote_identifier(work_schema), quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to disable capture trigger")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "TRUNCATE %s.%s", quote_identifier(work_schema),
                   quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to truncate working copy")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "INSERT INTO %s.%s SELECT * FROM %s.%s",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   quote_identifier(parent_schema),
                   quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR, (errmsg("failed to repopulate working copy from parent")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "ALTER TABLE %s.%s ENABLE TRIGGER _capture",
                   quote_identifier(work_schema), quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to re-enable capture trigger")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "TRUNCATE branch.%s", quote_identifier(delta_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to truncate delta table for branch \"%s\"",
                           branch_name)));
  }

  SPI_finish();

  elog(NOTICE, "rolled back materialized branch \"%s\"", branch_name);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_materialize(branch_name TEXT)
 *
 * Copies the branch view into a physical table, replaces the view,
 * switches to AFTER ROW capture + RETURN NULL, sets materialized.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_materialize);

Datum branch_materialize(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* fn_name;
  char* work_schema;
  char* mat_table;
  char* columns;
  char* new_columns;
  char* old_columns;
  bool isnull;
  Datum mat_datum;

  SPI_connect();

  initStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT base_table, delta_table, materialized FROM branch.branches "
      "WHERE name = %s",
      quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", branch_name)));
  }

  base_table =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
  mat_datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3,
                            &isnull);
  if ((!isnull) && DatumGetBool(mat_datum)) {
    ereport(ERROR,
            (errmsg("branch \"%s\" is already materialized", branch_name)));
  }

  if (delta_table == NULL) {
    ereport(ERROR, (errmsg("branch \"%s\" has no delta table", branch_name)));
  }
  delta_table = pstrdup(delta_table);
  work_schema = work_schema_name(branch_name);
  fn_name = trigger_fn_name(branch_name);
  mat_table = psprintf("_branch_mat_%s", branch_name);

  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT "
      "  string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position), "
      "  string_agg('NEW.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('OLD.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position) "
      "FROM information_schema.columns "
      "WHERE table_name = %s AND table_schema = 'public'",
      quote_literal_cstr(base_table));
  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not read columns for table \"%s\"", base_table)));
  }
  columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
  new_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
  old_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TABLE %s.%s (LIKE public.%s INCLUDING ALL)",
                   quote_identifier(work_schema), quote_identifier(mat_table),
                   quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create materialization temp table")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "INSERT INTO %s.%s SELECT * FROM %s.%s",
                   quote_identifier(work_schema), quote_identifier(mat_table),
                   quote_identifier(work_schema), quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR, (errmsg("failed to populate materialized copy")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "DROP VIEW %s.%s CASCADE",
                   quote_identifier(work_schema), quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to drop branch view")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "ALTER TABLE %s.%s RENAME TO %s",
                   quote_identifier(work_schema), quote_identifier(mat_table),
                   quote_identifier(base_table));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to rename materialized table")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE OR REPLACE FUNCTION branch.%s() RETURNS TRIGGER AS $fn$ "
                   "BEGIN "
                   "  IF TG_OP = 'INSERT' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('I', %s); "
                   "  ELSIF TG_OP = 'DELETE' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('D', %s); "
                   "  ELSE "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('U', %s); "
                   "  END IF; "
                   "  RETURN NULL; "
                   "END; "
                   "$fn$ LANGUAGE plpgsql",
                   quote_identifier(fn_name), quote_identifier(delta_table),
                   columns, new_columns, quote_identifier(delta_table), columns,
                   old_columns, quote_identifier(delta_table), columns,
                   new_columns);
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create AFTER trigger function")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TRIGGER _capture "
                   "AFTER INSERT OR UPDATE OR DELETE ON %s.%s "
                   "FOR EACH ROW EXECUTE FUNCTION branch.%s()",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   quote_identifier(fn_name));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to install AFTER capture trigger")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "UPDATE branch.branches SET materialized = true WHERE name = %s",
                   quote_literal_cstr(branch_name));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UPDATE) {
    ereport(ERROR, (errmsg("failed to mark branch materialized")));
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf, "ANALYZE %s.%s", quote_identifier(work_schema),
                   quote_identifier(base_table));
  SPI_execute(buf.data, false, 0);

  pfree(base_table);
  pfree(columns);
  pfree(new_columns);
  pfree(old_columns);
  pfree(delta_table);

  SPI_finish();
  elog(NOTICE, "materialized branch \"%s\"", branch_name);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_preview() -> SETOF RECORD
 *
 * Main: public base table. Other branches: work schema (view or table).
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_preview);

Datum branch_preview(PG_FUNCTION_ARGS) {
  FuncCallContext* funcctx;
  TupleDesc tupdesc;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;
    const char* branch_name;
    int ret;
    StringInfoData buf;
    char* base_table;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    branch_name = GetConfigOption("branch.active_branch", false, false);

    SPI_connect();
    base_table = lookup_base_table(branch_name);

    initStringInfo(&buf);
    if (strcmp(branch_name, "main") == 0) {
      appendStringInfo(&buf, "SELECT * FROM public.%s",
                       quote_identifier(base_table));
    } else {
      char* ws = work_schema_name(branch_name);
      appendStringInfo(&buf, "SELECT * FROM %s.%s", quote_identifier(ws),
                       quote_identifier(base_table));
    }

    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT) {
      ereport(ERROR, (errmsg("failed to preview branch \"%s\"", branch_name)));
    }

    funcctx->max_calls = SPI_processed;
    funcctx->user_fctx = SPI_tuptable;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(
          ERROR,
          (errmsg("branch.preview() must be called with a column "
                  "definition list, e.g.: "
                  "SELECT * FROM branch.preview() AS t(id INTEGER, name TEXT)")));
    }
    BlessTupleDesc(tupdesc);
    funcctx->tuple_desc = tupdesc;

    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  tupdesc = funcctx->tuple_desc;

  if (funcctx->call_cntr < funcctx->max_calls) {
    SPITupleTable* tuptable = (SPITupleTable*)funcctx->user_fctx;
    HeapTuple spi_tuple = tuptable->vals[funcctx->call_cntr];
    TupleDesc spi_tupdesc = tuptable->tupdesc;
    int natts = tupdesc->natts;
    Datum* values = (Datum*)palloc(natts * sizeof(Datum));
    bool* nulls = (bool*)palloc(natts * sizeof(bool));
    HeapTuple result_tuple;
    int i;

    for (i = 0; i < natts; i++) {
      values[i] = SPI_getbinval(spi_tuple, spi_tupdesc, i + 1, &nulls[i]);
    }

    result_tuple = heap_form_tuple(tupdesc, values, nulls);
    pfree(values);
    pfree(nulls);

    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(result_tuple));
  } else {
    SPI_finish();
    SRF_RETURN_DONE(funcctx);
  }
}

/* ----------------------------------------------------------------
 * branch_current() -> TEXT
 *
 * Returns the name of the currently active branch.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_current);

Datum branch_current(PG_FUNCTION_ARGS) {
  const char* branch_name =
      GetConfigOption("branch.active_branch", false, false);

  PG_RETURN_TEXT_P(cstring_to_text(branch_name));
}
