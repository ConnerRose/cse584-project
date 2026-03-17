// clang-format off
#include "postgres.h"
// clang-format on
#include "access/htup_details.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"

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

/* ----------------------------------------------------------------
 * branch_create(new_branch TEXT, from_branch TEXT)
 *
 * Creates a new branch by inserting a row into branch.branches
 * and creating an empty delta table for it.
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

  {
    int parent_id;
    char* base_table;
    char* delta_table;
    bool isnull;

    parent_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                            SPI_tuptable->tupdesc, 1, &isnull));
    base_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

    /* Delta table name: branch_delta_<new_branch> */
    delta_table = psprintf("branch_delta_%s", new_branch);

    /* Create the delta table with the same schema as the base table */
    resetStringInfo(&buf);
    appendStringInfo(
        &buf,
        "CREATE TABLE branch.%s ("
        "  _op CHAR(1) NOT NULL," /* 'I'nsert, 'D'elete, 'U'pdate */
        "  _seq BIGSERIAL,"
        "  LIKE %s INCLUDING DEFAULTS"
        ")",
        quote_identifier(delta_table), quote_identifier(base_table));

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_UTILITY) {
      ereport(ERROR, (errmsg("failed to create delta table for branch \"%s\"",
                             new_branch)));
    }

    /* Insert metadata row */
    resetStringInfo(&buf);
    appendStringInfo(&buf,
                     "INSERT INTO branch.branches (name, parent_id, "
                     "base_table, delta_table) "
                     "VALUES (%s, %d, %s, %s)",
                     quote_literal_cstr(new_branch), parent_id,
                     quote_literal_cstr(base_table),
                     quote_literal_cstr(delta_table));

    ret = SPI_execute(buf.data, false, 0);
    if (ret != SPI_OK_INSERT) {
      ereport(ERROR, (errmsg("failed to register branch \"%s\"", new_branch)));
    }
  }

  SPI_finish();

  elog(NOTICE, "branch \"%s\" created from \"%s\"", new_branch, from_branch);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_switch(target_branch TEXT)
 *
 * Sets the session GUC branch.active_branch to the target.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_switch);

Datum branch_switch(PG_FUNCTION_ARGS) {
  text* target_t = PG_GETARG_TEXT_PP(0);
  char* target = text_to_cstring(target_t);
  int ret;
  StringInfoData buf;

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

  /* Set the GUC */
  SetConfigOption("branch.active_branch", target, PGC_USERSET, PGC_S_SESSION);

  elog(NOTICE, "switched to branch \"%s\"", target);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_apply(branch_name TEXT)
 *
 * Replays the delta log for the given branch into the base table,
 * applying inserts, deletes, and updates in _seq order, then
 * truncates the delta table.
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

  base_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

  if (delta_table == NULL) {
    ereport(ERROR, (errmsg("branch \"%s\" has no delta table (is it main?)",
                           branch_name)));
  }

  /* Get column names from the base table (excluding delta metadata cols) */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT string_agg(column_name, ', ') "
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
        SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

    /* Get the primary key column name */
    resetStringInfo(&buf);
    appendStringInfo(&buf,
                     "SELECT a.attname FROM pg_index i "
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
      char* pk_col =
          SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

      /*
       * Materialize the latest delta per PK into a temp table so we
       * apply only the most recent operation for each row.
       */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "CREATE TEMP TABLE _branch_apply_latest AS "
                       "SELECT DISTINCT ON (%s) _op, %s FROM branch.%s "
                       "ORDER BY %s, _seq DESC",
                       quote_identifier(pk_col), columns,
                       quote_identifier(delta_table), quote_identifier(pk_col));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_UTILITY) {
        ereport(ERROR, (errmsg("failed to materialize latest deltas")));
      }

      /* Apply inserts */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO %s (%s) "
                       "SELECT %s FROM _branch_apply_latest "
                       "WHERE _op = 'I'",
                       quote_identifier(base_table), columns, columns);

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply inserts for branch \"%s\"",
                               branch_name)));
      }

      /* Apply deletes */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM %s WHERE %s IN "
                       "(SELECT %s FROM _branch_apply_latest WHERE _op = 'D')",
                       quote_identifier(base_table), quote_identifier(pk_col),
                       quote_identifier(pk_col));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply deletes for branch \"%s\"",
                               branch_name)));
      }

      /* Apply updates: delete old rows then insert updated rows */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM %s WHERE %s IN "
                       "(SELECT %s FROM _branch_apply_latest WHERE _op = 'U')",
                       quote_identifier(base_table), quote_identifier(pk_col),
                       quote_identifier(pk_col));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply updates (delete phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }

      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO %s (%s) "
                       "SELECT %s FROM _branch_apply_latest "
                       "WHERE _op = 'U'",
                       quote_identifier(base_table), columns, columns);

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply updates (insert phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }

      /* Drop the temp table */
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
 * Discards all changes in the branch's delta table by truncating it.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_rollback);

Datum branch_rollback(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* delta_table;

  SPI_connect();

  /* Look up the delta table */
  initStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT delta_table FROM branch.branches WHERE name = %s",
                   quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);

  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", branch_name)));
  }

  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

  if (delta_table == NULL) {
    ereport(ERROR, (errmsg("branch \"%s\" has no delta table (is it main?)",
                           branch_name)));
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

  elog(NOTICE, "rolled back all changes for branch \"%s\"", branch_name);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_preview() -> SETOF RECORD
 *
 * Returns the reconstructed state of the current branch by
 * overlaying the delta log onto the base table. Reads the active
 * branch from the session GUC. For main (no delta table), returns
 * the base table as-is.
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
    char* delta_table;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    branch_name = GetConfigOption("branch.active_branch", false, false);

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
    if (delta_table != NULL) {
      delta_table = pstrdup(delta_table);
    }

    if (delta_table == NULL) {
      /* Main branch: just return the base table */
      resetStringInfo(&buf);
      appendStringInfo(&buf, "SELECT * FROM %s", quote_identifier(base_table));
    } else {
      /* Get column names and PK */
      char* columns;
      char* pk_col;

      resetStringInfo(&buf);
      appendStringInfo(
          &buf,
          "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) "
          "FROM information_schema.columns "
          "WHERE table_name = %s AND table_schema = 'public'",
          quote_literal_cstr(base_table));

      ret = SPI_execute(buf.data, true, 1);
      if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        ereport(ERROR, (errmsg("could not read columns for table \"%s\"",
                               base_table)));
      }
      columns = pstrdup(
          SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "SELECT a.attname FROM pg_index i "
                       "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                       "AND a.attnum = ANY(i.indkey) "
                       "WHERE i.indrelid = %s::regclass AND i.indisprimary",
                       quote_literal_cstr(base_table));

      ret = SPI_execute(buf.data, true, 1);
      if (ret != SPI_OK_SELECT || SPI_processed == 0) {
        ereport(ERROR, (errmsg("could not determine primary key for \"%s\"",
                               base_table)));
      }
      pk_col = pstrdup(
          SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

      /* Build the reconstructed view query using latest delta per PK */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "WITH latest AS ("
                       "  SELECT DISTINCT ON (%s) _op, %s FROM branch.%s "
                       "  ORDER BY %s, _seq DESC"
                       ") "
                       "SELECT %s FROM %s "
                       "WHERE %s NOT IN (SELECT %s FROM latest) "
                       "UNION ALL "
                       "SELECT %s FROM latest WHERE _op IN ('I','U') "
                       "ORDER BY %s",
                       quote_identifier(pk_col), columns,
                       quote_identifier(delta_table), quote_identifier(pk_col),
                       columns, quote_identifier(base_table),
                       quote_identifier(pk_col), quote_identifier(pk_col),
                       columns, quote_identifier(pk_col));
    }

    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT) {
      ereport(ERROR, (errmsg("failed to preview branch \"%s\"", branch_name)));
    }

    funcctx->max_calls = SPI_processed;
    funcctx->user_fctx = SPI_tuptable;

    /* Use the caller-supplied tuple descriptor from the AS clause */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(
          ERROR,
          (errmsg(
              "branch.preview() must be called with a column "
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

    /* Extract values from SPI tuple and rebuild with caller's tupdesc */
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
 * branch_run(sql TEXT)
 *
 * Executes arbitrary SQL (INSERT/UPDATE/DELETE) against the base
 * table, but intercepts row-level effects via BEFORE ROW triggers
 * that capture changes into the delta table and suppress the
 * actual base table modification.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_run);

Datum branch_run(PG_FUNCTION_ARGS) {
  text* sql_t = PG_GETARG_TEXT_PP(0);
  char* sql = text_to_cstring(sql_t);
  const char* branch_name;
  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* columns;
  char* new_columns;
  char* old_columns;
  char* pk_col;

  branch_name = GetConfigOption("branch.active_branch", false, false);

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

  /* Get column lists: plain, NEW.-prefixed, OLD.-prefixed */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position), "
      "string_agg('NEW.' || column_name, ', ' ORDER BY ordinal_position), "
      "string_agg('OLD.' || column_name, ', ' ORDER BY ordinal_position) "
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

  /* Get primary key column */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT a.attname FROM pg_index i "
                   "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                   "AND a.attnum = ANY(i.indkey) "
                   "WHERE i.indrelid = %s::regclass AND i.indisprimary",
                   quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not determine primary key for \"%s\"", base_table)));
  }

  pk_col =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

  /*
   * Create a temp table with the same name as the base table, populated
   * with the branch preview (base + latest deltas). Temp tables shadow
   * regular tables in search_path, so the user's SQL will operate on
   * the branch state transparently.
   */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TEMP TABLE %s ON COMMIT DROP AS "
                   "WITH latest AS ("
                   "  SELECT DISTINCT ON (%s) _op, %s FROM branch.%s "
                   "  ORDER BY %s, _seq DESC"
                   ") "
                   "SELECT %s FROM public.%s "
                   "WHERE %s NOT IN (SELECT %s FROM latest) "
                   "UNION ALL "
                   "SELECT %s FROM latest WHERE _op IN ('I','U')",
                   quote_identifier(base_table), quote_identifier(pk_col),
                   columns, quote_identifier(delta_table),
                   quote_identifier(pk_col), columns,
                   quote_identifier(base_table), quote_identifier(pk_col),
                   quote_identifier(pk_col), columns);

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create branch preview table")));
  }

  /* Create the trigger function that captures changes into the delta table */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE OR REPLACE FUNCTION branch._run_trigger_fn() "
                   "RETURNS TRIGGER AS $t$ "
                   "BEGIN "
                   "  IF TG_OP = 'INSERT' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('I', %s); "
                   "    RETURN NULL; "
                   "  ELSIF TG_OP = 'DELETE' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('D', %s); "
                   "    RETURN NULL; "
                   "  ELSIF TG_OP = 'UPDATE' THEN "
                   "    INSERT INTO branch.%s (_op, %s) VALUES ('U', %s); "
                   "    RETURN NULL; "
                   "  END IF; "
                   "  RETURN NULL; "
                   "END; "
                   "$t$ LANGUAGE plpgsql",
                   quote_identifier(delta_table), columns, new_columns,
                   quote_identifier(delta_table), columns, old_columns,
                   quote_identifier(delta_table), columns, new_columns);

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create trigger function")));
  }

  /* Create the BEFORE ROW trigger on the temp table */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TRIGGER _branch_run_trigger "
                   "BEFORE INSERT OR UPDATE OR DELETE ON %s "
                   "FOR EACH ROW EXECUTE FUNCTION branch._run_trigger_fn()",
                   quote_identifier(base_table));

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to create trigger on \"%s\"", base_table)));
  }

  /* Execute the user's SQL — hits temp table, triggers capture deltas */
  ret = SPI_execute(sql, false, 0);
  if (ret < 0) {
    ereport(ERROR,
            (errmsg("failed to execute SQL on branch \"%s\"", branch_name)));
  }

  /* Clean up: drop temp table (trigger goes with it) and function */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "DROP TABLE IF EXISTS pg_temp.%s",
                   quote_identifier(base_table));
  SPI_execute(buf.data, false, 0);

  resetStringInfo(&buf);
  appendStringInfo(&buf, "DROP FUNCTION IF EXISTS branch._run_trigger_fn()");
  SPI_execute(buf.data, false, 0);

  SPI_finish();

  elog(NOTICE, "executed SQL on branch \"%s\"", branch_name);
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_binsert(data JSONB)
 *
 * Inserts a new row into the current branch's delta table.
 * Uses jsonb_populate_record to convert JSONB to the base table's
 * row type for type-safe insertion.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_binsert);

Datum branch_binsert(PG_FUNCTION_ARGS) {
  Jsonb* data = PG_GETARG_JSONB_P(0);
  char* data_str;
  const char* branch_name;
  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* columns;
  char* r_columns;

  data_str = JsonbToCString(NULL, &data->root, VARSIZE(data));
  branch_name = GetConfigOption("branch.active_branch", false, false);

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

  /* Get column lists: plain and r.-prefixed */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position), "
      "string_agg('r.' || column_name, ', ' ORDER BY ordinal_position) "
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
  r_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));

  /* Insert into delta via jsonb_populate_record */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "INSERT INTO branch.%s (_op, %s) "
                   "SELECT 'I', %s "
                   "FROM jsonb_populate_record(NULL::%s, %s::jsonb) AS r",
                   quote_identifier(delta_table), columns, r_columns,
                   quote_identifier(base_table), quote_literal_cstr(data_str));

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR,
            (errmsg("failed to insert into branch \"%s\"", branch_name)));
  }

  SPI_finish();
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_bdelete(pk_value INT)
 *
 * Deletes a row from the current branch by copying it from the
 * base table into the delta table with _op='D'.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_bdelete);

Datum branch_bdelete(PG_FUNCTION_ARGS) {
  int32 pk_value = PG_GETARG_INT32(0);
  const char* branch_name;
  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* columns;
  char* pk_col;

  branch_name = GetConfigOption("branch.active_branch", false, false);

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

  /* Get column names */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) "
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

  /* Get primary key column */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT a.attname FROM pg_index i "
                   "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                   "AND a.attnum = ANY(i.indkey) "
                   "WHERE i.indrelid = %s::regclass AND i.indisprimary",
                   quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not determine primary key for \"%s\"", base_table)));
  }

  pk_col =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

  /* Copy the row from base table into delta with _op='D' */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "INSERT INTO branch.%s (_op, %s) "
                   "SELECT 'D', %s FROM %s WHERE %s = %d",
                   quote_identifier(delta_table), columns, columns,
                   quote_identifier(base_table), quote_identifier(pk_col),
                   pk_value);

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR,
            (errmsg("failed to delete from branch \"%s\"", branch_name)));
  }

  if (SPI_processed == 0) {
    ereport(ERROR, (errmsg("row with %s = %d not found in \"%s\"", pk_col,
                           pk_value, base_table)));
  }

  SPI_finish();
  PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * branch_bupdate(pk_value INT, data JSONB)
 *
 * Updates a row on the current branch. Overlays the JSONB changes
 * onto the existing base row using jsonb_populate_record and
 * writes the result to the delta table with _op='U'.
 * ----------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(branch_bupdate);

Datum branch_bupdate(PG_FUNCTION_ARGS) {
  int32 pk_value = PG_GETARG_INT32(0);
  Jsonb* data = PG_GETARG_JSONB_P(1);
  char* data_str;
  const char* branch_name;
  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;
  char* columns;
  char* r_columns;
  char* pk_col;

  data_str = JsonbToCString(NULL, &data->root, VARSIZE(data));
  branch_name = GetConfigOption("branch.active_branch", false, false);

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

  /* Get column lists: plain and r.-prefixed */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position), "
      "string_agg('r.' || column_name, ', ' ORDER BY ordinal_position) "
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
  r_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));

  /* Get primary key column */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "SELECT a.attname FROM pg_index i "
                   "JOIN pg_attribute a ON a.attrelid = i.indrelid "
                   "AND a.attnum = ANY(i.indkey) "
                   "WHERE i.indrelid = %s::regclass AND i.indisprimary",
                   quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR,
            (errmsg("could not determine primary key for \"%s\"", base_table)));
  }

  pk_col =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

  /* Overlay JSONB onto existing row and insert as 'U' delta */
  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "INSERT INTO branch.%s (_op, %s) "
                   "SELECT 'U', %s "
                   "FROM jsonb_populate_record("
                   "  (SELECT b FROM %s b WHERE %s = %d), "
                   "  %s::jsonb"
                   ") AS r",
                   quote_identifier(delta_table), columns, r_columns,
                   quote_identifier(base_table), quote_identifier(pk_col),
                   pk_value, quote_literal_cstr(data_str));

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    ereport(ERROR, (errmsg("failed to update on branch \"%s\"", branch_name)));
  }

  if (SPI_processed == 0) {
    ereport(ERROR, (errmsg("row with %s = %d not found in \"%s\"", pk_col,
                           pk_value, base_table)));
  }

  SPI_finish();
  PG_RETURN_VOID();
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
