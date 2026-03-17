#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "executor/spi.h"

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
        "  LIKE %s INCLUDING ALL"
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
    ereport(ERROR,
            (errmsg("branch \"%s\" has no delta table (is it main?)", branch_name)));
  }

  /* Get column names from the base table (excluding delta metadata cols) */
  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(column_name, ', ') "
      "FROM information_schema.columns "
      "WHERE table_name = %s AND table_schema = 'public'",
      quote_literal_cstr(base_table));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errmsg("could not read columns for table \"%s\"", base_table)));
  }

  {
    char* columns =
        SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

    /* Get the primary key column name */
    resetStringInfo(&buf);
    appendStringInfo(
        &buf,
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

    {
      char* pk_col =
          SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

      /* Apply inserts */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO %s (%s) "
                       "SELECT %s FROM branch.%s "
                       "WHERE _op = 'I' ORDER BY _seq",
                       quote_identifier(base_table), columns, columns,
                       quote_identifier(delta_table));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply inserts for branch \"%s\"",
                               branch_name)));
      }

      /* Apply deletes */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM %s WHERE %s IN "
                       "(SELECT %s FROM branch.%s WHERE _op = 'D')",
                       quote_identifier(base_table), quote_identifier(pk_col),
                       quote_identifier(pk_col), quote_identifier(delta_table));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply deletes for branch \"%s\"",
                               branch_name)));
      }

      /* Apply updates: delete old rows then insert updated rows */
      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "DELETE FROM %s WHERE %s IN "
                       "(SELECT %s FROM branch.%s WHERE _op = 'U')",
                       quote_identifier(base_table), quote_identifier(pk_col),
                       quote_identifier(pk_col), quote_identifier(delta_table));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_DELETE) {
        ereport(ERROR, (errmsg("failed to apply updates (delete phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }

      resetStringInfo(&buf);
      appendStringInfo(&buf,
                       "INSERT INTO %s (%s) "
                       "SELECT %s FROM branch.%s "
                       "WHERE _op = 'U' ORDER BY _seq",
                       quote_identifier(base_table), columns, columns,
                       quote_identifier(delta_table));

      ret = SPI_execute(buf.data, false, 0);
      if (ret != SPI_OK_INSERT) {
        ereport(ERROR, (errmsg("failed to apply updates (insert phase) for "
                               "branch \"%s\"",
                               branch_name)));
      }
    }
  }

  /* Truncate the delta table */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "TRUNCATE branch.%s", quote_identifier(delta_table));

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to truncate delta table for branch \"%s\"", branch_name)));
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
  appendStringInfo(
      &buf, "SELECT delta_table FROM branch.branches WHERE name = %s",
      quote_literal_cstr(branch_name));

  ret = SPI_execute(buf.data, true, 1);

  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", branch_name)));
  }

  delta_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

  if (delta_table == NULL) {
    ereport(ERROR,
            (errmsg("branch \"%s\" has no delta table (is it main?)", branch_name)));
  }

  /* Truncate the delta table */
  resetStringInfo(&buf);
  appendStringInfo(&buf, "TRUNCATE branch.%s", quote_identifier(delta_table));

  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to truncate delta table for branch \"%s\"", branch_name)));
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
    delta_table =
        SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
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
        ereport(ERROR,
                (errmsg("could not read columns for table \"%s\"", base_table)));
      }
      columns =
          pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

      resetStringInfo(&buf);
      appendStringInfo(
          &buf,
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
      pk_col =
          pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

      /* Build the reconstructed view query */
      resetStringInfo(&buf);
      appendStringInfo(
          &buf,
          "SELECT %s FROM %s "
          "WHERE %s NOT IN (SELECT %s FROM branch.%s WHERE _op IN ('D','U')) "
          "UNION ALL "
          "SELECT %s FROM branch.%s WHERE _op IN ('I','U') "
          "ORDER BY %s",
          columns, quote_identifier(base_table),
          quote_identifier(pk_col), quote_identifier(pk_col),
          quote_identifier(delta_table), columns,
          quote_identifier(delta_table), quote_identifier(pk_col));
    }

    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT) {
      ereport(ERROR, (errmsg("failed to preview branch \"%s\"", branch_name)));
    }

    funcctx->max_calls = SPI_processed;
    funcctx->user_fctx = SPI_tuptable;

    /* Use the caller-supplied tuple descriptor from the AS clause */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
      ereport(ERROR,
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
