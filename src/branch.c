// clang-format off
#include "postgres.h"
// clang-format on
#include "access/htup_details.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

static char* active_branch = NULL;

void _PG_init(void) {
  DefineCustomStringVariable("branch.active_branch",
                             "The currently active branch name.", NULL,
                             &active_branch, "main",
                             PGC_USERSET, 0, NULL, NULL, NULL);
}

static char* work_schema_name(const char* branch_name) {
  return psprintf("branch_work_%s", branch_name);
}

static char* trigger_fn_name(const char* branch_name) {
  return psprintf("_cow_%s", branch_name);
}

/* Look up a branch's base_table by name. Must be called within SPI. */
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

/* Creates a new branch: work schema, empty delta table, overlay view,
 * INSTEAD OF triggers, and metadata registration. No data is copied. */
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
  char* parent_source;
  char* work_schema;
  char* delta_table;
  char* fn_name;
  char* columns;
  char* new_columns;
  char* old_columns;
  char* pk_cols;
  char* pk_join_cond;
  char* pk_first_col;
  char* base_columns;

  SPI_connect();

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

  if (strcmp(from_branch, "main") == 0) {
    parent_source = psprintf("public.%s", quote_identifier(base_table));
  } else {
    char* ps = work_schema_name(from_branch);
    parent_source = psprintf("%s.%s", quote_identifier(ps),
                             quote_identifier(base_table));
  }

  work_schema = work_schema_name(new_branch);
  delta_table = psprintf("branch_delta_%s", new_branch);
  fn_name = trigger_fn_name(new_branch);

  resetStringInfo(&buf);
  appendStringInfo(&buf, "CREATE SCHEMA %s", quote_identifier(work_schema));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to create work schema \"%s\"", work_schema)));
  }

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

  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT "
      "  string_agg(quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('NEW.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('OLD.' || quote_ident(column_name), ', ' "
      "             ORDER BY ordinal_position), "
      "  string_agg('base.' || quote_ident(column_name), ', ' "
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
  base_columns =
      pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4));

  resetStringInfo(&buf);
  appendStringInfo(
      &buf,
      "SELECT string_agg(quote_ident(a.attname), ', ' "
      "  ORDER BY array_position(i.indkey::int[], a.attnum::int)), "
      "string_agg('base.' || quote_ident(a.attname) || ' = d.' || "
      "  quote_ident(a.attname), ' AND ' "
      "  ORDER BY array_position(i.indkey::int[], a.attnum::int)), "
      "(array_agg(quote_ident(a.attname) "
      "  ORDER BY array_position(i.indkey::int[], a.attnum::int)))[1] "
      "FROM pg_index i "
      "JOIN pg_attribute a ON a.attrelid = i.indrelid "
      "AND a.attnum = ANY(i.indkey) "
      "WHERE i.indrelid = %s::regclass AND i.indisprimary",
      quote_literal_cstr(psprintf("public.%s", base_table)));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errmsg("could not determine primary key for \"%s\"",
                           base_table)));
  }
  {
    char* pk_val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
    char* join_val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
    char* first_val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
    if (pk_val == NULL) {
      ereport(ERROR, (errmsg("table \"%s\" has no primary key", base_table)));
    }
    pk_cols = pstrdup(pk_val);
    pk_join_cond = pstrdup(join_val);
    pk_first_col = pstrdup(first_val);
  }

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE VIEW %s.%s AS "
                   "SELECT %s FROM %s base "
                   "LEFT JOIN ("
                   "  SELECT DISTINCT ON (%s) %s "
                   "  FROM branch.%s "
                   "  ORDER BY %s, _seq DESC"
                   ") d ON %s "
                   "WHERE d.%s IS NULL "
                   "UNION ALL "
                   "SELECT %s FROM ("
                   "  SELECT DISTINCT ON (%s) _op, %s "
                   "  FROM branch.%s "
                   "  ORDER BY %s, _seq DESC"
                   ") d2 WHERE d2._op IN ('I','U')",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   base_columns, parent_source,
                   pk_cols, pk_cols, quote_identifier(delta_table), pk_cols,
                   pk_join_cond, pk_first_col,
                   columns,
                   pk_cols, columns, quote_identifier(delta_table), pk_cols);
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR,
            (errmsg("failed to create overlay view for branch \"%s\"",
                    new_branch)));
  }

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

  resetStringInfo(&buf);
  appendStringInfo(&buf,
                   "CREATE TRIGGER _cow "
                   "INSTEAD OF INSERT OR UPDATE OR DELETE ON %s.%s "
                   "FOR EACH ROW EXECUTE FUNCTION branch.%s()",
                   quote_identifier(work_schema), quote_identifier(base_table),
                   quote_identifier(fn_name));
  ret = SPI_execute(buf.data, false, 0);
  if (ret != SPI_OK_UTILITY) {
    ereport(ERROR, (errmsg("failed to install INSTEAD OF trigger on \"%s.%s\"",
                           work_schema, base_table)));
  }

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

  SPI_finish();

  elog(NOTICE, "branch \"%s\" created from \"%s\"", new_branch, from_branch);
  PG_RETURN_VOID();
}

/* Sets the active branch GUC and search_path for the session. */
PG_FUNCTION_INFO_V1(branch_switch);

Datum branch_switch(PG_FUNCTION_ARGS) {
  text* target_t = PG_GETARG_TEXT_PP(0);
  char* target = text_to_cstring(target_t);
  int ret;
  StringInfoData buf;
  char* new_search_path;

  SPI_connect();

  initStringInfo(&buf);
  appendStringInfo(&buf, "SELECT 1 FROM branch.branches WHERE name = %s",
                   quote_literal_cstr(target));

  ret = SPI_execute(buf.data, true, 1);
  if (ret != SPI_OK_SELECT || SPI_processed == 0) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("branch \"%s\" does not exist", target)));
  }

  SPI_finish();

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

/* Replays the branch's delta log into the base table, then truncates it. */
PG_FUNCTION_INFO_V1(branch_apply);

Datum branch_apply(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* base_table;
  char* delta_table;

  SPI_connect();

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

    resetStringInfo(&buf);
    appendStringInfo(
        &buf,
        "SELECT string_agg(quote_ident(a.attname), ', ' "
        "  ORDER BY array_position(i.indkey::int[], a.attnum::int)) "
        "FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary",
        quote_literal_cstr(psprintf("public.%s", base_table)));

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

/* Discards all branch changes by truncating the delta table. */
PG_FUNCTION_INFO_V1(branch_rollback);

Datum branch_rollback(PG_FUNCTION_ARGS) {
  text* branch_name_t = PG_GETARG_TEXT_PP(0);
  char* branch_name = text_to_cstring(branch_name_t);

  int ret;
  StringInfoData buf;
  char* delta_table;

  SPI_connect();

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

/* Returns the current branch state as a set of records. */
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

/* Returns the name of the currently active branch. */
PG_FUNCTION_INFO_V1(branch_current);

Datum branch_current(PG_FUNCTION_ARGS) {
  const char* branch_name =
      GetConfigOption("branch.active_branch", false, false);

  PG_RETURN_TEXT_P(cstring_to_text(branch_name));
}
