#include "postgres.h"
#include "fmgr.h"
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
