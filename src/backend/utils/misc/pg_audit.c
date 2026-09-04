#include "postgres.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"    /* MyProcPort, for client addr */
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/varlena.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_audit.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

/* ----------------------------------------------------------------
* GUC variable definitions
* ---------------------------------------------------------------- */
bool  dbblue_audit_enabled = true;
char *dbblue_audit_tables  = NULL;
char *dbblue_audit_operations = NULL;
bool  dbblue_audit_require_write = true;
char *dbblue_audit_exclude_columns = NULL;
char *dbblue_audit_retention = NULL;
bool  dbblue_audit_changed_columns_only = false;
char *dbblue_audit_database = NULL;

/*
 * Bitmask form of dbblue_audit_operations, kept in step with the GUC by the
 * assign hook so the per-row capture path does not have to re-parse the
 * string for every tuple.
 */
static int audit_ops_mask = DBBLUE_AUDIT_UPDATE | DBBLUE_AUDIT_DELETE;

/* ----------------------------------------------------------------
 * parse_audit_operations
 *
 * Turn the dbblue_audit_operations list into a bitmask.  Accepts
 * "insert", "update", "delete", plus "all" and "none" for convenience.
 * Returns false on an unrecognised name, leaving *mask untouched, so a
 * typo is rejected at SET time instead of silently auditing nothing.
 * ---------------------------------------------------------------- */
static bool
parse_audit_operations(const char *value, int *mask)
{
   char       *rawstring;
   List       *elemlist;
   ListCell   *lc;
   int         result = 0;

   rawstring = pstrdup(value);
   if (!SplitIdentifierString(rawstring, ',', &elemlist))
   {
       pfree(rawstring);
       list_free(elemlist);
       return false;
   }

   foreach(lc, elemlist)
   {
       const char *tok = (const char *) lfirst(lc);

       if (pg_strcasecmp(tok, "insert") == 0)
           result |= DBBLUE_AUDIT_INSERT;
       else if (pg_strcasecmp(tok, "update") == 0)
           result |= DBBLUE_AUDIT_UPDATE;
       else if (pg_strcasecmp(tok, "delete") == 0)
           result |= DBBLUE_AUDIT_DELETE;
       else if (pg_strcasecmp(tok, "all") == 0)
           result |= DBBLUE_AUDIT_INSERT | DBBLUE_AUDIT_UPDATE |
               DBBLUE_AUDIT_DELETE;
       else if (pg_strcasecmp(tok, "none") == 0)
           result = 0;
       else
       {
           pfree(rawstring);
           list_free(elemlist);
           return false;
       }
   }

   pfree(rawstring);
   list_free(elemlist);
   *mask = result;
   return true;
}

/* GUC check hook for dbblue_audit_operations. */
bool
dbblue_check_audit_operations(char **newval, void **extra, GucSource source)
{
   int         mask = 0;
   int        *myextra;

   if (!parse_audit_operations(*newval, &mask))
   {
       GUC_check_errdetail("Valid values are combinations of \"insert\", \"update\" and \"delete\", or \"all\" or \"none\".");
       return false;
   }

   myextra = (int *) guc_malloc(LOG, sizeof(int));
   if (myextra == NULL)
       return false;
   *myextra = mask;
   *extra = myextra;
   return true;
}

/* GUC assign hook for dbblue_audit_operations. */
void
dbblue_assign_audit_operations(const char *newval, void *extra)
{
   audit_ops_mask = *((int *) extra);
}

/* ----------------------------------------------------------------
 * dbblue_audit_operation_is_tracked
 *
 * True when this DML operation is one the operator asked to audit.
 * ---------------------------------------------------------------- */
bool
dbblue_audit_operation_is_tracked(int op)
{
   return (audit_ops_mask & op) != 0;
}


/* ----------------------------------------------------------------
* Reentrancy guard.
*
* dbblue_audit_write() issues an INSERT into dbblue_audit_log through SPI,
* which runs the executor again.  This guard makes sure that any DML issued
* while we are writing an audit row is never itself audited, so we can never
* recurse into the capture path.
* ---------------------------------------------------------------- */
static bool audit_in_progress = false;

/*
 * False once we know this backend cannot write audit rows (the log table
 * does not exist and this role may not create it).  The writer checks it
 * so a missing log table leaves the change unaudited rather than raising
 * an error that would abort the user's own statement.
 */
static bool audit_table_available = true;

static void audit_forget_plan(void);
static void audit_exclude_refresh(void);
static void audit_exclude_check_table(TupleDesc tupdesc, const char *schema_name, const char *table_name);
static void dbblue_audit_maybe_prune(void);
static bool audit_retention_is_set(void);
static SPIPlanPtr audit_insert_plan(void);
static const char *odoo_actor_name(int32 uid);

/* Scratch for the resolved Odoo actor names, set inside the write path. */
static const char *audit_created_by = NULL;
static const char *audit_changed_by = NULL;

/*
 * The audit INSERT, prepared once per backend and kept.
 *
 * Building and reparsing a literal INSERT for every audited row was the
 * dominant cost of this feature: a bulk UPDATE paid a full parse, analyse
 * and plan cycle per row on top of the execution itself.  A saved plan with
 * bound parameters skips all of that, and passing values as Datums also
 * removes the quoting round trip through text.
 */
static SPIPlanPtr audit_plan = NULL;

/*
 * Error context callback, so a failure raised from inside the audit write
 * says which table was being audited.
 *
 * It matters most with dbblue_audit_require_write on: there the error
 * propagates raw to the client, and without this the operator just sees
 * something like 'relation "dbblue.dbblue_audit_log" does not exist' with
 * no indication that the audit subsystem is what rejected their UPDATE.
 */
static void
audit_error_context(void *arg)
{
   errcontext("writing dbblue audit row for table \"%s\"", (const char *) arg);
}

/*
 * Do the actual insert.  Raises on any failure; the caller decides whether
 * that aborts the user's statement or is contained in a subtransaction.
 */
static void
audit_do_insert(const char *table_name, const char *operation,
                const char *old_json, const char *new_json,
                const char *username, const char *sess_user,
                const char *client_addr,
                bool have_create_uid, int32 create_uid,
                bool have_write_uid, int32 write_uid)
{
   Datum       values[11];
   char        nulls[11];
   int         spi_ret;
   ErrorContextCallback ctx;

   ctx.callback = audit_error_context;
   ctx.arg = unconstify(char *, table_name);
   ctx.previous = error_context_stack;
   error_context_stack = &ctx;

   if (SPI_connect() != SPI_OK_CONNECT)
       elog(ERROR, "dbblue_audit: SPI_connect failed");

   audit_created_by = have_create_uid ? odoo_actor_name(create_uid) : NULL;
   if (have_write_uid)
       audit_changed_by = (have_create_uid && write_uid == create_uid)
           ? audit_created_by
           : odoo_actor_name(write_uid);
   else
       audit_changed_by = NULL;

   memset(nulls, ' ', sizeof(nulls));

   values[0] = CStringGetTextDatum(table_name);
   values[1] = CStringGetTextDatum(operation);
   if (old_json)
       values[2] = DirectFunctionCall1(jsonb_in, CStringGetDatum(old_json));
   else
       nulls[2] = 'n';
   if (new_json)
       values[3] = DirectFunctionCall1(jsonb_in, CStringGetDatum(new_json));
   else
       nulls[3] = 'n';
   values[4] = CStringGetTextDatum(username);
   values[5] = CStringGetTextDatum(sess_user);
   values[6] = CStringGetTextDatum(client_addr);
   if (have_create_uid)
       values[7] = Int32GetDatum(create_uid);
   else
       nulls[7] = 'n';
   if (have_write_uid)
       values[8] = Int32GetDatum(write_uid);
   else
       nulls[8] = 'n';
   if (audit_created_by)
       values[9] = CStringGetTextDatum(audit_created_by);
   else
       nulls[9] = 'n';
   if (audit_changed_by)
       values[10] = CStringGetTextDatum(audit_changed_by);
   else
       nulls[10] = 'n';

   spi_ret = SPI_execute_plan(audit_insert_plan(), values, nulls, false, 0);
   if (spi_ret != SPI_OK_INSERT)
       elog(ERROR, "dbblue_audit: audit insert returned %d", spi_ret);

   SPI_finish();

   error_context_stack = ctx.previous;
}

static SPIPlanPtr
audit_insert_plan(void)
{
   static const Oid argtypes[11] = {
       TEXTOID, TEXTOID, JSONBOID, JSONBOID,
       TEXTOID, TEXTOID, TEXTOID,
       INT4OID, INT4OID, TEXTOID, TEXTOID
   };
   SPIPlanPtr  plan;

   if (audit_plan != NULL)
       return audit_plan;

   plan = SPI_prepare(
       "INSERT INTO dbblue.dbblue_audit_log "
       "(rel_name, dml_op, old_data, new_data,"
       " changed_by, session_usr, client_addr, logged_at,"
       " odoo_create_uid, odoo_write_uid, odoo_created_by, odoo_changed_by) "
       "VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8, $9, $10, $11)",
       11, (Oid *) argtypes);

   if (plan == NULL)
       elog(ERROR, "dbblue_audit: could not prepare the audit insert: %s",
            SPI_result_code_string(SPI_result));

   if (SPI_keepplan(plan) != 0)
       elog(ERROR, "dbblue_audit: could not cache the audit insert plan");

   audit_plan = plan;
   return audit_plan;
}

/*
 * Drop the cached plan when the log table goes away, so the next write
 * re-prepares against whatever replaced it rather than failing forever on
 * a stale plan.
 */
static void
audit_forget_plan(void)
{
   if (audit_plan != NULL)
   {
       SPI_freeplan(audit_plan);
       audit_plan = NULL;
   }
}

/*
 * The audit trail is written with the bootstrap superuser's identity, not
 * the identity of whoever ran the statement.
 *
 * That is what lets dbblue.dbblue_audit_log carry no privileges for PUBLIC
 * at all.  If the insert ran as the invoking user, the table would need
 * INSERT granted to PUBLIC -- and a role with INSERT can forge entries
 * naming someone else, while the SELECT it would also need lets it read
 * old_data/new_data for tables it has no rights to.  Elevating here closes
 * both: the only way to add a row is to actually modify an audited table,
 * and the identity columns are filled in from the real user before the
 * switch.
 *
 * Callers must pair these and restore in a PG_FINALLY, since an error
 * inside SPI would otherwise leave the elevated identity in place.
 */
typedef struct AuditPriv
{
   Oid         save_userid;
   int         save_sec_context;
} AuditPriv;

static void
audit_priv_enter(AuditPriv *priv)
{
   GetUserIdAndSecContext(&priv->save_userid, &priv->save_sec_context);
   SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
                          priv->save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
}

static void
audit_priv_leave(AuditPriv *priv)
{
   SetUserIdAndSecContext(priv->save_userid, priv->save_sec_context);
}


/* ----------------------------------------------------------------
* Forward declarations
* ---------------------------------------------------------------- */
static char *dbblue_audit_tuple_to_json(HeapTuple tuple, HeapTuple other,
                                        Bitmapset *keepcols,
                                        TupleDesc tupdesc,
                                        const char *schema_name,
                                        const char *table_name);








/* ----------------------------------------------------------------
* Static flag: have we already ensured the audit table exists
* this session? Avoids re-checking on every single DML operation.
*
* Reset to false by the xact callback whenever a transaction aborts.
* This prevents stale flag state after a rollback discards the table.
* ---------------------------------------------------------------- */
static bool audit_table_checked = false;
static bool xact_callback_registered = false;


/* ----------------------------------------------------------------
* dbblue_audit_xact_callback
*
* Transaction end callback registered once per backend session.  Resets
* the audit_table_checked flag when a transaction aborts, so that if the
* CREATE TABLE IF NOT EXISTS was rolled back, the next transaction will
* recreate it.
* ---------------------------------------------------------------- */
static void
dbblue_audit_xact_callback(XactEvent event, void *arg)
{
   if (event == XACT_EVENT_ABORT)
   {
       audit_table_checked = false;
       audit_table_available = true;   /* re-probe rather than stay disabled */

       /*
        * The saved plan references the log table by OID.  If that table was
        * just rolled back out of existence -- or was dropped by another
        * session, which is what made this write fail -- the plan is stale
        * and every later attempt would fail on it.  Drop it so the next
        * write re-prepares.
        */
       audit_forget_plan();
   }
}




/* ================================================================
 * dbblue_audit_retention
 *
 * Held as text and parsed as an SQL interval rather than as a number of
 * seconds, so a retention policy can be stated the way people actually
 * express one: "90 days", "6 months", "7 years".
 *
 * Months and years are not expressible in seconds without lying about
 * them -- a month is not 30 days and a year is not 365 -- and an audit
 * retention rule of "keep seven years" that silently drifts by two days
 * per leap cycle is the wrong answer for anything a compliance auditor
 * will read.  An interval carries calendar semantics through to the
 * subtraction, so now() - interval '1 year' means what it says.
 * ================================================================ */
static Interval audit_retention_iv = {0, 0, 0};

/*
 * Parse without raising.  interval_in() would ereport on bad input, which
 * is unusable in a GUC check hook, so decode through the lower-level
 * routines that return an error code instead.
 */
static bool
audit_parse_retention(const char *str, Interval *result)
{
   char       *field[MAXDATEFIELDS];
   int         ftype[MAXDATEFIELDS];
   char        workbuf[MAXDATELEN + MAXDATEFIELDS];
   struct pg_itm_in itm_in;
   int         nf;
   int         dtype;
   int         dterr;
   int64       months;

   itm_in.tm_year = 0;
   itm_in.tm_mon = 0;
   itm_in.tm_mday = 0;
   itm_in.tm_usec = 0;

   dterr = ParseDateTime(str, workbuf, sizeof(workbuf), field, ftype,
                         MAXDATEFIELDS, &nf);
   if (dterr == 0)
       dterr = DecodeInterval(field, ftype, nf, INTERVAL_FULL_RANGE,
                              &dtype, &itm_in);
   if (dterr != 0 || dtype != DTK_DELTA)
       return false;

   months = (int64) itm_in.tm_year * MONTHS_PER_YEAR + itm_in.tm_mon;
   if (months > PG_INT32_MAX || months < PG_INT32_MIN)
       return false;

   /* A negative retention would delete rows from the future. */
   if (months < 0 || itm_in.tm_mday < 0 || itm_in.tm_usec < 0)
       return false;

   result->month = (int32) months;
   result->day = itm_in.tm_mday;
   result->time = itm_in.tm_usec;
   return true;
}

bool
dbblue_check_audit_retention(char **newval, void **extra, GucSource source)
{
   Interval    iv;
   Interval   *myextra;

   if (*newval == NULL || **newval == '\0' || strcmp(*newval, "0") == 0)
   {
       iv.month = 0;
       iv.day = 0;
       iv.time = 0;
   }
   else if (!audit_parse_retention(*newval, &iv))
   {
       GUC_check_errdetail("Expected an interval such as \"90 days\", \"6 months\" or \"2 min\", or 0 to keep everything.");
       return false;
   }

   myextra = (Interval *) guc_malloc(LOG, sizeof(Interval));
   if (myextra == NULL)
       return false;
   *myextra = iv;
   *extra = myextra;
   return true;
}

void
dbblue_assign_audit_retention(const char *newval, void *extra)
{
   audit_retention_iv = *((Interval *) extra);
}

static bool
audit_retention_is_set(void)
{
   return audit_retention_iv.month != 0 ||
       audit_retention_iv.day != 0 ||
       audit_retention_iv.time != 0;
}

/* ----------------------------------------------------------------
 * dbblue_audit_prune_expired
 *
 * Delete audit rows older than dbblue_audit_retention.
 *
 * Nothing in this module used to remove anything, so the log grew without
 * bound.  Pruning runs once per backend, the first time that backend
 * touches the log, and is capped at DBBLUE_AUDIT_PRUNE_BATCH rows so a long
 * backlog is worked off over many sessions instead of stalling one
 * statement behind a huge DELETE.
 *
 * A busy system with few, long-lived pooled connections will prune rarely;
 * there, a scheduled job calling the same DELETE is the better tool.  This
 * exists so the default configuration cannot grow forever unattended.
 *
 * Caller must hold an SPI connection and the elevated identity.
 * ---------------------------------------------------------------- */
int64
dbblue_audit_prune_batch(void)
{
   char        sql[512];
   int         spi_ret;

   if (!audit_retention_is_set())
       return 0;

   /*
    * Rebuilt from the parsed components rather than interpolating the
    * setting's text, so nothing the operator typed reaches the SQL.
    */
   snprintf(sql, sizeof(sql),
            "DELETE FROM dbblue.dbblue_audit_log WHERE id IN ("
            "  SELECT id FROM dbblue.dbblue_audit_log"
            "   WHERE logged_at < now() - make_interval(months => %d,"
            "                                           days => %d,"
            "                                           secs => %.6f)"
            "   ORDER BY id LIMIT %d)",
            audit_retention_iv.month, audit_retention_iv.day,
            (double) audit_retention_iv.time / USECS_PER_SEC,
            DBBLUE_AUDIT_PRUNE_BATCH);

   spi_ret = SPI_execute(sql, false, 0);
   if (spi_ret < 0)
   {
       elog(WARNING, "dbblue_audit: could not prune expired rows: %d", spi_ret);
       return 0;
   }

   if (SPI_processed > 0)
       elog(DEBUG1, "dbblue_audit: pruned %llu audit row(s) older than %s",
            (unsigned long long) SPI_processed, dbblue_audit_retention);

   return (int64) SPI_processed;
}

/*
 * True when a retention period is configured.  Exposed so the retention
 * worker can skip its sweep entirely rather than issuing a DELETE that
 * can never match.
 */
bool
dbblue_audit_retention_is_active(void)
{
   return audit_retention_is_set();
}


/* ----------------------------------------------------------------
 * dbblue_audit_maybe_prune
 *
 * Run the retention sweep if enough time has passed since this backend
 * last ran one.
 *
 * It used to run only on a backend's first write to the log, which is not
 * a retention policy at all: a pooled connection that lives for days
 * swept once when it opened and never again, so rows sat far past their
 * retention while the setting claimed otherwise.
 *
 * The cadence follows the retention itself -- a quarter of it, clamped to
 * between 5 seconds and 5 minutes -- so a two-minute retention is swept
 * every thirty seconds while a seven-year one is not swept more than
 * every five minutes.
 *
 * This still only runs in a backend that is writing audit rows.  A
 * database with no audited DML at all prunes nothing, because nothing
 * calls in here; that needs a background worker, which this is not.
 * ---------------------------------------------------------------- */
static TimestampTz audit_last_prune = 0;

static void
dbblue_audit_maybe_prune(void)
{
   TimestampTz now;
   long        cadence_s;
   int64       retention_s;
   AuditPriv   priv;
   MemoryContext oldcxt;
   ResourceOwner oldowner;

   if (!audit_retention_is_set())
       return;

   /* Approximate: only used to pick how often to sweep. */
   retention_s = (int64) audit_retention_iv.month * 30 * SECS_PER_DAY
       + (int64) audit_retention_iv.day * SECS_PER_DAY
       + audit_retention_iv.time / USECS_PER_SEC;

   cadence_s = (long) (retention_s / 4);
   if (cadence_s < 5)
       cadence_s = 5;
   if (cadence_s > 300)
       cadence_s = 300;

   now = GetCurrentTimestamp();
   if (audit_last_prune != 0 &&
       !TimestampDifferenceExceeds(audit_last_prune, now, cadence_s * 1000))
       return;

   /*
    * Stamp before attempting, so a sweep that keeps failing is retried on
    * the cadence rather than on every single audited row.
    */
   audit_last_prune = now;

   oldcxt = CurrentMemoryContext;
   oldowner = CurrentResourceOwner;

   BeginInternalSubTransaction(NULL);
   audit_priv_enter(&priv);

   PG_TRY();
   {
       if (SPI_connect() != SPI_OK_CONNECT)
           elog(ERROR, "dbblue_audit: SPI_connect failed in prune");
       (void) dbblue_audit_prune_batch();
       SPI_finish();

       audit_priv_leave(&priv);
       ReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;
   }
   PG_CATCH();
   {
       ErrorData  *edata;

       audit_priv_leave(&priv);
       MemoryContextSwitchTo(oldcxt);
       edata = CopyErrorData();
       FlushErrorState();
       RollbackAndReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;

       ereport(WARNING,
               (errmsg("dbblue_audit: retention sweep failed: %s",
                       edata->message),
                errdetail("Old audit rows were not removed; the change being audited is unaffected.")));
       FreeErrorData(edata);
   }
   PG_END_TRY();
}

/* ----------------------------------------------------------------
* dbblue_audit_ensure_log_table
*
* Creates dbblue_audit_log if it does not already exist.
* Uses CREATE TABLE IF NOT EXISTS so it is safe to call multiple
* times — but the static flag above means we only ever call it
* once per backend session.
* ---------------------------------------------------------------- */
static void dbblue_audit_ensure_log_table_guts(void);

/*
 * Create the log table if needed, with the bootstrap superuser's rights.
 * Elevating covers the DDL as well as the insert: an ordinary role can
 * neither create the dbblue schema nor own the table, and failing that way
 * would abort the statement it was auditing.
 */
static void
dbblue_audit_ensure_log_table(void)
{
   AuditPriv   priv;
   MemoryContext oldcxt;
   ResourceOwner oldowner;

   if (audit_table_checked)
       return;

   /*
    * Contained in a subtransaction, because everything inside can raise:
    * the DDL races another backend creating the same schema, a GRANT hits
    * an unexpected ownership, or the retention sweep trips over a trigger
    * or a full disk.  None of that should abort the statement being
    * audited -- and unlike the per-row write, this runs at most once per
    * backend, so the subtransaction costs nothing worth counting.
    */
   oldcxt = CurrentMemoryContext;
   oldowner = CurrentResourceOwner;

   BeginInternalSubTransaction(NULL);
   audit_priv_enter(&priv);

   PG_TRY();
   {
       dbblue_audit_ensure_log_table_guts();
       audit_priv_leave(&priv);
       ReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;
   }
   PG_CATCH();
   {
       ErrorData  *edata;

       audit_priv_leave(&priv);
       MemoryContextSwitchTo(oldcxt);
       edata = CopyErrorData();
       FlushErrorState();
       RollbackAndReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;

       audit_forget_plan();
       audit_table_checked = false;    /* retry in the next transaction */

       if (dbblue_audit_require_write)
           ereport(ERROR,
                   (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("dbblue_audit: could not prepare the audit log"),
                    errdetail("%s", edata->message),
                    errhint("Set dbblue_audit_require_write to off to let unaudited changes proceed.")));

       ereport(WARNING,
               (errmsg("dbblue_audit: could not prepare the audit log: %s",
                       edata->message),
                errdetail("Changes to audited tables will not be recorded until this is resolved.")));
       FreeErrorData(edata);
       audit_table_available = false;
   }
   PG_END_TRY();
}

static void
dbblue_audit_ensure_log_table_guts(void)
{
   int spi_ret;


   if (audit_table_checked)
       return;


   /*
    * Register the xact callback once per session to reset the flag if a
    * transaction aborts (which would discard the audit table and require
    * recreating it in the next transaction).
    */
   if (!xact_callback_registered)
   {
       RegisterXactCallback(dbblue_audit_xact_callback, NULL);
       xact_callback_registered = true;
   }


   if (SPI_connect() != SPI_OK_CONNECT)
   {
       elog(WARNING, "dbblue_audit: SPI_connect failed in ensure_log_table");
       return;
   }

   /*
    * Fast path.  Once the log table exists there is nothing to create, and
    * running the DDL below anyway would fail for every non-superuser:
    * CREATE SCHEMA IF NOT EXISTS still requires CREATE on the database even
    * when the schema is already there, and that error would propagate out
    * of the audit path and abort the user's own statement.
    */
   spi_ret = SPI_execute(
       "SELECT to_regclass('dbblue.dbblue_audit_log') IS NOT NULL"
       "   AND has_table_privilege('dbblue.dbblue_audit_log', 'INSERT')"
       "   AND coalesce(has_sequence_privilege("
       "         pg_get_serial_sequence('dbblue.dbblue_audit_log','id'),"
       "         'USAGE'), true)", true, 1);

   if (spi_ret == SPI_OK_SELECT && SPI_processed == 1)
   {
       bool        isnull;
       Datum       d = SPI_getbinval(SPI_tuptable->vals[0],
                                     SPI_tuptable->tupdesc, 1, &isnull);

       if (!isnull && DatumGetBool(d))
       {
           audit_table_checked = true;
           SPI_finish();
           return;
       }
   }

   /*
    * Create the dbblue schema if it doesn't exist, then create the audit log
    * table in it. Using a dedicated schema prevents namespace collisions and
    * makes it clear that this is a system table.
    */
   spi_ret = SPI_execute(
       "CREATE SCHEMA IF NOT EXISTS dbblue",
       false, 0);

   if (spi_ret < 0)
   {
       elog(WARNING, "dbblue_audit: failed to create dbblue schema: %d", spi_ret);
       SPI_finish();
       return;
   }

   spi_ret = SPI_execute(
       "CREATE TABLE IF NOT EXISTS dbblue.dbblue_audit_log ("
       "    id          BIGSERIAL    PRIMARY KEY,"
       "    rel_name    TEXT         NOT NULL,"
       "    dml_op      TEXT         NOT NULL,"
       "    old_data    JSONB,"
       "    new_data    JSONB,"
       "    changed_by  TEXT         NOT NULL,"
       "    session_usr TEXT         NOT NULL,"
       "    client_addr TEXT,"
       "    logged_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),"
       /*
        * Odoo writes the acting user into every table as create_uid /
        * write_uid.  Those are res_users ids; the human-readable name lives
        * one join further on, in res_partner.  We record both the id and
        * the name resolved at the time of the change, so an audit row stays
        * meaningful even if the user is later renamed or removed.
        */
       "    odoo_create_uid INTEGER,"
       "    odoo_write_uid  INTEGER,"
       "    odoo_created_by TEXT,"
       "    odoo_changed_by TEXT"
       ")",
       false, 0);

   if (spi_ret < 0)
   {
       elog(WARNING, "dbblue_audit: failed to create dbblue_audit_log table: %d", spi_ret);
       SPI_finish();
       return;
   }

   /*
    * A log table created by an older dbblue lacks the four columns above.
    * ADD COLUMN IF NOT EXISTS is a no-op on a table that already has them.
    */
   spi_ret = SPI_execute(
       "ALTER TABLE dbblue.dbblue_audit_log"
       "  ADD COLUMN IF NOT EXISTS odoo_create_uid INTEGER,"
       "  ADD COLUMN IF NOT EXISTS odoo_write_uid  INTEGER,"
       "  ADD COLUMN IF NOT EXISTS odoo_created_by TEXT,"
       "  ADD COLUMN IF NOT EXISTS odoo_changed_by TEXT",
       false, 0);

   if (spi_ret < 0)
       elog(WARNING, "dbblue_audit: failed to add Odoo user columns: %d", spi_ret);

   /*
    * The retention sweep filters on logged_at, and reports usually do too;
    * without this index both degrade into a seq scan of the whole trail.
    */
   spi_ret = SPI_execute(
       "CREATE INDEX IF NOT EXISTS dbblue_audit_log_logged_at_idx"
       "  ON dbblue.dbblue_audit_log (logged_at)",
       false, 0);

   if (spi_ret < 0)
       elog(WARNING, "dbblue_audit: could not create logged_at index: %d", spi_ret);

   /*
    * Nobody but the owner gets any privilege on the audit trail.
    *
    * Audit rows are not written by the invoking user -- dbblue_audit_write()
    * switches to the bootstrap superuser for the insert -- so no grant to
    * PUBLIC is needed to make auditing work.  Granting one would be actively
    * harmful: INSERT to PUBLIC lets any role fabricate entries naming
    * somebody else, and SELECT to PUBLIC exposes old_data/new_data for every
    * audited table to roles that cannot query those tables at all.
    *
    * A DBA who wants someone to read the trail grants it explicitly:
    *     GRANT USAGE ON SCHEMA dbblue TO auditor;
    *     GRANT SELECT ON dbblue.dbblue_audit_log TO auditor;
    */
   spi_ret = SPI_execute(
       "REVOKE ALL ON dbblue.dbblue_audit_log FROM public",
       false, 0);

   if (spi_ret >= 0)
       spi_ret = SPI_execute("REVOKE ALL ON SCHEMA dbblue FROM public",
                             false, 0);

   if (spi_ret >= 0)
       spi_ret = SPI_execute(
           "REVOKE ALL ON ALL SEQUENCES IN SCHEMA dbblue FROM public",
           false, 0);

   if (spi_ret < 0)
       elog(WARNING, "dbblue_audit: failed to set table permissions: %d", spi_ret);
   else
   {
       /*
        * Mark it done for this session only after success.
        * CommandCounterIncrement makes the new table visible
        * to subsequent SPI calls in the same transaction.
        */
       CommandCounterIncrement();
       audit_table_checked = true;
       elog(DEBUG1, "dbblue_audit: dbblue_audit_log table ensured");
   }


   SPI_finish();
}


/* ================================================================
 * dbblue_audit_database
 *
 * Restricts auditing to named databases.  Empty (the default) means every
 * database in the cluster, which is how the feature behaved before this
 * existed.
 *
 * Note that PostgreSQL can already do this without any help --
 * "ALTER DATABASE odoo SET dbblue_audit_enabled = on" scopes any of these
 * settings to one database, and ALTER ROLE ... IN DATABASE goes finer
 * still.  This setting exists because it states the intent in one place
 * that survives a dump and reload of the settings, and because it matches
 * the shape of dbblue_repack_database and
 * dbblue_auto_index_suggestion_database.
 *
 * Precedence, since two mechanisms can now disagree: this is an additional
 * filter, never an override.  A database excluded here is not audited even
 * if ALTER DATABASE turned dbblue_audit_enabled on for it; a database
 * listed here is still not audited if dbblue_audit_enabled is off or
 * dbblue_audit_tables is empty.  Every condition must hold.
 *
 * The answer cannot change within a backend -- a session's database is
 * fixed -- so it is computed once and only recomputed if the setting
 * itself changes.
 * ================================================================ */
static int audit_db_match = -1;     /* -1 unknown, 0 no, 1 yes */

void
dbblue_assign_audit_database(const char *newval, void *extra)
{
   audit_db_match = -1;
}

static bool
audit_database_matches(void)
{
   char       *dbname;
   char       *rawstring;
   List       *elemlist;
   ListCell   *lc;

   if (audit_db_match >= 0)
       return audit_db_match == 1;

   if (dbblue_audit_database == NULL || dbblue_audit_database[0] == '\0')
   {
       audit_db_match = 1;
       return true;
   }

   /* Not connected to a database yet; decide later. */
   if (!OidIsValid(MyDatabaseId))
       return false;

   dbname = get_database_name(MyDatabaseId);
   if (dbname == NULL)
       return false;

   audit_db_match = 0;

   rawstring = pstrdup(dbblue_audit_database);
   if (SplitIdentifierString(rawstring, ',', &elemlist))
   {
       foreach(lc, elemlist)
       {
           if (strcmp((const char *) lfirst(lc), dbname) == 0)
           {
               audit_db_match = 1;
               break;
           }
       }
   }
   pfree(rawstring);
   list_free(elemlist);
   pfree(dbname);

   return audit_db_match == 1;
}

/* ----------------------------------------------------------------
 * dbblue_audit_active
 *
 * The cheapest possible "is there anything to do?" test, meant to be the
 * first thing every capture entry point asks.
 *
 * It matters because these entry points are called once per modified row
 * for every DML statement in the system, audited or not.  Anything done
 * before this returns false -- a syscache lookup for the schema name, a
 * pstrdup of a GUC, building a List -- is paid by every row of every
 * UPDATE and DELETE on the instance, including the overwhelmingly common
 * case where nothing is configured for auditing at all.
 * ---------------------------------------------------------------- */
static inline bool
dbblue_audit_active(int op)
{
   if (audit_in_progress)
       return false;
   if (!dbblue_audit_enabled)
       return false;
   if (dbblue_audit_tables == NULL || dbblue_audit_tables[0] == '\0')
       return false;
   if (!dbblue_audit_operation_is_tracked(op))
       return false;
   return audit_database_matches();
}

/* ----------------------------------------------------------------
* dbblue_audit_table_is_tracked
*
* Returns true if table_name appears in the comma-separated
* dbblue_audit_tables GUC value.
*
* Always returns false for the audit log table itself to prevent
* recursive audit logging.
* ---------------------------------------------------------------- */
bool
dbblue_audit_table_is_tracked(Relation rel, const char **schema_out)
{
   const char *table_name = RelationGetRelationName(rel);
   const char *schema_name = NULL;     /* resolved only if actually needed */
   char       *rawstring;
   List       *elemlist;
   ListCell   *lc;
   bool        tracked = false;

   if (!dbblue_audit_enabled)
       return false;
   if (dbblue_audit_tables == NULL || dbblue_audit_tables[0] == '\0')
       return false;

   /* Never audit the audit table itself */
   if (strcmp(table_name, "dbblue_audit_log") == 0)
       return false;

   rawstring = pstrdup(dbblue_audit_tables);
   if (!SplitIdentifierString(rawstring, ',', &elemlist))
   {
       pfree(rawstring);
       list_free(elemlist);
       return false;
   }

   /*
    * An entry may be written either qualified ("public.orders") or bare
    * ("orders").  A qualified entry matches only that schema; a bare one
    * matches the table in any schema, which is what entries written before
    * schema support meant, so existing configurations keep working.
    */
   foreach(lc, elemlist)
   {
       const char *item = (const char *) lfirst(lc);
       const char *dot = strchr(item, '.');

       if (dot != NULL)
       {
           size_t      slen = (size_t) (dot - item);

           /*
            * Only a schema-qualified entry needs the namespace, and
            * resolving it costs a syscache lookup plus a palloc.  Defer it
            * until one is actually seen, so a configuration written with
            * bare names never pays for it.
            */
           if (schema_name == NULL)
               schema_name = get_namespace_name(RelationGetNamespace(rel));

           if (schema_name != NULL &&
               strlen(schema_name) == slen &&
               strncmp(item, schema_name, slen) == 0 &&
               strcmp(dot + 1, table_name) == 0)
           {
               tracked = true;
               break;
           }
       }
       else if (strcmp(item, table_name) == 0)
       {
           tracked = true;
           break;
       }
   }

   pfree(rawstring);
   list_free(elemlist);

   if (tracked && schema_out != NULL)
   {
       if (schema_name == NULL)
           schema_name = get_namespace_name(RelationGetNamespace(rel));
       *schema_out = schema_name;
   }
   return tracked;
}


/* ================================================================
 * dbblue_audit_exclude_columns, parsed
 *
 * The list used to be re-parsed for every column of every audited row --
 * a pstrdup, a SplitIdentifierString and a List walk, 61 times per row on
 * an Odoo-width table.  Turning on redaction, the security feature, nearly
 * doubled the cost of auditing.  It is parsed once now and rebuilt only
 * when the setting changes.
 *
 * Parsing is done here rather than with SplitIdentifierString because that
 * folds unquoted text to lower case *and discards the quotes*, which left
 * no way at all to name a column created as "MixedCase".  These entries
 * follow ordinary SQL identifier rules instead: unquoted components fold to
 * lower case, a double-quoted component keeps its case verbatim.
 * ================================================================ */
typedef struct AuditExcludeEntry
{
   char       *schema;         /* NULL when the entry is unqualified */
   char       *table;          /* "*" matches every audited table */
   char       *column;
   bool        matched;        /* has it ever redacted anything */
   bool        validated;      /* has its column been checked to exist */
} AuditExcludeEntry;

static AuditExcludeEntry *exclude_entries = NULL;
static int  exclude_nentries = 0;
static bool exclude_cache_valid = false;

/*
 * Copy one dotted component, applying SQL identifier folding: "Quoted"
 * keeps its case, anything else is lower-cased.  Returns NULL for an empty
 * component.
 */
static char *
audit_fold_component(const char *start, const char *end)
{
   size_t      len;
   char       *out;
   size_t      i;

   while (start < end && (*start == ' ' || *start == '\t'))
       start++;
   while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
       end--;

   if (start >= end)
       return NULL;

   if (*start == '"' && end[-1] == '"' && end - start >= 2)
   {
       start++;
       end--;
       len = (size_t) (end - start);
       out = (char *) MemoryContextAlloc(TopMemoryContext, len + 1);
       memcpy(out, start, len);
       out[len] = '\0';
       return out;            /* quoted: verbatim */
   }

   len = (size_t) (end - start);
   out = (char *) MemoryContextAlloc(TopMemoryContext, len + 1);
   for (i = 0; i < len; i++)
       out[i] = pg_tolower((unsigned char) start[i]);
   out[len] = '\0';
   return out;
}

static void
audit_exclude_reset(void)
{
   int         i;

   for (i = 0; i < exclude_nentries; i++)
   {
       if (exclude_entries[i].schema)
           pfree(exclude_entries[i].schema);
       pfree(exclude_entries[i].table);
       pfree(exclude_entries[i].column);
   }
   if (exclude_entries)
       pfree(exclude_entries);
   exclude_entries = NULL;
   exclude_nentries = 0;
}

/*
 * Rebuild the parsed list if the setting has changed since last time.
 */
static void
audit_exclude_refresh(void)
{
   const char *p;
   int         capacity;

   if (exclude_cache_valid)
       return;

   audit_exclude_reset();
   exclude_cache_valid = true;

   if (dbblue_audit_exclude_columns == NULL ||
       dbblue_audit_exclude_columns[0] == '\0')
       return;

   /* One entry per comma at most, plus one. */
   capacity = 1;
   for (p = dbblue_audit_exclude_columns; *p; p++)
       if (*p == ',')
           capacity++;
   exclude_entries = (AuditExcludeEntry *)
       MemoryContextAllocZero(TopMemoryContext,
                              capacity * sizeof(AuditExcludeEntry));

   p = dbblue_audit_exclude_columns;
   while (*p != '\0')
   {
       const char *item_start = p;
       const char *dots[8];
       int         ndots = 0;
       bool        inquote = false;
       const char *comp[3];
       const char *compend[3];
       int         ncomp;
       int         i;

       /* find the end of this comma-separated item, respecting quotes */
       while (*p != '\0' && (inquote || *p != ','))
       {
           if (*p == '"')
               inquote = !inquote;
           else if (*p == '.' && !inquote && ndots < 8)
               dots[ndots++] = p;
           p++;
       }

       /* split into at most three components on the recorded dots */
       ncomp = ndots + 1;
       if (ncomp > 3)
       {
           /* too many parts to be a valid entry; skip it */
           if (*p == ',')
               p++;
           continue;
       }
       comp[0] = item_start;
       for (i = 0; i < ndots; i++)
       {
           compend[i] = dots[i];
           comp[i + 1] = dots[i] + 1;
       }
       compend[ncomp - 1] = p;

       if (ncomp >= 2)
       {
           AuditExcludeEntry *e = &exclude_entries[exclude_nentries];
           char       *a = audit_fold_component(comp[0], compend[0]);
           char       *b = audit_fold_component(comp[1], compend[1]);
           char       *c = (ncomp == 3)
               ? audit_fold_component(comp[2], compend[2]) : NULL;

           if (ncomp == 2 && a && b)
           {
               e->schema = NULL;
               e->table = a;
               e->column = b;
               exclude_nentries++;
           }
           else if (ncomp == 3 && a && b && c)
           {
               e->schema = a;
               e->table = b;
               e->column = c;
               exclude_nentries++;
           }
           else
           {
               if (a) pfree(a);
               if (b) pfree(b);
               if (c) pfree(c);
           }
       }

       if (*p == ',')
           p++;
   }
}

/* GUC assign hook: the parsed list is stale as soon as the text changes. */
void
dbblue_assign_audit_exclude_columns(const char *newval, void *extra)
{
   exclude_cache_valid = false;
}

/* ----------------------------------------------------------------
 * dbblue_audit_column_is_excluded
 *
 * True when this column's value must not be written to the audit log.
 *
 * dbblue_audit_exclude_columns holds entries of the form "table.column",
 * "schema.table.column" or "*.column".  A "*" table matches every audited
 * table, which is how a column name that is sensitive everywhere (say
 * "password") is redacted in one entry.
 * ---------------------------------------------------------------- */
static bool
dbblue_audit_column_is_excluded(const char *schema_name,
                                const char *table_name,
                                const char *column_name)
{
   int         i;

   audit_exclude_refresh();

   for (i = 0; i < exclude_nentries; i++)
   {
       AuditExcludeEntry *e = &exclude_entries[i];

       if (strcmp(e->column, column_name) != 0)
           continue;
       if (e->schema != NULL &&
           (schema_name == NULL || strcmp(e->schema, schema_name) != 0))
           continue;
       if (strcmp(e->table, "*") != 0 && strcmp(e->table, table_name) != 0)
           continue;

       e->matched = true;
       return true;
   }

   return false;
}

/* ----------------------------------------------------------------
 * audit_exclude_check_table
 *
 * Warn about an exclude entry that names this table explicitly but whose
 * column does not exist on it.
 *
 * Without this the failure is silent: an operator writes
 * "users.password_hsah", sees no error, and believes the column is
 * redacted.  For a setting whose whole job is keeping secrets out of the
 * log, silently doing nothing is the worst possible outcome, so say so
 * once per entry per backend.
 *
 * Entries with a "*" table are skipped -- they are meant to apply only
 * where the column happens to exist, so absence is not a mistake.
 * ---------------------------------------------------------------- */
static void
audit_exclude_check_table(TupleDesc tupdesc, const char *schema_name,
                          const char *table_name)
{
   int         i;

   for (i = 0; i < exclude_nentries; i++)
   {
       AuditExcludeEntry *e = &exclude_entries[i];
       bool        found = false;
       int         j;

       if (e->validated)
           continue;
       if (strcmp(e->table, "*") == 0)
           continue;
       if (strcmp(e->table, table_name) != 0)
           continue;
       if (e->schema != NULL &&
           (schema_name == NULL || strcmp(e->schema, schema_name) != 0))
           continue;

       for (j = 0; j < tupdesc->natts && !found; j++)
       {
           Form_pg_attribute att = TupleDescAttr(tupdesc, j);

           if (!att->attisdropped &&
               strcmp(NameStr(att->attname), e->column) == 0)
               found = true;
       }

       e->validated = true;

       if (!found)
           ereport(WARNING,
                   (errmsg("dbblue_audit_exclude_columns names \"%s\" on table \"%s\", which has no such column",
                           e->column, table_name),
                    errdetail("That column is not being redacted."),
                    errhint("Column names are matched as SQL identifiers: an unquoted name is folded to lower case, so a column created as \"MixedCase\" must be written that way, in double quotes.")));
   }
}


/* ----------------------------------------------------------------
 * audit_value_unchanged
 *
 * True when a column holds the same value in both images of an UPDATE.
 *
 * Compared with the type's own equality operator rather than a raw byte
 * comparison, so a value that is merely stored differently -- detoasted
 * against toasted, or a numeric with a different but equal
 * representation -- is correctly seen as unchanged.  A type with no
 * equality operator is reported as changed, which errs towards recording
 * too much rather than too little.
 * ---------------------------------------------------------------- */
static bool
audit_value_unchanged(Form_pg_attribute att,
                      Datum oldval, bool oldnull,
                      Datum newval, bool newnull)
{
   TypeCacheEntry *typentry;

   if (oldnull && newnull)
       return true;
   if (oldnull || newnull)
       return false;

   typentry = lookup_type_cache(att->atttypid, TYPECACHE_EQ_OPR_FINFO);
   if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
       return false;

   return DatumGetBool(FunctionCall2Coll(&typentry->eq_opr_finfo,
                                         att->attcollation,
                                         oldval, newval));
}

/* ----------------------------------------------------------------
* dbblue_audit_tuple_to_json
*
* Converts a HeapTuple to a JSON string using the row's TupleDesc.
* Returns NULL if tuple is NULL.
* ---------------------------------------------------------------- */
static char *
dbblue_audit_tuple_to_json(HeapTuple tuple, HeapTuple other,
                           Bitmapset *keepcols, TupleDesc tupdesc,
                           const char *schema_name,
                           const char *table_name)
{
   StringInfoData buf;
   int            i;
   bool           first = true;


   if (tuple == NULL)
       return NULL;


   /*
    * Cheap when nothing is configured, and each entry is only ever
    * examined once per backend.
    */
   audit_exclude_refresh();
   if (exclude_nentries > 0)
       audit_exclude_check_table(tupdesc, schema_name, table_name);

   initStringInfo(&buf);
   appendStringInfoChar(&buf, '{');


   for (i = 0; i < tupdesc->natts; i++)
   {
       Form_pg_attribute att = TupleDescAttr(tupdesc, i);
       bool   isnull;
       Datum  val;
       Oid    typoutput;
       bool   typisvarlena;
       char  *valstr;


       /* skip dropped columns */
       if (att->attisdropped)
           continue;


       val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

       /*
        * In changed-columns-only mode, drop a column whose value is the
        * same in both images of an UPDATE.  Key columns are always kept:
        * without them an audit row records that something changed but not
        * which row it was.
        */
       if (other != NULL)
       {
           bool        othernull;
           Datum       otherval = heap_getattr(other, i + 1, tupdesc,
                                               &othernull);

           if (!bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
                              keepcols) &&
               audit_value_unchanged(att, val, isnull, otherval, othernull))
               continue;
       }

       if (!first)
           appendStringInfoChar(&buf, ',');
       first = false;

       /*
        * A column the operator marked sensitive keeps its key -- so the
        * audit row still shows the table's shape and that the column
        * exists -- but its value never reaches the log.
        *
        * Note the mask is a constant, so an excluded column reads the same
        * before and after a change: you cannot tell from the audit row
        * whether it was modified.  That is the point of excluding it.
        */
       if (dbblue_audit_column_is_excluded(schema_name, table_name,
                                           NameStr(att->attname)))
       {
           escape_json(&buf, NameStr(att->attname));
           appendStringInfoChar(&buf, ':');
           escape_json(&buf, "***");
           continue;
       }


       /*
        * Column name and value are emitted through escape_json() so that
        * quotes, backslashes and control characters (newlines, tabs, etc.)
        * are properly escaped.  Building the JSON by hand previously produced
        * invalid JSON for any value containing a control character, which
        * made the JSONB cast in the INSERT fail and silently dropped the
        * audit row.
        */
       escape_json(&buf, NameStr(att->attname));
       appendStringInfoChar(&buf, ':');


       if (isnull)
       {
           appendStringInfoString(&buf, "null");
       }
       else
       {
           getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena);
           valstr = OidOutputFunctionCall(typoutput, val);
           escape_json(&buf, valstr);
       }
   }


   appendStringInfoChar(&buf, '}');
   return buf.data;
}





/* ================================================================
 * Odoo actor resolution
 *
 * Odoo stamps every row it manages with create_uid and write_uid, both
 * res_users ids.  The readable name is one join further on:
 *
 *     <audited table>.write_uid -> res_users.id
 *     res_users.partner_id      -> res_partner.id
 *     res_partner.name          -> the name we want
 *
 * Resolution is skipped entirely on a database that has no res_users /
 * res_partner (a non-Odoo database, or an audited table that has no such
 * columns), so the audit log stays usable outside Odoo.
 * ================================================================ */

/* -1 = not probed yet, 0 = this database is not an Odoo database, 1 = it is */
static int odoo_schema_present = -1;

/* uid -> partner name, for this backend.  See the note on staleness below. */
typedef struct OdooActorEntry
{
   int32   uid;                /* hash key */
   char   *name;               /* TopMemoryContext; NULL if unresolvable */
} OdooActorEntry;

static HTAB *odoo_actor_cache = NULL;

/* ----------------------------------------------------------------
 * audit_get_int_attr
 *
 * Read a named int4 column out of a tuple.  Returns false when the table
 * has no such column, when it is dropped, when it is not an integer, or
 * when the value is NULL -- all of which simply mean "no actor recorded".
 * ---------------------------------------------------------------- */
static bool
audit_get_int_attr(HeapTuple tup, TupleDesc tupdesc, const char *attname,
                  int32 *result)
{
   int         i;

   if (tup == NULL || tupdesc == NULL)
       return false;

   for (i = 0; i < tupdesc->natts; i++)
   {
       Form_pg_attribute att = TupleDescAttr(tupdesc, i);
       Datum       value;
       bool        isnull;

       if (att->attisdropped)
           continue;
       if (strcmp(NameStr(att->attname), attname) != 0)
           continue;
       if (att->atttypid != INT4OID)
           return false;

       value = heap_getattr(tup, i + 1, tupdesc, &isnull);
       if (isnull)
           return false;

       *result = DatumGetInt32(value);
       return true;
   }

   return false;
}

/* ----------------------------------------------------------------
 * odoo_schema_is_present
 *
 * One probe per backend for the tables and columns the join needs.  Doing
 * this up front means the lookup below can never fail on a missing
 * relation or column, which matters because an error raised here would
 * propagate out of the audit path and abort the user's transaction.
 *
 * Caller must hold an SPI connection.
 * ---------------------------------------------------------------- */
static bool
odoo_schema_is_present(void)
{
   int         spi_ret;
   bool        isnull;
   Datum       d;

   if (odoo_schema_present >= 0)
       return odoo_schema_present == 1;

   spi_ret = SPI_execute(
       "SELECT to_regclass('res_users') IS NOT NULL"
       "   AND to_regclass('res_partner') IS NOT NULL"
       "   AND EXISTS (SELECT 1 FROM pg_attribute"
       "                WHERE attrelid = to_regclass('res_users')"
       "                  AND attname = 'partner_id' AND NOT attisdropped)"
       "   AND EXISTS (SELECT 1 FROM pg_attribute"
       "                WHERE attrelid = to_regclass('res_partner')"
       "                  AND attname = 'name' AND NOT attisdropped)"
       /*
        * And that this role may actually read them.  The lookup runs as
        * whoever ran the statement; without this check a role lacking
        * SELECT on res_users would get "permission denied" raised from
        * inside the audit path, aborting its own DML rather than merely
        * leaving the actor name unresolved.
        */
       "   AND has_table_privilege(to_regclass('res_users'), 'SELECT')"
       "   AND has_table_privilege(to_regclass('res_partner'), 'SELECT')",
       true, 1);

   if (spi_ret != SPI_OK_SELECT || SPI_processed != 1)
   {
       odoo_schema_present = 0;
       return false;
   }

   d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
   odoo_schema_present = (!isnull && DatumGetBool(d)) ? 1 : 0;
   return odoo_schema_present == 1;
}

/* ----------------------------------------------------------------
 * odoo_actor_name
 *
 * Resolve a res_users id to its partner name, or NULL when it cannot be
 * resolved.  The result is cached for the life of the backend: the audit
 * path already pays for one SPI INSERT per row, and re-running this join
 * per row would roughly double that on a bulk update.
 *
 * The cache means a partner renamed during a long-lived session keeps its
 * old name in audit rows written by that session.  For an audit trail
 * that is the lesser evil -- and arguably the right answer, since the row
 * records who acted at the time.
 *
 * Caller must hold an SPI connection.
 * ---------------------------------------------------------------- */
static const char *
odoo_actor_name(int32 uid)
{
   OdooActorEntry *entry;
   bool        found;
   int         spi_ret;
   char        sql[256];
   char       *name = NULL;

   if (!odoo_schema_is_present())
       return NULL;

   if (odoo_actor_cache == NULL)
   {
       HASHCTL     ctl;

       memset(&ctl, 0, sizeof(ctl));
       ctl.keysize = sizeof(int32);
       ctl.entrysize = sizeof(OdooActorEntry);
       ctl.hcxt = TopMemoryContext;
       odoo_actor_cache = hash_create("dbblue audit odoo actors", 32, &ctl,
                                      HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
   }

   entry = (OdooActorEntry *) hash_search(odoo_actor_cache, &uid,
                                          HASH_ENTER, &found);
   if (found)
       return entry->name;

   entry->name = NULL;         /* so a failure below is remembered too */

   snprintf(sql, sizeof(sql),
            "SELECT p.name FROM res_users u"
            "  JOIN res_partner p ON p.id = u.partner_id"
            " WHERE u.id = %d",
            uid);

   spi_ret = SPI_execute(sql, true, 1);
   if (spi_ret == SPI_OK_SELECT && SPI_processed == 1)
   {
       bool        isnull;
       char       *val;

       val = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
       isnull = (val == NULL);
       if (!isnull)
       {
           MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

           name = pstrdup(val);
           MemoryContextSwitchTo(oldcxt);
           pfree(val);
       }
   }

   entry->name = name;
   return entry->name;
}

/* ----------------------------------------------------------------
* dbblue_audit_write
*
* Inserts one row into dbblue_audit_log using SPI.
* Called once per audited row.
* ---------------------------------------------------------------- */
void
dbblue_audit_write(Relation rel,
                const char *schema_name,
                const char *table_name,
                const char *operation,
                HeapTuple   old_tuple,
                HeapTuple   new_tuple,
                TupleDesc   tupdesc)
{
   char       *old_json  = NULL;
   char       *new_json  = NULL;
   const char *username;
   const char *sess_user;
   char        client_addr[64] = "local";
   HeapTuple   actor_tuple;
   int32       create_uid = 0;
   int32       write_uid = 0;
   bool        have_create_uid;
   bool        have_write_uid;
   AuditPriv   priv;
   MemoryContext oldcxt;
   ResourceOwner oldowner;
   volatile bool wrote = false;
   Bitmapset  *keepcols = NULL;
   HeapTuple   counterpart_old = NULL;
   HeapTuple   counterpart_new = NULL;


   /* Auto-create the audit log table if it doesn't exist yet */
   dbblue_audit_ensure_log_table();

   /*
    * If it could not be made available, skip quietly.  The warning was
    * already emitted once by ensure_log_table; raising here instead would
    * abort the statement the user was actually running.
    */
   if (!audit_table_available)
       return;

   dbblue_audit_maybe_prune();


   /* Build JSON for old/new rows */
   /*
    * Changed-columns-only applies to UPDATE alone: it needs two images to
    * compare, and for INSERT or DELETE every column is meaningful anyway.
    *
    * Key columns are always emitted so an audit row still identifies the
    * row it describes.  A table with no primary key has nothing to
    * identify it by, so filtering is skipped entirely there rather than
    * producing rows nobody can trace back.
    */
   if (dbblue_audit_changed_columns_only &&
       old_tuple != NULL && new_tuple != NULL)
   {
       keepcols = RelationGetIndexAttrBitmap(rel,
                                             INDEX_ATTR_BITMAP_PRIMARY_KEY);
       if (keepcols != NULL)
           counterpart_old = new_tuple, counterpart_new = old_tuple;
   }

   old_json = dbblue_audit_tuple_to_json(old_tuple, counterpart_old, keepcols,
                                         tupdesc, schema_name, table_name);
   new_json = dbblue_audit_tuple_to_json(new_tuple, counterpart_new, keepcols,
                                         tupdesc, schema_name, table_name);


   username  = GetUserNameFromId(GetUserId(), false);
   sess_user = GetUserNameFromId(GetSessionUserId(), false);


   if (MyProcPort && MyProcPort->remote_host)
       snprintf(client_addr, sizeof(client_addr), "%s",
                MyProcPort->remote_host);


   /*
    * Read Odoo's actor columns off the row itself.  Prefer the post-image:
    * for an INSERT or UPDATE it carries the current write_uid, and for a
    * DELETE there is no post-image so the pre-image is the last state the
    * row was in.  A table without these columns simply yields nothing.
    */
   actor_tuple = (new_tuple != NULL) ? new_tuple : old_tuple;
   have_create_uid = audit_get_int_attr(actor_tuple, tupdesc, "create_uid",
                                        &create_uid);
   have_write_uid = audit_get_int_attr(actor_tuple, tupdesc, "write_uid",
                                       &write_uid);

   /*
    * The write runs in its own subtransaction so that a failure inside it --
    * the log table dropped by another session, its tablespace full, a
    * constraint violation -- is contained here instead of propagating out
    * and aborting the user's own UPDATE or DELETE.  Without it the audit
    * subsystem could take down legitimate application traffic.
    *
    * dbblue_audit_require_write chooses what "contained" means: by default
    * the failure is reported as a WARNING and the change proceeds
    * unaudited, which keeps the application available.  Turn it on where
    * an unaudited change is worse than a failed one and the error is
    * re-raised instead.
    *
    * Everything inside also runs as the bootstrap superuser.  The identity
    * columns were read from the real user just above, so the row still
    * records who actually made the change.
    */
   oldcxt = CurrentMemoryContext;
   oldowner = CurrentResourceOwner;

   /*
    * Containment costs a subtransaction, and a subtransaction per audited
    * row overflows this backend's subxid cache on any bulk write (the cache
    * holds 64).  Once overflowed, every other backend has to consult
    * pg_subtrans for visibility checks against this transaction -- a
    * cluster-wide penalty far worse than the local cost of the audit
    * itself.  So only pay for it in the mode that needs it.
    */
   if (dbblue_audit_require_write)
   {
       /*
        * Fail closed.  Nothing to contain: a failure here must reject the
        * change, which is exactly what letting the error propagate does.
        */
       audit_priv_enter(&priv);
       PG_TRY();
       {
           audit_do_insert(table_name, operation, old_json, new_json,
                           username, sess_user, client_addr,
                           have_create_uid, create_uid,
                           have_write_uid, write_uid);
       }
       PG_FINALLY();
       {
           audit_priv_leave(&priv);
       }
       PG_END_TRY();
       return;
   }

   BeginInternalSubTransaction(NULL);
   audit_priv_enter(&priv);

   PG_TRY();
   {
       audit_do_insert(table_name, operation, old_json, new_json,
                       username, sess_user, client_addr,
                       have_create_uid, create_uid,
                       have_write_uid, write_uid);

       audit_priv_leave(&priv);
       ReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;
       wrote = true;
   }
   PG_CATCH();
   {
       ErrorData  *edata;

       audit_priv_leave(&priv);
       MemoryContextSwitchTo(oldcxt);
       edata = CopyErrorData();
       FlushErrorState();
       RollbackAndReleaseCurrentSubTransaction();
       MemoryContextSwitchTo(oldcxt);
       CurrentResourceOwner = oldowner;

       /*
        * Whatever went wrong may have been a stale cached plan or a
        * vanished log table, so re-check both on the next write instead of
        * failing identically forever.
        */
       audit_forget_plan();
       audit_table_checked = false;

       if (dbblue_audit_require_write)
       {
           /*
            * Fail closed: the change must not be allowed to happen without
            * a record of it.  Re-raise with the original message as detail
            * so the cause is not lost.
            */
           ereport(ERROR,
                   (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("dbblue_audit: could not record the change to \"%s\"",
                           table_name),
                    errdetail("%s", edata->message),
                    errhint("Set dbblue_audit_require_write to off to let unaudited changes proceed.")));
       }

       ereport(WARNING,
               (errmsg("dbblue_audit: could not record the change to \"%s\": %s",
                       table_name, edata->message),
                errdetail("The change itself was applied; this audit row is missing."),
                errhint("Set dbblue_audit_require_write to on to reject changes that cannot be audited.")));
       FreeErrorData(edata);
   }
   PG_END_TRY();

   (void) wrote;
}
/* ----------------------------------------------------------------
* dbblue_audit_capture_update
*
* Records a single UPDATEd row.  Called from ExecUpdateEpilogue() in the
* executor, once per row that was actually updated, so it captures every
* affected row (not just the last one, which was the fatal flaw of the old
* ExecutorFinish-hook approach that read the reusable projection slots after
* the whole statement had finished).
*
* oldslot holds this row's pre-image (resultRelInfo->ri_oldTupleSlot, which
* the executor fills before applying the update, in both the plain UPDATE and
* the MERGE paths).  newslot holds the post-image.
* ---------------------------------------------------------------- */
void
dbblue_audit_capture_update(ResultRelInfo *rri,
                            TupleTableSlot *oldslot,
                            TupleTableSlot *newslot)
{
   Relation    rel;
   const char *relname;
   const char *nspname;
   TupleDesc   tupdesc;
   HeapTuple   oldtup;
   HeapTuple   newtup;


   if (!dbblue_audit_active(DBBLUE_AUDIT_UPDATE))
       return;
   if (oldslot == NULL || newslot == NULL)
       return;


   rel = rri->ri_RelationDesc;
   if (!dbblue_audit_table_is_tracked(rel, &nspname))
       return;
   relname = RelationGetRelationName(rel);


   tupdesc = RelationGetDescr(rel);
   oldtup  = ExecFetchSlotHeapTuple(oldslot, false, NULL);
   newtup  = ExecFetchSlotHeapTuple(newslot, false, NULL);


   audit_in_progress = true;
   PG_TRY();
   {
       dbblue_audit_write(rel, nspname, relname, "UPDATE", oldtup, newtup,
                          tupdesc);
   }
   PG_FINALLY();
   {
       audit_in_progress = false;
   }
   PG_END_TRY();
}


/* ----------------------------------------------------------------
* dbblue_audit_capture_delete
*
* Records a single DELETEd row.  Called from ExecDeleteEpilogue() in the
* executor, once per row actually deleted.
*
* For a regular DELETE the executor does not materialise the old row, so
* oldtuple is NULL and only its TID (tupleid) is known; we fetch the row
* being deleted with SnapshotAny (the tuple is still present, just marked
* deleted by our own transaction), exactly as the AFTER ROW trigger machinery
* does.  For view/wholerow cases oldtuple is supplied directly.
* ---------------------------------------------------------------- */
void
dbblue_audit_capture_delete(ResultRelInfo *rri,
                            ItemPointer tupleid,
                            HeapTuple oldtuple)
{
   Relation        rel;
   const char     *relname;
   const char     *nspname;
   TupleDesc       tupdesc;
   TupleTableSlot *fetchslot = NULL;
   HeapTuple       oldtup    = oldtuple;


   if (!dbblue_audit_active(DBBLUE_AUDIT_DELETE))
       return;


   rel = rri->ri_RelationDesc;
   if (!dbblue_audit_table_is_tracked(rel, &nspname))
       return;
   relname = RelationGetRelationName(rel);


   tupdesc = RelationGetDescr(rel);


   /* Fetch the pre-image by TID when the caller did not supply it. */
   if (oldtup == NULL)
   {
       if (tupleid == NULL || !ItemPointerIsValid(tupleid))
           return;


       fetchslot = table_slot_create(rel, NULL);
       if (!table_tuple_fetch_row_version(rel, tupleid, SnapshotAny, fetchslot))
       {
           ExecDropSingleTupleTableSlot(fetchslot);
           return;
       }
       oldtup = ExecFetchSlotHeapTuple(fetchslot, false, NULL);
   }


   audit_in_progress = true;
   PG_TRY();
   {
       dbblue_audit_write(rel, nspname, relname, "DELETE", oldtup, NULL,
                          tupdesc);
   }
   PG_FINALLY();
   {
       audit_in_progress = false;
       if (fetchslot != NULL)
           ExecDropSingleTupleTableSlot(fetchslot);
   }
   PG_END_TRY();
}

/* ----------------------------------------------------------------
 * dbblue_audit_capture_insert
 *
 * Record a newly inserted row.  Called once per inserted row from
 * ExecInsert(), after the AFTER ROW INSERT triggers have run, so the row
 * is known to have survived constraints and triggers.
 *
 * There is no pre-image, so old_data is left NULL in the audit row; that
 * is what distinguishes an INSERT entry from an UPDATE one, which carries
 * both images.
 * ---------------------------------------------------------------- */
void
dbblue_audit_capture_insert(ResultRelInfo *rri, TupleTableSlot *newslot)
{
   Relation    rel;
   const char *relname;
   const char *nspname;
   TupleDesc   tupdesc;
   HeapTuple   newtup;


   if (!dbblue_audit_active(DBBLUE_AUDIT_INSERT))
       return;
   if (newslot == NULL)
       return;


   rel = rri->ri_RelationDesc;
   if (!dbblue_audit_table_is_tracked(rel, &nspname))
       return;
   relname = RelationGetRelationName(rel);


   tupdesc = RelationGetDescr(rel);
   newtup  = ExecFetchSlotHeapTuple(newslot, false, NULL);


   audit_in_progress = true;
   PG_TRY();
   {
       dbblue_audit_write(rel, nspname, relname, "INSERT", NULL, newtup,
                          tupdesc);
   }
   PG_FINALLY();
   {
       audit_in_progress = false;
   }
   PG_END_TRY();
}
