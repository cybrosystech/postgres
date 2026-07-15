#include "postgres.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
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
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_audit.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/* ----------------------------------------------------------------
* GUC variable definitions
* ---------------------------------------------------------------- */
bool  dbblue_audit_enabled = true;
char *dbblue_audit_tables  = NULL;


/* ----------------------------------------------------------------
* Reentrancy guard.
*
* dbblue_audit_write() issues an INSERT into dbblue_audit_log through SPI,
* which runs the executor again.  This guard makes sure that any DML issued
* while we are writing an audit row is never itself audited, so we can never
* recurse into the capture path.
* ---------------------------------------------------------------- */
static bool audit_in_progress = false;


/* ----------------------------------------------------------------
* Forward declarations
* ---------------------------------------------------------------- */
static char *dbblue_audit_tuple_to_json(HeapTuple tuple, TupleDesc tupdesc);








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
       audit_table_checked = false;
}


/* ----------------------------------------------------------------
* dbblue_audit_ensure_log_table
*
* Creates dbblue_audit_log if it does not already exist.
* Uses CREATE TABLE IF NOT EXISTS so it is safe to call multiple
* times — but the static flag above means we only ever call it
* once per backend session.
* ---------------------------------------------------------------- */
static void
dbblue_audit_ensure_log_table(void)
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
       "    logged_at   TIMESTAMPTZ  NOT NULL DEFAULT now()"
       ")",
       false, 0);

   if (spi_ret < 0)
   {
       elog(WARNING, "dbblue_audit: failed to create dbblue_audit_log table: %d", spi_ret);
       SPI_finish();
       return;
   }

   /*
    * Restrict permissions: only superusers can truncate, alter, or drop the
    * audit table. Regular users can only INSERT and SELECT.
    * This prevents accidental or malicious audit trail destruction.
    */
   spi_ret = SPI_execute(
       "REVOKE ALL ON dbblue.dbblue_audit_log FROM public",
       false, 0);

   if (spi_ret >= 0)
   {
       spi_ret = SPI_execute(
           "GRANT INSERT, SELECT ON dbblue.dbblue_audit_log TO public",
           false, 0);
   }

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
dbblue_audit_table_is_tracked(const char *table_name)
{
   char *rawlist;
   char *tok;
   char *saveptr;


   if (!dbblue_audit_enabled)
       return false;
   if (dbblue_audit_tables == NULL || dbblue_audit_tables[0] == '\0')
       return false;

   /* Never audit the audit table itself */
   if (strcmp(table_name, "dbblue_audit_log") == 0)
       return false;

   rawlist = pstrdup(dbblue_audit_tables);


   tok = strtok_r(rawlist, ",", &saveptr);
   while (tok != NULL)
   {
       char *end;   /* declared at top of block — C90 compliant */


       /* strip leading/trailing whitespace and quotes */
       while (*tok == ' ' || *tok == '"') tok++;
       end = tok + strlen(tok) - 1;
       while (end > tok && (*end == ' ' || *end == '"')) *end-- = '\0';


       if (strcmp(tok, table_name) == 0)
           return true;
       tok = strtok_r(NULL, ",", &saveptr);
   }
   return false;
}




/* ----------------------------------------------------------------
* dbblue_audit_tuple_to_json
*
* Converts a HeapTuple to a JSON string using the row's TupleDesc.
* Returns NULL if tuple is NULL.
* ---------------------------------------------------------------- */
static char *
dbblue_audit_tuple_to_json(HeapTuple tuple, TupleDesc tupdesc)
{
   StringInfoData buf;
   int            i;
   bool           first = true;


   if (tuple == NULL)
       return NULL;


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


       if (!first)
           appendStringInfoChar(&buf, ',');
       first = false;


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




/* ----------------------------------------------------------------
* dbblue_audit_write
*
* Inserts one row into dbblue_audit_log using SPI.
* Called once per audited row.
* ---------------------------------------------------------------- */
void
dbblue_audit_write(const char *table_name,
                const char *operation,
                HeapTuple   old_tuple,
                HeapTuple   new_tuple,
                TupleDesc   tupdesc)
{
   char       *old_json  = NULL;
   char       *new_json  = NULL;
   StringInfoData sql;
   int         spi_ret;
   const char *username;
   const char *sess_user;
   char        client_addr[64] = "local";


   /* Auto-create the audit log table if it doesn't exist yet */
   dbblue_audit_ensure_log_table();


   /* Build JSON for old/new rows */
   old_json = dbblue_audit_tuple_to_json(old_tuple, tupdesc);
   new_json = dbblue_audit_tuple_to_json(new_tuple, tupdesc);


   username  = GetUserNameFromId(GetUserId(), false);
   sess_user = GetUserNameFromId(GetSessionUserId(), false);


   if (MyProcPort && MyProcPort->remote_host)
       snprintf(client_addr, sizeof(client_addr), "%s",
                MyProcPort->remote_host);


   if (SPI_connect() != SPI_OK_CONNECT)
   {
       elog(WARNING, "dbblue_audit: SPI_connect failed");
       return;
   }


   initStringInfo(&sql);
   appendStringInfo(&sql,
       "INSERT INTO dbblue.dbblue_audit_log "
       "(rel_name, dml_op, old_data, new_data,"
       " changed_by, session_usr, client_addr, logged_at) "
       "VALUES (%s, %s, %s, %s, %s, %s, %s, now())",
       quote_literal_cstr(table_name),
       quote_literal_cstr(operation),
       old_json ? quote_literal_cstr(old_json) : "NULL",
       new_json ? quote_literal_cstr(new_json) : "NULL",
       quote_literal_cstr(username),
       quote_literal_cstr(sess_user),
       quote_literal_cstr(client_addr)
   );


   spi_ret = SPI_execute(sql.data, false, 0);
   if (spi_ret < 0)
       elog(WARNING, "dbblue_audit: failed to insert audit row: %d", spi_ret);


   SPI_finish();
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
   TupleDesc   tupdesc;
   HeapTuple   oldtup;
   HeapTuple   newtup;


   if (audit_in_progress)
       return;
   if (oldslot == NULL || newslot == NULL)
       return;


   rel     = rri->ri_RelationDesc;
   relname = RelationGetRelationName(rel);


   if (!dbblue_audit_table_is_tracked(relname))
       return;


   tupdesc = RelationGetDescr(rel);
   oldtup  = ExecFetchSlotHeapTuple(oldslot, false, NULL);
   newtup  = ExecFetchSlotHeapTuple(newslot, false, NULL);


   audit_in_progress = true;
   PG_TRY();
   {
       dbblue_audit_write(relname, "UPDATE", oldtup, newtup, tupdesc);
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
   TupleDesc       tupdesc;
   TupleTableSlot *fetchslot = NULL;
   HeapTuple       oldtup    = oldtuple;


   if (audit_in_progress)
       return;


   rel     = rri->ri_RelationDesc;
   relname = RelationGetRelationName(rel);


   if (!dbblue_audit_table_is_tracked(relname))
       return;


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
       dbblue_audit_write(relname, "DELETE", oldtup, NULL, tupdesc);
   }
   PG_FINALLY();
   {
       audit_in_progress = false;
       if (fetchslot != NULL)
           ExecDropSingleTupleTableSlot(fetchslot);
   }
   PG_END_TRY();
}
