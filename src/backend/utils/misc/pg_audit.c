#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
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
#include "executor/spi.h"
#include "libpq/libpq-be.h"    /* MyProcPort, for client addr */
#include "utils/pg_audit.h"

/* ----------------------------------------------------------------
* GUC variable definitions
* ---------------------------------------------------------------- */
bool  odoo_audit_enabled = true;
char *odoo_audit_tables  = NULL;


/* ----------------------------------------------------------------
* Saved previous hooks so we can chain them
* ---------------------------------------------------------------- */
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;


/* ----------------------------------------------------------------
* Forward declarations
* ---------------------------------------------------------------- */
static void odoo_audit_executor_finish(QueryDesc *queryDesc);
static void odoo_audit_process_relation(ResultRelInfo *rri,
                                       EState        *estate,
                                       const char    *operation);
static char *odoo_audit_tuple_to_json(HeapTuple tuple, TupleDesc tupdesc);








/* ----------------------------------------------------------------
* Static flag: have we already ensured the audit table exists
* this session? Avoids re-checking on every single DML operation.
* ---------------------------------------------------------------- */
static bool audit_table_checked = false;




/* ----------------------------------------------------------------
* odoo_audit_ensure_log_table
*
* Creates odoo_audit_log if it does not already exist.
* Uses CREATE TABLE IF NOT EXISTS so it is safe to call multiple
* times — but the static flag above means we only ever call it
* once per backend session.
* ---------------------------------------------------------------- */
static void
odoo_audit_ensure_log_table(void)
{
   int spi_ret;


   if (audit_table_checked)
       return;


   if (SPI_connect() != SPI_OK_CONNECT)
   {
       elog(WARNING, "odoo_audit: SPI_connect failed in ensure_log_table");
       return;
   }


   spi_ret = SPI_execute(
       "CREATE TABLE IF NOT EXISTS odoo_audit_log ("
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
       elog(WARNING, "odoo_audit: failed to create odoo_audit_log table: %d",
            spi_ret);
   else
   {
       /*
        * Mark it done for this session only after success.
        * CommandCounterIncrement makes the new table visible
        * to subsequent SPI calls in the same transaction.
        */
       CommandCounterIncrement();
       audit_table_checked = true;
       elog(DEBUG1, "odoo_audit: odoo_audit_log table ensured");
   }


   SPI_finish();
}
/* ----------------------------------------------------------------
* odoo_audit_table_is_tracked
*
* Returns true if table_name appears in the comma-separated
* odoo_audit_tables GUC value.
* ---------------------------------------------------------------- */
bool
odoo_audit_table_is_tracked(const char *table_name)
{
   char *rawlist;
   char *tok;
   char *saveptr;


   if (!odoo_audit_enabled)
       return false;
   if (odoo_audit_tables == NULL || odoo_audit_tables[0] == '\0')
       return false;


   rawlist = pstrdup(odoo_audit_tables);


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
* odoo_audit_tuple_to_json
*
* Converts a HeapTuple to a JSON string using the row's TupleDesc.
* Returns NULL if tuple is NULL.
* ---------------------------------------------------------------- */
static char *
odoo_audit_tuple_to_json(HeapTuple tuple, TupleDesc tupdesc)
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


       /* column name */
       appendStringInfo(&buf, "\"%s\":", NameStr(att->attname));


       if (isnull)
       {
           appendStringInfoString(&buf, "null");
       }
     else
       {
           char *p;   /* move to top */
           getTypeOutputInfo(att->atttypid, &typoutput, &typisvarlena);
           valstr = OidOutputFunctionCall(typoutput, val);
           appendStringInfoChar(&buf, '"');
           for (p = valstr; *p; p++)   /* p declared above, not here */
           {
               if (*p == '"' || *p == '\\')
                   appendStringInfoChar(&buf, '\\');
               appendStringInfoChar(&buf, *p);
           }
           appendStringInfoChar(&buf, '"');
       }
   }


   appendStringInfoChar(&buf, '}');
   return buf.data;
}




/* ----------------------------------------------------------------
* odoo_audit_write
*
* Inserts one row into odoo_audit_log using SPI.
* Called once per audited row.
* ---------------------------------------------------------------- */
void
odoo_audit_write(const char *table_name,
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
   odoo_audit_ensure_log_table();


   /* Build JSON for old/new rows */
   old_json = odoo_audit_tuple_to_json(old_tuple, tupdesc);
   new_json = odoo_audit_tuple_to_json(new_tuple, tupdesc);


   username  = GetUserNameFromId(GetUserId(), false);
   sess_user = GetUserNameFromId(GetSessionUserId(), false);


   if (MyProcPort && MyProcPort->remote_host)
       snprintf(client_addr, sizeof(client_addr), "%s",
                MyProcPort->remote_host);


   if (SPI_connect() != SPI_OK_CONNECT)
   {
       elog(WARNING, "odoo_audit: SPI_connect failed");
       return;
   }


   initStringInfo(&sql);
   appendStringInfo(&sql,
       "INSERT INTO odoo_audit_log "
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
       elog(WARNING, "odoo_audit: failed to insert audit row: %d", spi_ret);


   SPI_finish();
}
/* ----------------------------------------------------------------
* odoo_audit_process_relation
*
* After DML on one result relation, walk the es_tupleTable
* transition slot to extract old/new tuples and call
* odoo_audit_write() for each modified row.
* ---------------------------------------------------------------- */
static void
odoo_audit_process_relation(ResultRelInfo *rri,
                            EState        *estate,
                            const char    *operation)
{
   Relation    rel      = rri->ri_RelationDesc;
   const char *relname  = RelationGetRelationName(rel);
   TupleDesc   tupdesc  = RelationGetDescr(rel);


   if (!odoo_audit_table_is_tracked(relname))
       return;


   /*
    * Access the OLD and NEW transition tuple stores if present.
    * These are populated when the planner sets up tuple routing for
    * ModifyTable nodes with transition tables, but for our core hook
    * we iterate es_range_table to find the ModifyTable node and
    * inspect per-row old/new tuples stored in the result rel's
    * transition tuplestores.
    */
   if (rri->ri_oldTupleSlot && rri->ri_newTupleSlot)
   {
       TupleTableSlot *old_slot = rri->ri_oldTupleSlot;
       TupleTableSlot *new_slot = rri->ri_newTupleSlot;


       HeapTuple old_tup = ExecFetchSlotHeapTuple(old_slot, false, NULL);
       HeapTuple new_tup = (strcmp(operation, "DELETE") == 0)
                           ? NULL
                           : ExecFetchSlotHeapTuple(new_slot, false, NULL);


       odoo_audit_write(relname, operation, old_tup, new_tup, tupdesc);
   }
}




/* ----------------------------------------------------------------
* odoo_audit_executor_finish  (ExecutorFinish hook)
*
* Fires after all rows have been processed by the executor.
* Walks every result relation to find UPDATE/DELETE targets.
* ---------------------------------------------------------------- */
static void
odoo_audit_executor_finish(QueryDesc *queryDesc)
{
   // ereport(LOG,(errmsg("odoo_audit: inside odoo_audit_executor_finish hook")));
   /* Chain to previous hook first */
   if (prev_ExecutorFinish)
       prev_ExecutorFinish(queryDesc);
   else
       standard_ExecutorFinish(queryDesc);


   if (!odoo_audit_enabled)
       return;
   if (odoo_audit_tables == NULL || odoo_audit_tables[0] == '\0')
       return;


   if (queryDesc->operation != CMD_UPDATE &&
       queryDesc->operation != CMD_DELETE)
       return;


   if (queryDesc->estate == NULL)
       return;


   {
       const char *op = (queryDesc->operation == CMD_UPDATE)
                        ? "UPDATE" : "DELETE";
       ListCell   *lc;


       /*
        * es_opened_result_relations is a List of ResultRelInfo pointers
        * for every relation that was actually opened for DML in this query.
        * This replaces the old es_num_result_relations counter in PG19.
        */
       foreach(lc, queryDesc->estate->es_opened_result_relations)
       {
           ResultRelInfo *rri = (ResultRelInfo *) lfirst(lc);
           odoo_audit_process_relation(rri, queryDesc->estate, op);
       }
   }
}


/* ----------------------------------------------------------------
* odoo_audit_init
*
* Called once from PostmasterMain / InitPostgres to install hooks.
* ---------------------------------------------------------------- */
void
odoo_audit_init(void)
{
   // ereport(LOG, (errmsg("odoo_audit: inside pg_audit.c file")));
   prev_ExecutorFinish  = ExecutorFinish_hook;
   ExecutorFinish_hook  = odoo_audit_executor_finish;
}
