/*-------------------------------------------------------------------------
 * db_blue_pinner.c
 *
 * Background worker: reads db_blue.pinned_tables and
 * db_blue.ring_buffer_tables from GUCs, preloads those relations
 * into shared_buffers, applies soft-pin flags, and enforces ring-buffer
 * strategy for tables that must never pollute the cache.
 *
 * DROP THIS FILE into: contrib/pg_prewarm/db_blue_pinner.c
 * Add it to contrib/pg_prewarm/Makefile OBJS list.
 *
 * REGISTRATION: Call DBBluePinnerRegister() from pg_prewarm's _PG_init().
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "db_blue_pinner.h"
#include "utils/wait_event.h"   /* PG_WAIT_EXTENSION */
#include "tcop/tcopprot.h"      /* die */


/* =========================================================================
 * GUC VARIABLES
 * These become parameters in postgresql.conf.
 * All are PGC_SIGHUP so they reload without a server restart — the
 * operator just edits postgresql.conf and sends SIGHUP to the postmaster.
 * =========================================================================
 */

/*
 * db_blue.pinned_tables
 *
 * Comma-separated list of table names to keep warm in shared_buffers.
 * Supports two-tier syntax:
 *
 *   "table_name"            → Tier 2 (OLTP, yields under pressure)
 *   "table_name:tier1"      → Tier 1 (never yield, metadata only)
 *
 * Example:
 *   db_blue.pinned_tables = '
 *       res_users:tier1, res_company:tier1,
 *       res_partner, account_move, product_product
 *   '
 */
static char *DBBluePinner_pinned_tables = NULL;

/*
 * db_blue.ring_buffer_tables
 *
 * Comma-separated list of tables whose sequential scans must NEVER
 * pollute shared_buffers. All I/O for these tables is routed through
 * a private 256KB ring buffer.
 *
 * Best candidates: large append-mostly tables used in reports.
 *
 * Example:
 *   db_blue.ring_buffer_tables = '
 *       account_move_line, stock_move, mail_message, mail_mail,
 *       account_analytic_line, mrp_production
 *   '
 */
static char *DBBluePinner_ring_buffer_tables = NULL;

/*
 * db_blue.pin_check_interval
 *
 * How often (seconds) the pinner re-checks which tables need re-loading.
 * Set to 0 to disable periodic checks (prewarm on startup only).
 *
 * Recommended: 300 (5 minutes). Under heavy autovacuum activity or
 * when reports run frequently, consider 60-120.
 */
static int DBBluePinner_check_interval = 300;

/*
 * db_blue.max_pin_size_percent
 *
 * Maximum percentage of shared_buffers the pinner may occupy with
 * pinned data. The pinner sorts pinned_tables by access frequency
 * (from pg_stat_user_tables) and fills up to this budget.
 *
 * IMPORTANT: Leave at least 60% free for normal OLTP working set.
 * Recommended: 25-40%. Never set above 60%.
 */
static int DBBluePinner_max_pin_percent = 35;

/*
 * db_blue.pinner_database
 *
 * The database the pinner worker connects to. For single-database
 * Odoo setups, set this to your Odoo database name. For multi-DB
 * setups, run one pinner per database (future enhancement).
 */
static char *DBBluePinner_database = NULL;

/*
 * db_blue.min_access_count
 *
 * Minimum total access count (seq_scan + idx_scan from
 * pg_stat_user_tables) a table must have before the pinner considers
 * it a candidate. Tables below this threshold are skipped even if
 * listed in pinned_tables. Prevents warming tables that are
 * configured but never actually accessed.
 *
 * Set to 0 to disable this filter (always warm listed tables).
 */
static int DBBluePinner_min_access_count = 100;

/* =========================================================================
 * INTERNAL TYPES
 * =========================================================================
 */

/*
 * PinEntry — one resolved entry from the pinned_tables GUC.
 * The pinner resolves table names → OIDs once per cycle.
 */
typedef struct PinEntry
{
    char    table_name[NAMEDATALEN];
    Oid     relid;          /* pg_class.oid */
    Oid     relfileOid;     /* pg_class.relfilenode */
    Oid     relspcOid;      /* pg_class.reltablespace */
    int64   table_size;     /* bytes, from pg_total_relation_size */
    int64   access_count;   /* seq_scan + idx_scan */
    float4  cache_ratio;    /* fraction currently in shared_buffers */
    uint8   tier;           /* SOFT_PIN_TIER_1 or SOFT_PIN_TIER_2 */
} PinEntry;

/* Max tables we will process in one cycle */
#define MAX_PIN_ENTRIES     256

/* =========================================================================
 * FORWARD DECLARATIONS
 * =========================================================================
 */
/* Public functions — declared in db_blue_pinner.h */
PGDLLEXPORT void
DBBluePinnerMain(Datum main_arg);
void    DBBluePinnerRegister(void);
void    DBBluePinnerRegisterGUCs(void);

/* Private forward declarations */
static void  PinnerRunCycle(void);
static void  PinnerSetupRingBuffers(void);
static int   ResolvePinEntries(PinEntry *entries, int max_entries);
static void  SortEntriesByAccessCount(PinEntry *entries, int nentries);
static float4 GetRelCacheRatio(Oid relid);
static int64 GetRelTotalSize(Oid relid);
static int64 GetRelAccessCount(const char *table_name);
static void  PrewarmRelationIntoBuffers(PinEntry *entry);
static void  SoftPinRelationIndexes(PinEntry *entry);
static uint8 ParseTierSuffix(char *table_name);
static void  PinnerHandleSignals(void);

static bool pinner_shutdown_requested = false;

/* =========================================================================
 * GUC REGISTRATION
 *
 * Call DBBluePinnerRegisterGUCs() from _PG_init() in pg_prewarm.c
 * =========================================================================
 */

/*
 * GetRelFileNumber — internal helper.
 * Looks up the relfilenode for a given relation OID directly from
 * the syscache. This is the correct C-level approach — do not use
 * pg_relation_filenode() which is the SQL-callable wrapper and
 * takes FunctionCallInfo, not an Oid.
 */
static RelFileNumber
GetRelFileNumber(Oid relid)
{
    HeapTuple       tuple;
    Form_pg_class   classForm;
    RelFileNumber   relfilenum;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR,
             "db_blue pinner: cache lookup failed for relation %u", relid);

    classForm  = (Form_pg_class) GETSTRUCT(tuple);
    relfilenum = classForm->relfilenode;

    ReleaseSysCache(tuple);
    return relfilenum;
}


void
DBBluePinnerRegisterGUCs(void)
{
    DefineCustomStringVariable(
        "db_blue.pinned_tables",
        "Tables to keep resident in shared_buffers (comma-separated)",
        "Use 'tablename:tier1' for metadata tables that must never be evicted.",
        &DBBluePinner_pinned_tables,
        /* default — sensible Odoo metadata + OLTP tables */
        "res_users:tier1,res_company:tier1,res_currency:tier1,"
        "res_partner,product_template,product_product,"
        "account_move,sale_order,stock_quant",
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL
    );

    DefineCustomStringVariable(
        "db_blue.ring_buffer_tables",
        "Tables whose scans must never pollute shared_buffers",
        "All sequential reads for these tables are routed through a "
        "private ring buffer, preventing large reports from evicting "
        "hot OLTP data.",
        &DBBluePinner_ring_buffer_tables,
        /* default — known large Odoo report tables */
        "account_move_line,stock_move,mail_message,mail_mail,"
        "account_analytic_line,stock_move_line",
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL
    );

    DefineCustomIntVariable(
        "db_blue.pin_check_interval",
        "Seconds between pinner maintenance cycles (0 = startup only)",
        NULL,
        &DBBluePinner_check_interval,
        300,    /* default: 5 minutes */
        0,      /* min: disabled */
        3600,   /* max: 1 hour */
        PGC_SIGHUP,
        GUC_UNIT_S,
        NULL, NULL, NULL
    );

    DefineCustomIntVariable(
        "db_blue.max_pin_size_percent",
        "Max percentage of shared_buffers used for pinned tables",
        "The pinner fills tables in order of access frequency until "
        "this percentage of shared_buffers is consumed.",
        &DBBluePinner_max_pin_percent,
        35,     /* default: 35% */
        5,      /* min */
        60,     /* max — hard cap to prevent starving OLTP */
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL
    );

    DefineCustomStringVariable(
        "db_blue.pinner_database",
        "Database the pinner background worker connects to",
        NULL,
        &DBBluePinner_database,
        "postgres",   /* operator must change this to their Odoo DB */
        PGC_POSTMASTER,   /* requires restart — connects at startup */
        0,
        NULL, NULL, NULL
    );

    DefineCustomIntVariable(
        "db_blue.min_access_count",
        "Minimum pg_stat_user_tables access count before a table is warmed",
        "Set to 0 to warm all listed tables regardless of access history.",
        &DBBluePinner_min_access_count,
        100,
        0,
        INT_MAX,
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL
    );
}

/* =========================================================================
 * BACKGROUND WORKER REGISTRATION
 * =========================================================================
 */
void
DBBluePinnerRegister(void)
{
    BackgroundWorker worker;

    MemSet(&worker, 0, sizeof(worker));

    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS |
                              BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;   /* restart 10s after crash */

    strlcpy(worker.bgw_library_name, "pg_prewarm",
            sizeof(worker.bgw_library_name));
    strlcpy(worker.bgw_function_name, "DBBluePinnerMain",
            sizeof(worker.bgw_function_name));
    strlcpy(worker.bgw_name, "db_blue pinner",
            sizeof(worker.bgw_name));
    strlcpy(worker.bgw_type, "db_blue pinner",
            sizeof(worker.bgw_type));

    RegisterBackgroundWorker(&worker);
}

/* =========================================================================
 * WORKER MAIN LOOP
 * =========================================================================
 */
void
DBBluePinnerMain(Datum main_arg)
{
    /* Setup signal handlers */
    pqsignal(SIGHUP,  SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* Connect to Odoo database */
    BackgroundWorkerInitializeConnection(
        DBBluePinner_database ? DBBluePinner_database : "postgres",
        NULL,
        0
    );

    elog(LOG, "db_blue pinner started (database: %s, interval: %ds, "
         "max_pin_pct: %d%%)",
         DBBluePinner_database ? DBBluePinner_database : "postgres",
         DBBluePinner_check_interval,
         DBBluePinner_max_pin_percent);

    /*
     * First cycle — run immediately on startup.
     * This is the "cold start" prewarm.
     * For Odoo, this runs once when PostgreSQL starts, before any
     * user connections arrive, giving the best chance of a warm cache.
     */
    PinnerRunCycle();

    /* Main loop */
    for (;;)
    {
        int     sleep_ms;

        PinnerHandleSignals();

        if (pinner_shutdown_requested)
            break;

        /* If interval is 0, do startup prewarm only */
        if (DBBluePinner_check_interval == 0)
        {
            elog(LOG, "db_blue pinner: pin_check_interval=0, "
                 "startup prewarm complete, exiting");
            break;
        }

        sleep_ms = DBBluePinner_check_interval * 1000;

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         sleep_ms,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        PinnerHandleSignals();

        if (pinner_shutdown_requested)
            break;

        PinnerRunCycle();
    }

    proc_exit(0);
}

/* =========================================================================
 * PINNER CYCLE — the core logic
 * =========================================================================
 */

/*
 * PinnerRunCycle — one full check, prewarm, and pin cycle.
 *
 * Steps:
 *   1. Calculate byte budget from max_pin_size_percent
 *   2. Resolve pinned_tables GUC → relfileOids
 *   3. Query pg_stat_user_tables for access counts
 *   4. Sort by access count descending
 *   5. For each table (in order): check cache ratio,
 *      prewarm if needed, apply soft pin
 *   6. Update ring_buffer_tables registration
 */
static void
PinnerRunCycle(void)
{
    PinEntry    entries[MAX_PIN_ENTRIES];
    int         nentries;
    int64       budget_bytes;
    int64       used_bytes   = 0;
    int         i;

    elog(DEBUG1, "db_blue pinner: starting cycle");

    /* ---- Calculate byte budget ---- */
    budget_bytes = (int64) NBuffers * BLCKSZ *
                   DBBluePinner_max_pin_percent / 100;

    elog(DEBUG1,
         "db_blue pinner: shared_buffers=%ldMB, budget=%ldMB (%d%%)",
         (long) ((int64) NBuffers * BLCKSZ / (1024 * 1024)),
         (long) (budget_bytes / (1024 * 1024)),
         DBBluePinner_max_pin_percent);

    /* ---- Resolve table names to OIDs, get stats ---- */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    nentries = ResolvePinEntries(entries, MAX_PIN_ENTRIES);

    if (nentries == 0)
    {
        elog(DEBUG1, "db_blue pinner: no valid tables found in "
             "db_blue.pinned_tables");
        goto cycle_end;
    }

    /* ---- Sort: highest access count first ---- */
    SortEntriesByAccessCount(entries, nentries);

    /* ---- Process each table ---- */
    for (i = 0; i < nentries; i++)
    {
        PinEntry   *e = &entries[i];

        /* Skip tables below minimum access threshold */
        if (DBBluePinner_min_access_count > 0 &&
            e->access_count < DBBluePinner_min_access_count)
        {
            elog(DEBUG1,
                 "db_blue pinner: skipping \"%s\" "
                 "(access_count=%ld < min=%d)",
                 e->table_name, (long) e->access_count,
                 DBBluePinner_min_access_count);
            continue;
        }

        /* Check budget */
        if (used_bytes + e->table_size > budget_bytes)
        {
            elog(LOG,
                 "db_blue pinner: budget reached at \"%s\" "
                 "(used %ldMB of %ldMB budget)",
                 e->table_name,
                 (long) (used_bytes   / (1024 * 1024)),
                 (long) (budget_bytes / (1024 * 1024)));
            continue;
        }

        /* Prewarm if cache ratio is below 80% */
        if (e->cache_ratio < 0.80f)
        {
            elog(LOG,
                 "db_blue pinner: prewarming \"%s\" "
                 "(%.0f%% cached, %ldMB, tier %d)",
                 e->table_name,
                 e->cache_ratio * 100.0,
                 (long) (e->table_size / (1024 * 1024)),
                 e->tier);

            PrewarmRelationIntoBuffers(e);
        }
        else
        {
            elog(DEBUG1,
                 "db_blue pinner: \"%s\" already %.0f%% cached — "
                 "applying soft pin only",
                 e->table_name, e->cache_ratio * 100.0);
        }

        /* Apply soft-pin flag to all cached heap buffers */
        SoftPinRelationBuffers(e->relspcOid,
                               MyDatabaseId,
                               e->relfileOid,
                               e->tier);

        /*
         * Also pin each index's buffers. SoftPinRelationBuffers stamps
         * by relfilenode, which is per-relation — index relfilenodes
         * differ from the heap's, so without this call indexes remain
         * evictable even when the heap is fully protected. Applies to
         * both tiers: protecting the heap of a hot OLTP table without
         * its indexes still costs idx_blks_read under pressure.
         */
        SoftPinRelationIndexes(e);

        used_bytes += e->table_size;
    }

    /* ---- Update ring buffer registration ---- */
    PinnerSetupRingBuffers();

cycle_end:
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    elog(DEBUG1,
         "db_blue pinner: cycle complete, %ldMB pinned",
         (long) (used_bytes / (1024 * 1024)));
}

/* =========================================================================
 * RING BUFFER SETUP
 * =========================================================================
 */

/*
 * PinnerSetupRingBuffers — parse ring_buffer_tables GUC and register
 * each table's relfilenode in the shared RingBufferRelations array.
 *
 * This runs inside a transaction so we can resolve table names to OIDs.
 */
static void
PinnerSetupRingBuffers(void)
{
    List       *namelist;
    ListCell   *lc;
    char       *rawstring;

    if (DBBluePinner_ring_buffer_tables == NULL ||
        strlen(DBBluePinner_ring_buffer_tables) == 0)
        return;

    rawstring = pstrdup(DBBluePinner_ring_buffer_tables);

    if (!SplitIdentifierString(rawstring, ',', &namelist))
    {
        elog(WARNING,
             "db_blue pinner: invalid db_blue.ring_buffer_tables syntax");
        return;
    }

    foreach(lc, namelist)
    {
        char   *table_name = (char *) lfirst(lc);
        Oid     relid;
        Oid     relfileOid;

        /* Trim whitespace */
        while (*table_name == ' ') table_name++;

        relid = RelnameGetRelid(table_name);
        if (!OidIsValid(relid))
        {
            elog(WARNING,
                 "db_blue pinner: ring_buffer_table \"%s\" not found",
                 table_name);
            continue;
        }

        relfileOid = GetRelFileNumber(relid);
        if (!OidIsValid(relfileOid))
            continue;

        RegisterRingBufferRelation(relfileOid);

        elog(DEBUG1,
             "db_blue pinner: ring buffer forced for \"%s\" "
             "(relfileOid=%u)",
             table_name, relfileOid);
    }

    list_free(namelist);
}

/* =========================================================================
 * RESOLVE PINNED_TABLES GUC → PinEntry ARRAY
 * =========================================================================
 */

/*
 * ParseTierSuffix — parse ":tier1" or ":tier2" from a table name string.
 *
 * Modifies table_name in-place to strip the suffix.
 * Returns SOFT_PIN_TIER_1 or SOFT_PIN_TIER_2.
 */
static uint8
ParseTierSuffix(char *table_name)
{
    char   *colon = strrchr(table_name, ':');
    uint8   tier  = SOFT_PIN_TIER_2;   /* default */

    if (colon != NULL)
    {
        if (strcmp(colon + 1, "tier1") == 0)
            tier = SOFT_PIN_TIER_1;
        else if (strcmp(colon + 1, "tier2") == 0)
            tier = SOFT_PIN_TIER_2;
        else
            elog(WARNING,
                 "db_blue pinner: unknown tier suffix '%s' for table '%s', "
                 "using tier2",
                 colon + 1, table_name);

        *colon = '\0';   /* strip suffix from name */
    }

    return tier;
}

/*
 * ResolvePinEntries — turn the pinned_tables GUC string into a
 * populated PinEntry array with OIDs, sizes, and access counts.
 *
 * Returns the number of valid entries filled.
 * Must be called inside a transaction with an active snapshot.
 */
static int
ResolvePinEntries(PinEntry *entries, int max_entries)
{
    List       *namelist;
    ListCell   *lc;
    char       *rawstring;
    int         nentries = 0;

    if (DBBluePinner_pinned_tables == NULL ||
        strlen(DBBluePinner_pinned_tables) == 0)
        return 0;

    rawstring = pstrdup(DBBluePinner_pinned_tables);

    if (!SplitIdentifierString(rawstring, ',', &namelist))
    {
        elog(WARNING,
             "db_blue pinner: invalid db_blue.pinned_tables syntax");
        return 0;
    }

    foreach(lc, namelist)
    {
        char       *raw_name;
        char        table_name[NAMEDATALEN];
        uint8       tier;
        Oid         relid;
        PinEntry   *e;

        if (nentries >= max_entries)
            break;

        raw_name = pstrdup((char *) lfirst(lc));

        /* Trim whitespace */
        while (*raw_name == ' ') raw_name++;

        strlcpy(table_name, raw_name, NAMEDATALEN);

        /* Parse and strip tier suffix */
        tier = ParseTierSuffix(table_name);

        /* Resolve name → OID */
        relid = RelnameGetRelid(table_name);
        if (!OidIsValid(relid))
        {
            elog(WARNING,
                 "db_blue pinner: table \"%s\" not found in database, "
                 "skipping",
                 table_name);
            continue;
        }

        e = &entries[nentries];
        MemSet(e, 0, sizeof(PinEntry));

        strlcpy(e->table_name, table_name, NAMEDATALEN);
        e->relid        = relid;
        e->relfileOid = GetRelFileNumber(relid);
        e->relspcOid    = get_rel_tablespace(relid);
        if (!OidIsValid(e->relspcOid))
            e->relspcOid = MyDatabaseTableSpace;
        e->tier         = tier;
        e->table_size   = GetRelTotalSize(relid);
        e->access_count = GetRelAccessCount(table_name);
        e->cache_ratio  = GetRelCacheRatio(relid);

        nentries++;
    }

    list_free(namelist);
    return nentries;
}

/* =========================================================================
 * STATISTICS HELPERS (run inside SPI transaction)
 * =========================================================================
 */

/*
 * GetRelTotalSize — pg_total_relation_size() in bytes.
 * Includes table + all indexes + TOAST.
 */
static int64
GetRelTotalSize(Oid relid)
{
    int         ret;
    int64       size = 0;
    static const char *query =
        "SELECT pg_total_relation_size($1)";
    Datum       args[1];
    Oid         argtypes[1] = { OIDOID };

    args[0] = ObjectIdGetDatum(relid);

    ret = SPI_execute_with_args(query, 1, argtypes, args, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool    isnull;
        size = DatumGetInt64(
                   SPI_getbinval(SPI_tuptable->vals[0],
                                 SPI_tuptable->tupdesc,
                                 1, &isnull));
        if (isnull) size = 0;
    }

    return size;
}

/*
 * GetRelAccessCount — total access count from pg_stat_user_tables.
 * Returns seq_scan + idx_scan for the given table name.
 * Returns 0 if table has no statistics yet (never accessed).
 */
static int64
GetRelAccessCount(const char *table_name)
{
    int         ret;
    int64       count = 0;
    static const char *query =
        "SELECT COALESCE(seq_scan, 0) + COALESCE(idx_scan, 0) "
        "FROM pg_stat_user_tables "
        "WHERE relname = $1";
    Datum       args[1];
    Oid         argtypes[1] = { TEXTOID };

    args[0] = CStringGetTextDatum(table_name);

    ret = SPI_execute_with_args(query, 1, argtypes, args, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool    isnull;
        count = DatumGetInt64(
                    SPI_getbinval(SPI_tuptable->vals[0],
                                  SPI_tuptable->tupdesc,
                                  1, &isnull));
        if (isnull) count = 0;
    }

    return count;
}

/*
 * GetRelCacheRatio — fraction of relation pages currently in
 * shared_buffers. Uses pg_buffercache.
 *
 * Returns a value between 0.0 (nothing cached) and 1.0 (fully cached).
 *
 * NOTE: Requires pg_buffercache extension. If not installed,
 * returns 0.0 to force a prewarm (safe default).
 */
static float4
GetRelCacheRatio(Oid relid)
{
    int         ret;
    float4      ratio = 0.0f;
    static const char *query =
        "SELECT "
        "  COALESCE(b.cached_blocks, 0)::float4 / "
        "  GREATEST(c.relpages, 1)::float4 "
        "FROM pg_class c "
        "LEFT JOIN ("
        "    SELECT relfilenode, count(*) AS cached_blocks "
        "    FROM pg_buffercache "
        "    WHERE relfilenode = pg_relation_filenode($1) "
        "      AND relforknumber = 0 "
        "    GROUP BY relfilenode "
        ") b ON b.relfilenode = pg_relation_filenode($1) "
        "WHERE c.oid = $1";

    Datum       args[1];
    Oid         argtypes[1] = { OIDOID };

    args[0] = ObjectIdGetDatum(relid);

    ret = SPI_execute_with_args(query, 1, argtypes, args, NULL, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool    isnull;
        ratio = DatumGetFloat4(
                    SPI_getbinval(SPI_tuptable->vals[0],
                                  SPI_tuptable->tupdesc,
                                  1, &isnull));
        if (isnull) ratio = 0.0f;
    }

    return ratio;
}

/* =========================================================================
 * SORT HELPER
 * =========================================================================
 */

static int
CompareEntriesByAccessCount(const void *a, const void *b)
{
    const PinEntry *ea = (const PinEntry *) a;
    const PinEntry *eb = (const PinEntry *) b;

    /* Tier 1 always sorts first regardless of access count */
    if (ea->tier != eb->tier)
        return (ea->tier == SOFT_PIN_TIER_1) ? -1 : 1;

    /* Within same tier: higher access count first */
    if (eb->access_count > ea->access_count) return  1;
    if (eb->access_count < ea->access_count) return -1;
    return 0;
}

static void
SortEntriesByAccessCount(PinEntry *entries, int nentries)
{
    qsort(entries, nentries, sizeof(PinEntry),
          CompareEntriesByAccessCount);
}

/* =========================================================================
 * PREWARM A SINGLE RELATION
 * =========================================================================
 */

/*
 * PrewarmRelationIntoBuffers — load all pages of a relation (+ indexes)
 * into shared_buffers using the pg_prewarm(relname, 'buffer') approach.
 *
 * We call pg_prewarm() via SPI rather than re-implementing the page
 * loading loop, since pg_prewarm already handles forks (main + FSM + VM),
 * error recovery, and lock management correctly.
 *
 * For Tier 1 tables we also prewarm all indexes. For Tier 2 we prewarm
 * the table only (indexes will warm naturally through query access).
 */
static void
PrewarmRelationIntoBuffers(PinEntry *entry)
{
    int     ret;

    /* Prewarm main relation */
    {
        static const char *query =
            "SELECT pg_prewarm($1, 'buffer', 'main')";
        Datum   args[1];
        Oid     argtypes[1] = { TEXTOID };

        args[0] = CStringGetTextDatum(entry->table_name);
        ret = SPI_execute_with_args(query, 1, argtypes, args,
                                    NULL, false, 1);

        if (ret != SPI_OK_SELECT)
            elog(WARNING, "db_blue pinner: pg_prewarm failed for \"%s\"",
                 entry->table_name);
    }

    /* For Tier 1: also prewarm all indexes */
    if (entry->tier == SOFT_PIN_TIER_1)
    {
        static const char *idx_query =
            "SELECT pg_prewarm(indexrelid::regclass::text, 'buffer', 'main') "
            "FROM pg_index "
            "WHERE indrelid = $1";
        Datum   args[1];
        Oid     argtypes[1] = { OIDOID };

        args[0] = ObjectIdGetDatum(entry->relid);
        ret = SPI_execute_with_args(idx_query, 1, argtypes, args,
                                    NULL, false, 0);

        if (ret != SPI_OK_SELECT)
            elog(WARNING,
                 "db_blue pinner: index prewarm failed for \"%s\"",
                 entry->table_name);
    }
}

/* =========================================================================
 * SOFT-PIN INDEXES OF A RELATION
 * =========================================================================
 */

/*
 * SoftPinRelationIndexes — apply the relation's soft-pin tier to every
 * resident buffer of every index defined on the table.
 *
 * The heap-only call to SoftPinRelationBuffers leaves index pages
 * unprotected because each index has its own relfilenode. Under cache
 * pressure that turns into idx_blks_read on tables whose heaps are
 * fully soft-pinned — exactly the cost the pinner is supposed to
 * eliminate. Applies to both tiers.
 *
 * Soft pins only stamp resident buffers, so for Tier 2 (where indexes
 * are not prewarmed) this only protects index pages that have already
 * been faulted in by query traffic. The next pinner cycle will pick up
 * any newly-resident index pages.
 *
 * Must be called inside a transaction with an active SPI snapshot.
 */
static void
SoftPinRelationIndexes(PinEntry *entry)
{
    int         ret;
    uint64      i;
    int         pinned_indexes = 0;
    static const char *idx_query =
        "SELECT c.relfilenode, c.reltablespace "
        "FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE i.indrelid = $1";
    Datum       args[1];
    Oid         argtypes[1] = { OIDOID };

    args[0] = ObjectIdGetDatum(entry->relid);

    ret = SPI_execute_with_args(idx_query, 1, argtypes, args,
                                NULL, true, 0);
    if (ret != SPI_OK_SELECT)
    {
        elog(WARNING,
             "db_blue pinner: index lookup failed for \"%s\"",
             entry->table_name);
        return;
    }

    for (i = 0; i < SPI_processed; i++)
    {
        bool        isnull;
        Oid         idx_relfileOid;
        Oid         idx_relspcOid;

        idx_relfileOid = DatumGetObjectId(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 1, &isnull));
        if (isnull || !OidIsValid(idx_relfileOid))
            continue;

        idx_relspcOid = DatumGetObjectId(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull || !OidIsValid(idx_relspcOid))
            idx_relspcOid = MyDatabaseTableSpace;

        SoftPinRelationBuffers(idx_relspcOid,
                               MyDatabaseId,
                               idx_relfileOid,
                               entry->tier);
        pinned_indexes++;
    }

    if (pinned_indexes > 0)
        elog(DEBUG1,
             "db_blue pinner: soft-pinned %d indexes for \"%s\" (tier %d)",
             pinned_indexes, entry->table_name, entry->tier);
}

/* =========================================================================
 * SIGNAL HANDLING
 * =========================================================================
 */
static void
PinnerHandleSignals(void)
{
    if (ConfigReloadPending)
    {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
        elog(LOG, "db_blue pinner: configuration reloaded");
    }
}