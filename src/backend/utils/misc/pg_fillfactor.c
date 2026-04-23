/*-------------------------------------------------------------------------
* odoo_fillfactor.c
*
* Parses the GUC  odoo_fillfactor_map  and answers lookup queries
* from the CREATE TABLE path.
*
* GUC format:
*   odoo_fillfactor_map = 'res.partner:70, stock.move:75,
*                          account.move.line:80'
*
* Both dot-notation (Odoo model name) and underscore form (PG table name)
* are accepted — they are normalised to underscores in the hash key.
*-------------------------------------------------------------------------
*/


#include "postgres.h"


#include <ctype.h>


#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pg_fillfactor.h"
#include "miscadmin.h"
/* ----------------------------------------------------------------
*  GUC variable (the raw string from postgresql.conf / SET)
* ---------------------------------------------------------------- */
char *odoo_fillfactor_map = NULL;


/* ----------------------------------------------------------------
*  Internal hash table
* ---------------------------------------------------------------- */
#define ODOO_FF_KEYLEN  128          /* max table-name length */


typedef struct
{
   char key[ODOO_FF_KEYLEN];        /* normalised table name  */
   int  fillfactor;
} OdooFFEntry;


static HTAB            *odoo_ff_htab   = NULL;
static MemoryContext    odoo_ff_mcxt   = NULL;


/* ----------------------------------------------------------------
*  normalize_name
*
*  Lower-case + dots → underscores.
*  "res.partner"  →  "res_partner"
*  "res_partner"  →  "res_partner"   (idempotent)
* ---------------------------------------------------------------- */
static void
normalize_name(const char *src, char *dst, int dstlen)
{
   int i;
   for (i = 0; i < dstlen - 1 && src[i] != '\0'; i++)
   {
       unsigned char c = (unsigned char) src[i];
       dst[i] = (c == '.') ? '_' : pg_tolower(c);
   }
   dst[i] = '\0';
}


/* ----------------------------------------------------------------
*  rebuild_hash_table
*
*  Parses "name:ff, name:ff, ..." into the hash table.
*  Called at startup and on every GUC change.
* ---------------------------------------------------------------- */
static void
rebuild_hash_table(const char *mapstr)
{


    // ereport(LOG,
    //        (errmsg("rebuild_hash_table called: mapstr=\"%s\", "
    //                "current htab=%p",
    //                mapstr ? mapstr : "(NULL)",
    //                (void *) odoo_ff_htab)));


   // ereport(LOG,errmsg("rebuild hash table function part 1"));
   HASHCTL  ctl;
   char    *buf, *token, *saveptr;


   if (odoo_ff_htab)
   {
       hash_destroy(odoo_ff_htab);
       odoo_ff_htab = NULL;
   }
   if (odoo_ff_mcxt)
   {
       MemoryContextDelete(odoo_ff_mcxt);
       odoo_ff_mcxt = NULL;
   }


   if (mapstr == NULL || mapstr[0] == '\0')
       return;


   odoo_ff_mcxt = AllocSetContextCreate(TopMemoryContext,
                                        "OdooFillfactorMap",
                                        ALLOCSET_SMALL_SIZES);


   memset(&ctl, 0, sizeof(ctl));
   ctl.keysize   = ODOO_FF_KEYLEN;
   ctl.entrysize = sizeof(OdooFFEntry);
   ctl.hcxt      = odoo_ff_mcxt;


   odoo_ff_htab = hash_create("OdooFillfactorMap", 32, &ctl,
                              HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);


   buf = MemoryContextStrdup(odoo_ff_mcxt, mapstr);


  for (token = strtok_r(buf, ",", &saveptr);
    token != NULL;
    token = strtok_r(NULL, ",", &saveptr))
{
   /* ALL declarations at the top - C90 rule */
   char         *colon;
   char         *ffstart;
   char          rawname[ODOO_FF_KEYLEN];
   char          normname[ODOO_FF_KEYLEN];
   long          ff;
   char         *endptr;
   int           namelen;
   bool          found;
   OdooFFEntry  *entry;


   ereport(LOG, (errmsg("inside for loop")));


   /* Trim leading whitespace from token */
   while (isspace((unsigned char) *token))
       token++;


       /* Find the LAST colon — handles "account.move.line:80"
        * where the name itself contains dots (no colons)          */
       colon = strrchr(token, ':');     /* ← use strrchr not strchr */
       if (!colon)
       {
           ereport(WARNING,
                   (errmsg("odoo_fillfactor_map: ignoring malformed entry \"%s\","
                           " expected \"tablename:fillfactor\"", token)));
           continue;
       }


       /* Extract table name, trim trailing whitespace */
       namelen = (int)(colon - token);
       while (namelen > 0 && isspace((unsigned char) token[namelen - 1]))
           namelen--;


       if (namelen == 0 || namelen >= ODOO_FF_KEYLEN)
       {
           ereport(WARNING,
                   (errmsg("odoo_fillfactor_map: skipping entry with "
                           "empty or too-long table name")));
           continue;
       }


       strncpy(rawname, token, namelen);
       rawname[namelen] = '\0';


       /* Parse fillfactor — skip leading whitespace first */
       ffstart = colon + 1;
       while (isspace((unsigned char) *ffstart))
           ffstart++;


       ff = strtol(ffstart, &endptr, 10);


       /* Trim trailing whitespace after digits */
       while (endptr && isspace((unsigned char) *endptr))
           endptr++;


       /* ffstart == endptr means no digits were consumed at all */
       if (ffstart == endptr || (endptr && *endptr != '\0'))
       {
           ereport(WARNING,
                   (errmsg("odoo_fillfactor_map: invalid fillfactor value "
                           "for table \"%s\", skipping", rawname)));
           continue;
       }


       if (ff < 10 || ff > 100)
       {
           ereport(WARNING,
                   (errmsg("odoo_fillfactor_map: fillfactor %ld for \"%s\" "
                           "out of range [10,100], skipping", ff, rawname)));
           continue;
       }


       // normalize_name(rawname, normname, sizeof(normname));
       memset(normname, 0, sizeof(normname));
       normalize_name(rawname, normname, sizeof(normname));


       entry = (OdooFFEntry *) hash_search(odoo_ff_htab, normname,
                                           HASH_ENTER, &found);
       if (found)
           ereport(DEBUG1,
                   (errmsg("odoo_fillfactor_map: duplicate \"%s\", overwriting",
                           normname)));


       entry->fillfactor = (int) ff;


       ereport(LOG,
               (errmsg("odoo_fillfactor_map: registered \"%s\" → fillfactor=%d",
                       normname, (int) ff)));  /* LOG not DEBUG2 so you can see it */
   }
}
/* ================================================================
*  Public API
* ================================================================ */


/*
* odoo_fillfactor_init
* Called once from InitializeGUCOptions() or PostmasterMain.
*/
void
odoo_fillfactor_init(void)
{
   /* The GUC value may already be set from postgresql.conf at this point */
   rebuild_hash_table(odoo_fillfactor_map);
}


/*
* odoo_fillfactor_rebuild
* GUC assign hook — called whenever odoo_fillfactor_map changes.
*/
void
odoo_fillfactor_rebuild(void)
{
   rebuild_hash_table(odoo_fillfactor_map);
}




// int
// odoo_fillfactor_lookup(const char *tablename)
// {
//    char         normname[ODOO_FF_KEYLEN];
//    OdooFFEntry *entry;


//    if (!tablename)
//        return -1;


//    // rebuild if hash is NULL OR empty */
//    if (!odoo_ff_htab || hash_get_num_entries(odoo_ff_htab) == 0)
//    {
//        ereport(LOG,
//                (errmsg("lookup: rebuilding hash in PID %d (htab=%p)",
//                        MyProcPid, (void *) odoo_ff_htab)));


//        rebuild_hash_table(odoo_fillfactor_map);
//    }


//    // normalize_name(tablename, normname, sizeof(normname));
//    memset(normname, 0, sizeof(normname));
// normalize_name(tablename, normname, sizeof(normname));


//    ereport(LOG, (errmsg("lookup: searching key = %s", normname)));


//    entry = (OdooFFEntry *) hash_search(odoo_ff_htab, normname,
//                                        HASH_FIND, NULL);


//    if (!entry)
//    {
//        ereport(LOG, (errmsg("lookup: entry NOT FOUND")));
//        return -1;
//    }


//    ereport(LOG,
//            (errmsg("lookup: found fillfactor = %d", entry->fillfactor)));


//    return entry->fillfactor;
// }



int
odoo_fillfactor_lookup(const char *tablename)
{
    char         normname[ODOO_FF_KEYLEN];
    OdooFFEntry *entry;

    if (!tablename)
        return -1;

    /* Rebuild if hash is NULL or empty */
    if (!odoo_ff_htab || hash_get_num_entries(odoo_ff_htab) == 0)
    {
        ereport(LOG,
                (errmsg("lookup: rebuilding hash in PID %d (htab=%p)",
                        MyProcPid, (void *) odoo_ff_htab)));
        rebuild_hash_table(odoo_fillfactor_map);
    }

    /*
     * If the map is empty/unset, rebuild_hash_table() returns without
     * creating the hash table. Bail out here instead of passing NULL
     * into hash_search().
     */
    if (!odoo_ff_htab)
        return -1;

    memset(normname, 0, sizeof(normname));
    normalize_name(tablename, normname, sizeof(normname));

    ereport(LOG, (errmsg("lookup: searching key = %s", normname)));

    entry = (OdooFFEntry *) hash_search(odoo_ff_htab, normname,
                                        HASH_FIND, NULL);

    if (!entry)
    {
        ereport(LOG, (errmsg("lookup: entry NOT FOUND")));
        return -1;
    }

    ereport(LOG,
            (errmsg("lookup: found fillfactor = %d", entry->fillfactor)));

    return entry->fillfactor;
}

/*
* odoo_fillfactor_assign_hook
*
* GUC assign hook - called by GUC machinery whenever
* odoo_fillfactor_map is changed via SET or postgresql.conf reload.
* Signature must match GucStringAssignHook exactly.
*/
void
odoo_fillfactor_assign_hook(const char *newval, void *extra)
{
   rebuild_hash_table(newval);
}
