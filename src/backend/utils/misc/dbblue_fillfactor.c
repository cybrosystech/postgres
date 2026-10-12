/*-------------------------------------------------------------------------
 * dbblue_fillfactor.c
 *
 * Parses the GUC  dbblue_fillfactor_map  and answers lookup queries
 * from the CREATE TABLE path.
 *
 * GUC format:
 *   dbblue_fillfactor_map = 'res.partner:70, stock.move:75,
 *                           account.move.line:80'
 *
 * Both dot-notation (dbblue model name) and underscore form (PG table name)
 * are accepted -- they are normalised to underscores in the hash key.
 *
 * The value is validated by a GUC check hook (dbblue_fillfactor_check_hook),
 * so a malformed map is rejected at SET / configuration-reload time with a
 * clear message rather than being accepted and then warned about -- once per
 * backend, on every connection -- from the build path.  Because the check
 * hook guarantees the stored value is well formed, the build path
 * (rebuild_hash_table) never has to warn.
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include <ctype.h>

#include "miscadmin.h"
#include "utils/dbblue_fillfactor.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 *  GUC variable (the raw string from postgresql.conf / SET)
 * ---------------------------------------------------------------- */
char	   *dbblue_fillfactor_map = NULL;

/* ----------------------------------------------------------------
 *  Internal hash table
 * ---------------------------------------------------------------- */
#define DBBLUE_FF_KEYLEN  128		/* max table-name length */

typedef struct
{
	char		key[DBBLUE_FF_KEYLEN];	/* normalised table name */
	int			fillfactor;
} DbblueFFEntry;

static HTAB *dbblue_ff_htab = NULL;
static MemoryContext dbblue_ff_mcxt = NULL;

/* ----------------------------------------------------------------
 *  normalize_name
 *
 *  Lower-case + dots -> underscores.
 *  "res.partner"  ->  "res_partner"
 *  "res_partner"  ->  "res_partner"   (idempotent)
 * ---------------------------------------------------------------- */
static void
normalize_name(const char *src, char *dst, int dstlen)
{
	int			i;

	for (i = 0; i < dstlen - 1 && src[i] != '\0'; i++)
	{
		unsigned char c = (unsigned char) src[i];

		dst[i] = (c == '.') ? '_' : pg_tolower(c);
	}
	dst[i] = '\0';
}

/* ----------------------------------------------------------------
 *  parse_fillfactor_map
 *
 *  Walk "name:ff, name:ff, ..." once.  This is the single place that knows
 *  the map grammar; both validation and building go through it.
 *
 *  - When htab != NULL, each well-formed entry is inserted into it.
 *  - When errbuf != NULL (validation mode), parsing stops at the first
 *    malformed entry, a human-readable reason is written to errbuf, and the
 *    function returns false.  In build mode (errbuf == NULL) a malformed
 *    entry is silently skipped -- it cannot occur once the check hook has
 *    validated the value, and warning here is what used to spam the log on
 *    every connection.
 *
 *  Returns true when the whole string is well formed.
 * ---------------------------------------------------------------- */
static bool
parse_fillfactor_map(const char *mapstr, HTAB *htab,
					 char *errbuf, size_t errbuflen)
{
	char	   *buf;
	char	   *token;
	char	   *saveptr;

	if (mapstr == NULL || mapstr[0] == '\0')
		return true;			/* an empty map is valid (feature off) */

	buf = pstrdup(mapstr);

	for (token = strtok_r(buf, ",", &saveptr);
		 token != NULL;
		 token = strtok_r(NULL, ",", &saveptr))
	{
		char	   *colon;
		char	   *ffstart;
		char	   *endptr;
		char		rawname[DBBLUE_FF_KEYLEN];
		char		normname[DBBLUE_FF_KEYLEN];
		long		ff;
		int			namelen;

		/* Trim leading whitespace from the token. */
		while (isspace((unsigned char) *token))
			token++;
		if (*token == '\0')
			continue;			/* whitespace-only between commas: ignore */

		/*
		 * Find the LAST colon, so a dotted model name that contains no colon
		 * of its own ("account.move.line:80") still parses.
		 */
		colon = strrchr(token, ':');
		if (!colon)
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "malformed entry \"%s\"; expected \"tablename:fillfactor\".",
						 token);
				pfree(buf);
				return false;
			}
			continue;
		}

		/* Extract the table name, trimming trailing whitespace. */
		namelen = (int) (colon - token);
		while (namelen > 0 && isspace((unsigned char) token[namelen - 1]))
			namelen--;

		if (namelen == 0 || namelen >= DBBLUE_FF_KEYLEN)
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "entry has an empty or too-long table name (maximum %d characters).",
						 DBBLUE_FF_KEYLEN - 1);
				pfree(buf);
				return false;
			}
			continue;
		}

		memcpy(rawname, token, namelen);
		rawname[namelen] = '\0';

		/* Parse the fillfactor value. */
		ffstart = colon + 1;
		while (isspace((unsigned char) *ffstart))
			ffstart++;

		ff = strtol(ffstart, &endptr, 10);

		while (endptr && isspace((unsigned char) *endptr))
			endptr++;

		/* ffstart == endptr means no digits were consumed at all. */
		if (ffstart == endptr || (endptr && *endptr != '\0'))
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "invalid fillfactor value for table \"%s\".", rawname);
				pfree(buf);
				return false;
			}
			continue;
		}

		if (ff < 10 || ff > 100)
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "fillfactor %ld for \"%s\" is out of range [10,100].",
						 ff, rawname);
				pfree(buf);
				return false;
			}
			continue;
		}

		if (htab)
		{
			DbblueFFEntry *entry;

			memset(normname, 0, sizeof(normname));
			normalize_name(rawname, normname, sizeof(normname));

			entry = (DbblueFFEntry *) hash_search(htab, normname,
												  HASH_ENTER, NULL);
			entry->fillfactor = (int) ff;	/* last one wins on duplicates */
		}
	}

	pfree(buf);
	return true;
}

/* ----------------------------------------------------------------
 *  rebuild_hash_table
 *
 *  (Re)build the per-backend lookup hash from the current map string.  The
 *  value has already been validated by the check hook, so this never warns.
 * ---------------------------------------------------------------- */
static void
rebuild_hash_table(const char *mapstr)
{
	HASHCTL		ctl;

	if (dbblue_ff_htab)
	{
		hash_destroy(dbblue_ff_htab);
		dbblue_ff_htab = NULL;
	}
	if (dbblue_ff_mcxt)
	{
		MemoryContextDelete(dbblue_ff_mcxt);
		dbblue_ff_mcxt = NULL;
	}

	if (mapstr == NULL || mapstr[0] == '\0')
		return;

	dbblue_ff_mcxt = AllocSetContextCreate(TopMemoryContext,
										   "DbblueFillfactorMap",
										   ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = DBBLUE_FF_KEYLEN;
	ctl.entrysize = sizeof(DbblueFFEntry);
	ctl.hcxt = dbblue_ff_mcxt;

	dbblue_ff_htab = hash_create("DbblueFillfactorMap", 32, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	(void) parse_fillfactor_map(mapstr, dbblue_ff_htab, NULL, 0);
}

/* ================================================================
 *  Public API
 * ================================================================ */

/*
 * dbblue_fillfactor_init
 *		Build the lookup hash from the current GUC value.
 */
void
dbblue_fillfactor_init(void)
{
	rebuild_hash_table(dbblue_fillfactor_map);
}

/*
 * dbblue_fillfactor_rebuild
 *		Rebuild the lookup hash (e.g. after the GUC changes).
 */
void
dbblue_fillfactor_rebuild(void)
{
	rebuild_hash_table(dbblue_fillfactor_map);
}

/*
 * dbblue_fillfactor_lookup
 *		Look up a table name.  Accepts both dbblue dot-notation
 *		(res.partner) and PostgreSQL underscore form (res_partner).
 *		Returns the fillfactor (10-100) if found, -1 otherwise.
 */
int
dbblue_fillfactor_lookup(const char *tablename)
{
	char		normname[DBBLUE_FF_KEYLEN];
	DbblueFFEntry *entry;

	if (!tablename)
		return -1;

	/*
	 * Build the hash lazily on first use in this backend.  This is the single
	 * per-backend build point now that the redundant startup rebuilds have
	 * been removed; it is silent because the value was validated at SET time.
	 */
	if (!dbblue_ff_htab)
		rebuild_hash_table(dbblue_fillfactor_map);

	if (!dbblue_ff_htab)
		return -1;				/* no map configured */

	memset(normname, 0, sizeof(normname));
	normalize_name(tablename, normname, sizeof(normname));

	entry = (DbblueFFEntry *) hash_search(dbblue_ff_htab, normname,
										  HASH_FIND, NULL);
	return entry ? entry->fillfactor : -1;
}

/*
 * dbblue_fillfactor_check_hook
 *		GUC check hook.  Rejects a malformed map at SET / reload time, so an
 *		invalid value is caught once, with a clear message, instead of being
 *		accepted and then warned about on every connection.
 */
bool
dbblue_fillfactor_check_hook(char **newval, void **extra, GucSource source)
{
	char		errbuf[256];

	if (!parse_fillfactor_map(*newval, NULL, errbuf, sizeof(errbuf)))
	{
		GUC_check_errdetail("%s", errbuf);
		return false;
	}
	return true;
}

/*
 * dbblue_fillfactor_assign_hook
 *		GUC assign hook.  The new value is already validated; rebuild the
 *		lookup hash from it.
 */
void
dbblue_fillfactor_assign_hook(const char *newval, void *extra)
{
	rebuild_hash_table(newval);
}
