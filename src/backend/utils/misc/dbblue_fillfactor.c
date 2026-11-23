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
 * clear message.  The check hook also builds the parsed map, in one
 * guc_malloc'd block that becomes the setting's "extra"; the assign hook
 * only installs that pointer.  An assign hook must not fail: it also runs
 * while a transaction aborts and, on configuration reload, in the postmaster,
 * where an error is FATAL (see src/backend/utils/misc/README).
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include <ctype.h>

#include "miscadmin.h"
#include "utils/dbblue_fillfactor.h"
#include "utils/guc.h"

/* ----------------------------------------------------------------
 *  GUC variable (the raw string from postgresql.conf / SET)
 * ---------------------------------------------------------------- */
char	   *dbblue_fillfactor_map = NULL;

/* ----------------------------------------------------------------
 *  Parsed map
 * ---------------------------------------------------------------- */
/*
 * A table name is at most NAMEDATALEN - 1 bytes: the parser truncates longer
 * identifiers before CREATE TABLE sees them, so a longer map entry could never
 * match a table and is rejected instead.
 */
#define DBBLUE_FF_KEYLEN  NAMEDATALEN

typedef struct
{
	char		key[DBBLUE_FF_KEYLEN];	/* normalised table name */
	int			fillfactor;
} DbblueFFEntry;

/* The parsed map: the "extra" of a dbblue_fillfactor_map setting. */
typedef struct
{
	int			nentries;
	DbblueFFEntry entries[FLEXIBLE_ARRAY_MEMBER];
} DbblueFFMap;

/* Map in effect, installed by the assign hook; NULL when empty. */
static const DbblueFFMap *current_map = NULL;

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
 *  - The number of well-formed entries is returned in *nentries.
 *  - When map != NULL, each well-formed entry is also stored in it; it must
 *    have room for the *nentries counted by an earlier call.
 *  - When errbuf != NULL (validation mode), parsing stops at the first
 *    malformed entry, a human-readable reason is written to errbuf, and the
 *    function returns false.  In build mode (errbuf == NULL) a malformed
 *    entry is silently skipped -- it cannot occur once the check hook has
 *    validated the value.
 *
 *  Returns true when the whole string is well formed.
 * ---------------------------------------------------------------- */
static bool
parse_fillfactor_map(const char *mapstr, DbblueFFMap *map, int *nentries,
					 char *errbuf, size_t errbuflen)
{
	char	   *buf;
	char	   *token;
	char	   *saveptr;

	*nentries = 0;
	if (map)
		map->nentries = 0;

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

		if (namelen == 0)
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "entry \"%s\" has an empty table name.", token);
				pfree(buf);
				return false;
			}
			continue;
		}

		if (namelen >= DBBLUE_FF_KEYLEN)
		{
			if (errbuf)
			{
				snprintf(errbuf, errbuflen,
						 "table name \"%.*s\" is longer than %d bytes, so it can never match a table.",
						 namelen, token, DBBLUE_FF_KEYLEN - 1);
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

		(*nentries)++;

		if (map)
		{
			DbblueFFEntry *entry = NULL;

			memset(normname, 0, sizeof(normname));
			normalize_name(rawname, normname, sizeof(normname));

			for (int i = 0; i < map->nentries; i++)
			{
				if (strcmp(map->entries[i].key, normname) == 0)
				{
					entry = &map->entries[i];
					break;
				}
			}
			if (entry == NULL)
			{
				entry = &map->entries[map->nentries++];
				strlcpy(entry->key, normname, sizeof(entry->key));
			}
			entry->fillfactor = (int) ff;	/* last one wins on duplicates */
		}
	}

	pfree(buf);
	return true;
}

/* ================================================================
 *  Public API
 * ================================================================ */

/*
 * dbblue_fillfactor_lookup
 *		Look up a table name.  Accepts both dbblue dot-notation
 *		(res.partner) and PostgreSQL underscore form (res_partner).
 *		Returns the fillfactor (10-100) if found, -1 otherwise.
 *		A linear scan: maps are small and this runs once per CREATE TABLE.
 */
int
dbblue_fillfactor_lookup(const char *tablename)
{
	char		normname[DBBLUE_FF_KEYLEN];

	if (!tablename || current_map == NULL)
		return -1;				/* no map configured */

	memset(normname, 0, sizeof(normname));
	normalize_name(tablename, normname, sizeof(normname));

	for (int i = 0; i < current_map->nentries; i++)
	{
		if (strcmp(current_map->entries[i].key, normname) == 0)
			return current_map->entries[i].fillfactor;
	}
	return -1;
}

/*
 * dbblue_fillfactor_check_hook
 *		GUC check hook.  Rejects a malformed map at SET / reload time with a
 *		clear message, and otherwise builds the parsed map as the setting's
 *		extra.  Out of memory makes it return false (the value is refused, a
 *		reload keeps the old one) instead of failing in the assign hook.
 */
bool
dbblue_fillfactor_check_hook(char **newval, void **extra, GucSource source)
{
	char		errbuf[256];
	DbblueFFMap *map;
	int			nentries;

	if (!parse_fillfactor_map(*newval, NULL, &nentries,
							  errbuf, sizeof(errbuf)))
	{
		GUC_check_errdetail("%s", errbuf);
		return false;
	}

	/* An empty map (feature off) needs no extra. */
	if (nentries == 0)
		return true;

	map = (DbblueFFMap *) guc_malloc(LOG,
									 offsetof(DbblueFFMap, entries) +
									 nentries * sizeof(DbblueFFEntry));
	if (map == NULL)
		return false;

	(void) parse_fillfactor_map(*newval, map, &nentries, NULL, 0);
	*extra = map;
	return true;
}

/*
 * dbblue_fillfactor_assign_hook
 *		GUC assign hook.  Installs the map the check hook built; cannot fail.
 *		Rolling a setting back reinstalls the extra kept for the old value.
 */
void
dbblue_fillfactor_assign_hook(const char *newval, void *extra)
{
	current_map = (const DbblueFFMap *) extra;
}
