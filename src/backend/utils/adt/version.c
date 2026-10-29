/*-------------------------------------------------------------------------
 *
 * version.c
 *	 Returns the PostgreSQL version string
 *
 * Copyright (c) 1998-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *
 * src/backend/utils/adt/version.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"


Datum
pgsql_version(PG_FUNCTION_ARGS)
{
	/*
	 * DBblue: report PG_VERSION_STR with "PostgreSQL" replaced by "DBblue",
	 * so the version, platform and compiler always match the build.
	 */
	const char *pgname = "PostgreSQL";
	const char *version_str = PG_VERSION_STR;

	if (strncmp(version_str, pgname, strlen(pgname)) == 0)
		version_str = psprintf("DBblue%s", version_str + strlen(pgname));

	PG_RETURN_TEXT_P(cstring_to_text(version_str));
}
