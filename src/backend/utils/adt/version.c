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
	/* Modified to return DBblue version string */
	const char *version_str =
		"DBblue 19beta2 on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 11.4.0-1ubuntu1~22.04.3) 11.4.0, 64-bit";
	PG_RETURN_TEXT_P(cstring_to_text(version_str));
}
