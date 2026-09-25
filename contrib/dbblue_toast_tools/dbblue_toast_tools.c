/*-------------------------------------------------------------------------
 *
 * dbblue_toast_tools.c
 *	  Helper for rewriting TOAST values to a different compression method.
 *
 * Changing a column's compression method does not touch data already
 * stored, and the obvious ways of forcing a rewrite do not work: an
 * UPDATE that assigns a column to itself carries the existing TOAST
 * pointer across untouched, VACUUM FULL and CLUSTER copy tuples verbatim,
 * and ALTER COLUMN TYPE with an identity USING clause likewise preserves
 * the datum.  All three silently leave the data exactly as it was.
 *
 * To actually recompress, the value has to be materialized as a fresh
 * plain datum, so that re-toasting has to run from scratch.  That is what
 * this function does, for any varlena type, without going through a text
 * representation (which would risk reformatting values of types such as
 * jsonb).
 *
 * IDENTIFICATION
 *	  contrib/dbblue_toast_tools/dbblue_toast_tools.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC_EXT(
					.name = "dbblue_toast_tools",
					.version = PG_VERSION
);

/*
 * Return the argument as a fully detoasted, decompressed copy.
 *
 * Non-varlena types cannot be toasted, so they are passed through.
 */
PG_FUNCTION_INFO_V1(dbblue_force_detoast);
Datum
dbblue_force_detoast(PG_FUNCTION_ARGS)
{
	Oid			typid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int16		typlen;
	bool		typbyval;
	char		typalign;

	if (!OidIsValid(typid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine argument data type")));

	get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign);

	if (typlen != -1)
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));

	PG_RETURN_DATUM(PointerGetDatum(PG_DETOAST_DATUM_COPY(PG_GETARG_DATUM(0))));
}
