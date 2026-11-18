/*-------------------------------------------------------------------------
 *
 * dbblue_predfingerprint.c
 *	  Value-sensitive fingerprint of a predicate subtree, used as the key
 *	  for the DBblue COUNT cache.
 *
 * The fingerprint is computed by walking the qual tree to reject
 * uncacheable shapes (Params, sublinks, volatile functions) and then
 * delegating to JumbleExpr() with include_consts = true so that two
 * predicates differing only in their literal values produce distinct
 * hashes.  The relation OID is mixed in so the same predicate against
 * two different tables cannot collide.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/dbblue_predfingerprint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/catalog.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_inherits.h"
#include "nodes/nodeFuncs.h"
#include "nodes/queryjumble.h"
#include "optimizer/optimizer.h"
#include "utils/dbblue_predfingerprint.h"
#include "utils/lsyscache.h"

typedef struct CacheabilityCtx
{
	bool		rejected;
} CacheabilityCtx;

static bool
predicate_cacheability_walker(Node *node, CacheabilityCtx *ctx)
{
	if (node == NULL)
		return false;

	/*
	 * A Param means the predicate's literal value is supplied at execute
	 * time; caching a row count under a key that has not seen the bound
	 * value would be unsafe.  In practice Odoo's psycopg2 path produces
	 * custom plans where Params are folded to Consts by
	 * eval_const_expressions before we ever look at the tree, so this
	 * branch is the safety net for the generic-plan case.
	 */
	if (IsA(node, Param))
	{
		ctx->rejected = true;
		return true;			/* stop walking */
	}

	/* Subqueries are out of scope for the COUNT cache. */
	if (IsA(node, SubLink) || IsA(node, SubPlan))
	{
		ctx->rejected = true;
		return true;
	}

	return expression_tree_walker(node, predicate_cacheability_walker, ctx);
}

int64
dbblue_predicate_fingerprint(Oid reloid, Node *quals)
{
	CacheabilityCtx ctx = {false};
	int64		hash;

	if (quals == NULL || !OidIsValid(reloid))
		return INT64CONST(0);

	/*
	 * System catalogs are written through paths that bypass the table AM --
	 * CatalogTupleInsert/Update/Delete reach heap_* directly, and
	 * heap_inplace_update bypasses even that -- so their modification stamps
	 * are not reliably maintained.  Rather than chase every catalog write
	 * site, refuse to cache counts over catalogs; no paginated application
	 * query counts them anyway.
	 */
	if (IsCatalogRelationOid(reloid))
		return INT64CONST(0);

	/*
	 * A count over a partitioned table or an inheritance parent aggregates
	 * rows that physically live in child relations, and writes bump the
	 * child's stamp, not the parent's.  Tracking the whole ancestry on every
	 * write would make the common case pay for the rare one, so exclude
	 * these instead.
	 */
	if (get_rel_relkind(reloid) == RELKIND_PARTITIONED_TABLE ||
		has_subclass(reloid))
		return INT64CONST(0);

	/*
	 * A foreign table's rows do not live here.  Writes go through the FDW
	 * rather than the tableam wrappers, so no stamp ever moves -- and the
	 * remote side can be changed by something outside this cluster entirely,
	 * which no amount of local tracking could observe.
	 *
	 * Aggregate pushdown hides this most of the time: the plan root becomes a
	 * ForeignScan rather than an Agg, and the shape gate declines it for that
	 * reason.  But a qual the FDW cannot ship -- a plpgsql function, say --
	 * keeps the Agg local, and then the count is captured and served stale.
	 */
	if (get_rel_relkind(reloid) == RELKIND_FOREIGN_TABLE)
		return INT64CONST(0);

	(void) predicate_cacheability_walker(quals, &ctx);
	if (ctx.rejected)
		return INT64CONST(0);

	/*
	 * Reject anything that is not IMMUTABLE, not merely anything VOLATILE.
	 *
	 * The fingerprint hashes the *expression*, so two evaluations that read
	 * differently but look identical collapse onto one cache key.  A STABLE
	 * function is exactly that case: "WHERE owner = current_user" has one
	 * fingerprint but a different answer per role, and "WHERE ts > now() -
	 * interval '1 day'" has one fingerprint but a different answer per day.
	 * contain_mutable_functions() also covers SQLValueFunction, which is how
	 * current_user and friends are represented.
	 */
	if (contain_mutable_functions(quals))
		return INT64CONST(0);

	hash = JumbleExpr(quals, true);

	/*
	 * Fold the relation OID in so the same predicate against a different
	 * base relation cannot share a cache slot.  hash_combine64 is overkill
	 * for a single 32-bit input; a rotate + xor is plenty given the
	 * jumble buffer already hashed everything else.
	 */
	hash ^= ((int64) reloid) * INT64CONST(0x9E3779B97F4A7C15);

	/*
	 * Reserve 0 as the "uncacheable" sentinel returned above.  Map a
	 * spurious zero result to 1.
	 */
	if (hash == INT64CONST(0))
		hash = INT64CONST(1);

	return hash;
}
