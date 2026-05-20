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

#include "nodes/nodeFuncs.h"
#include "nodes/queryjumble.h"
#include "optimizer/optimizer.h"
#include "utils/dbblue_predfingerprint.h"

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

	(void) predicate_cacheability_walker(quals, &ctx);
	if (ctx.rejected)
		return INT64CONST(0);

	/*
	 * A volatile function call invalidates the very notion of a cached
	 * row count, since two calls in the same snapshot could disagree.
	 * contain_volatile_functions is the standard PG check; reuse it.
	 */
	if (contain_volatile_functions(quals))
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
