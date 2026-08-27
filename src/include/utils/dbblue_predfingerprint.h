/*-------------------------------------------------------------------------
 *
 * dbblue_predfingerprint.h
 *	  Value-sensitive fingerprint of a predicate subtree, used as the key
 *	  for the DBblue COUNT cache.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dbblue_predfingerprint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBBLUE_PREDFINGERPRINT_H
#define DBBLUE_PREDFINGERPRINT_H

#include "nodes/parsenodes.h"

/*
 * Compute a 64-bit fingerprint of a predicate subtree on a single base
 * relation, suitable for use as a COUNT-cache key.  The fingerprint folds
 * Const values into the hash so two otherwise-identical predicates that
 * differ only in literal arguments produce different fingerprints, while
 * the same predicate emitted twice (Odoo's COUNT then SELECT) produces an
 * identical fingerprint.
 *
 * The reloid is mixed into the hash so the same predicate cannot collide
 * across different base relations.
 *
 * Returns 0 when the predicate is not cacheable; that means the caller
 * must not consult nor write the cache.  A predicate is rejected when it
 * contains a Param node (we want literal-only predicates so cache entries
 * cannot capture session-specific parameter state), a SubLink/SubPlan, or
 * a call to a volatile function (whose results are not stable enough to
 * justify caching a row count).
 */
extern int64 dbblue_predicate_fingerprint(Oid reloid, Node *quals);

#endif							/* DBBLUE_PREDFINGERPRINT_H */
