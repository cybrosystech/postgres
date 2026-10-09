/*-------------------------------------------------------------------------
 *
 * nodeHashgroupjoin.h
 *	  prototypes for nodeHashgroupjoin.c
 *
 * dbblue-specific; see dbblue_groupjoin.md.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHashgroupjoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASHGROUPJOIN_H
#define NODEHASHGROUPJOIN_H

#include "nodes/execnodes.h"

extern HashGroupJoinState *ExecInitHashGroupJoin(HashGroupJoin *node,
												 EState *estate, int eflags);
extern void ExecEndHashGroupJoin(HashGroupJoinState *node);
extern void ExecReScanHashGroupJoin(HashGroupJoinState *node);

#endif							/* NODEHASHGROUPJOIN_H */
