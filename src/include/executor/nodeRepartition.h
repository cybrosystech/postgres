/*-------------------------------------------------------------------------
 *
 * nodeRepartition.h
 *	  Hash-exchange of partial aggregate rows between parallel participants.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeRepartition.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEREPARTITION_H
#define NODEREPARTITION_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern RepartitionState *ExecInitRepartition(Repartition *node, EState *estate,
											 int eflags);
extern void ExecEndRepartition(RepartitionState *node);
extern void ExecReScanRepartition(RepartitionState *node);

/* parallel support */
extern void ExecRepartitionEstimate(RepartitionState *node, ParallelContext *pcxt);
extern void ExecRepartitionInitializeDSM(RepartitionState *node, ParallelContext *pcxt);
extern void ExecRepartitionReInitializeDSM(RepartitionState *node, ParallelContext *pcxt);
extern void ExecRepartitionInitializeWorker(RepartitionState *node,
											ParallelWorkerContext *pwcxt);

#endif							/* NODEREPARTITION_H */
