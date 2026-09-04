#ifndef PG_AUDIT_H
#define PG_AUDIT_H


#include "postgres.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"
#include "utils/guc.h"


/*
 * Which DML operations dbblue_audit_operations selects.  The GUC is a list
 * of names; these bits are what the rest of the code tests against.
 */
#define DBBLUE_AUDIT_INSERT  (1 << 0)
#define DBBLUE_AUDIT_UPDATE  (1 << 1)
#define DBBLUE_AUDIT_DELETE  (1 << 2)

/* Rows removed per DELETE, so one sweep cannot hold a long lock. */
#define DBBLUE_AUDIT_PRUNE_BATCH 10000

/* GUC variables - defined in pg_audit.c */
extern bool  dbblue_audit_enabled;
extern char *dbblue_audit_tables;
extern char *dbblue_audit_operations;
extern char *dbblue_audit_exclude_columns;
extern char *dbblue_audit_retention;
extern bool  dbblue_audit_changed_columns_only;
extern char *dbblue_audit_database;
extern bool  dbblue_audit_require_write;

/* GUC hooks for dbblue_audit_operations */
extern bool  dbblue_check_audit_operations(char **newval, void **extra,
                                           GucSource source);
extern void  dbblue_assign_audit_operations(const char *newval, void *extra);
extern void  dbblue_assign_audit_exclude_columns(const char *newval, void *extra);
extern void  dbblue_assign_audit_database(const char *newval, void *extra);
extern bool  dbblue_check_audit_retention(char **newval, void **extra,
                                          GucSource source);
extern void  dbblue_assign_audit_retention(const char *newval, void *extra);

/* True when the named operation ("INSERT"/"UPDATE"/"DELETE") is selected. */
extern bool  dbblue_audit_operation_is_tracked(int op);

/* Retention, used by the dbblue audit pruner worker. */
extern bool  dbblue_audit_retention_is_active(void);
extern int64 dbblue_audit_prune_batch(void);


/* Core audit writer */
extern void  dbblue_audit_write(Relation rel,
                              const char *schema_name,
                              const char *table_name,
                              const char *operation,
                              HeapTuple   old_tuple,
                              HeapTuple   new_tuple,
                              TupleDesc   tupdesc);


/* Check if a table is in the dbblue_audit_tables list */
extern bool  dbblue_audit_table_is_tracked(Relation rel,
                                           const char **schema_out);


/*
 * Per-row capture entry points, called from the executor (nodeModifyTable.c)
 * once per row actually modified.
 */
extern void  dbblue_audit_capture_update(ResultRelInfo   *rri,
                                         TupleTableSlot  *oldslot,
                                         TupleTableSlot  *newslot);
extern void  dbblue_audit_capture_delete(ResultRelInfo   *rri,
                                         ItemPointer      tupleid,
                                         HeapTuple        oldtuple);
extern void  dbblue_audit_capture_insert(ResultRelInfo   *rri,
                                         TupleTableSlot  *newslot);


#endif /* PG_AUDIT_H */