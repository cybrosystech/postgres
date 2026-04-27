#ifndef PG_AUDIT_H
#define PG_AUDIT_H


#include "postgres.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"


/* GUC variables - defined in pg_audit.c */
extern bool  dbblue_audit_enabled;
extern char *dbblue_audit_tables;


/* Hook installation - called from postinit.c */
extern void  dbblue_audit_init(void);


/* Core audit writer */
extern void  dbblue_audit_write(const char *table_name,
                                const char *operation,
                                HeapTuple   old_tuple,
                                HeapTuple   new_tuple,
                                TupleDesc   tupdesc);


/* Check if a table is in the dbblue_audit_tables list */
extern bool  dbblue_audit_table_is_tracked(const char *table_name);


#endif /* PG_AUDIT_H */