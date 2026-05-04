#ifndef PG_AUDIT_H
#define PG_AUDIT_H


#include "postgres.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"


/* GUC variables - defined in pg_audit.c */
extern bool  odoo_audit_enabled;
extern char *odoo_audit_tables;


/* Hook installation - called from postinit.c */
extern void  odoo_audit_init(void);


/* Core audit writer */
extern void  odoo_audit_write(const char *table_name,
                              const char *operation,
                              HeapTuple   old_tuple,
                              HeapTuple   new_tuple,
                              TupleDesc   tupdesc);


/* Check if a table is in the odoo_audit_tables list */
extern bool  odoo_audit_table_is_tracked(const char *table_name);


#endif /* PG_AUDIT_H */