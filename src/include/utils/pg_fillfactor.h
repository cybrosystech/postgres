#ifndef PG_FILLFACTOR_H
#define PG_FILLFACTOR_H

/* GUC variable - declared here, defined in dbblue_fillfactor.c */
extern char *dbblue_fillfactor_map;

/*
* Call once after GUC is initialised.
* Parses dbblue_fillfactor_map and builds the internal hash table.
*/
extern void dbblue_fillfactor_init(void);

/*
* Call whenever dbblue_fillfactor_map GUC changes (assign hook).
* Rebuilds the hash table from the new string.
*/
extern void dbblue_fillfactor_rebuild(void);

extern void dbblue_fillfactor_assign_hook(const char *newval, void *extra);

/*
* Look up a table name.
* Accepts both Odoo dot-notation (res.partner) and
* PostgreSQL underscore form (res_partner).
*
* Returns fillfactor (10-100) if found, -1 if not in the map.
*/
extern int  dbblue_fillfactor_lookup(const char *tablename);
/* test testtt*/
#endif /* PG_FILLFACTOR_H */