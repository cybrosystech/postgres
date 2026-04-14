#ifndef PG_FILLFACTOR_H
#define PG_FILLFACTOR_H

/* GUC variable - declared here, defined in odoo_fillfactor.c */
extern char *odoo_fillfactor_map;

/*
* Call once after GUC is initialised.
* Parses odoo_fillfactor_map and builds the internal hash table.
*/
extern void odoo_fillfactor_init(void);

/*
* Call whenever odoo_fillfactor_map GUC changes (assign hook).
* Rebuilds the hash table from the new string.
*/
extern void odoo_fillfactor_rebuild(void);

extern void odoo_fillfactor_assign_hook(const char *newval, void *extra);

/*
* Look up a table name.
* Accepts both Odoo dot-notation (res.partner) and
* PostgreSQL underscore form (res_partner).
*
* Returns fillfactor (10-100) if found, -1 if not in the map.
*/
extern int  odoo_fillfactor_lookup(const char *tablename);
/* test testtt*/
#endif /* PG_FILLFACTOR_H */