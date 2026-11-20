#ifndef DBBLUE_FILLFACTOR_H
#define DBBLUE_FILLFACTOR_H

#include "utils/guc.h"

/* GUC variable - declared here, defined in dbblue_fillfactor.c */
extern char *dbblue_fillfactor_map;

/*
 * GUC check hook: validates dbblue_fillfactor_map, rejecting a malformed
 * value at SET / configuration-reload time, and builds the parsed map.
 */
extern bool dbblue_fillfactor_check_hook(char **newval, void **extra,
										 GucSource source);

/*
 * GUC assign hook: installs the map built by the check hook.  Cannot fail.
 */
extern void dbblue_fillfactor_assign_hook(const char *newval, void *extra);

/*
 * Look up a table name.
 * Accepts both dbblue dot-notation (res.partner) and
 * PostgreSQL underscore form (res_partner).
 *
 * Returns fillfactor (10-100) if found, -1 if not in the map.
 */
extern int	dbblue_fillfactor_lookup(const char *tablename);

#endif							/* DBBLUE_FILLFACTOR_H */
