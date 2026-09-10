/*-------------------------------------------------------------------------
 * db_blue_pinner.h
 *     Public interface for the DBblue pinner background worker.
 *     Include this in autoprewarm.c.
 *-------------------------------------------------------------------------
 */
#ifndef DB_BLUE_PINNER_H
#define DB_BLUE_PINNER_H

extern void DBBluePinnerRegisterGUCs(void);
extern void DBBluePinnerRegister(void);
extern PGDLLEXPORT void DBBluePinnerMain(Datum main_arg);

#endif  /* DB_BLUE_PINNER_H */