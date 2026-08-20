/*-------------------------------------------------------------------------
 * db_blue_pinner.h
 *     Public interface for the DBblue pinner background worker.
 *     Include this in autoprewarm.c.
 *-------------------------------------------------------------------------
 */
#ifndef DB_BLUE_PINNER_H
#define DB_BLUE_PINNER_H

/*
 * The worker is registered from core (DBBlueRegisterPinnerWorker in
 * src/backend/storage/buffer/bufmgr.c), gated by dbblue_pinner_enabled. The
 * only symbol the .so must export is the worker entry point below, which core
 * names via bgw_function_name and loads lazily from this library at startup.
 */
extern PGDLLEXPORT void DBBluePinnerMain(Datum main_arg);

#endif  /* DB_BLUE_PINNER_H */