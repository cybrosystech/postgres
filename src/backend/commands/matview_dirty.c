/*-------------------------------------------------------------------------
 *
 * matview_dirty.c
 *	  DBblue: dirty-flag tracking for REFRESH MATERIALIZED VIEW SKIP IF UNCHANGED
 *
 * Design
 * ------
 * A fixed-size array in shared memory holds one entry per watched source
 * table OID.  "Watched" means it is a source table of some matview that has
 * auto_skip_unchanged = true.  Each entry carries a dirty flag.
 *
 * Write path (heap_insert / heap_update / heap_delete):
 *   MatviewDirtyNote(relid) -- O(1): append to process-local list, no shmem.
 *
 * Commit path (xact callback):
 *   For every relid in the local list, look it up in the shmem array and, if
 *   found, set dirty = true.  Relids absent from the array are ignored.
 *   The callback fires on COMMIT and PREPARE (flush dirty flags) and on
 *   ABORT (discard without flushing).  PRE_COMMIT and other intermediate
 *   events are ignored so that xact_noted_rels survives to actual commit.
 *
 * REFRESH path:
 *   1. Collect source relids from the matview rewrite rule.
 *   2. Lazily register any that are not yet in the array (dirty = true).
 *   3. If every source relid is registered AND clean, skip the refresh.
 *   4. Otherwise refresh normally, then mark every source relid clean.
 *
 * Server restart: shmem is re-initialized empty.  Empty means "not found"
 * which is treated as dirty, so the first refresh after restart always runs.
 *
 * src/backend/commands/matview_dirty.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "commands/matview_dirty.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/* ----------
 * Shared-memory layout
 * ----------
 */
typedef struct MatviewDirtyEntry
{
	Oid			relid;
	bool		dirty;
} MatviewDirtyEntry;

typedef struct MatviewDirtyState
{
	slock_t		lock;
	int			nentries;
	MatviewDirtyEntry entries[MATVIEW_DIRTY_MAX_RELS];
} MatviewDirtyState;

static MatviewDirtyState *matview_dirty_state = NULL;

/* ----------
 * Process-local transaction list
 * We record relids written by the current transaction here and flush to
 * shared memory only on commit.
 * ----------
 */
static Oid	xact_noted_rels[MATVIEW_DIRTY_MAX_RELS];
static int	xact_noted_nrels = 0;
static bool callback_registered = false;

/* ----------
 * Forward declarations
 * ----------
 */
static void matview_dirty_xact_callback(XactEvent event, void *arg);
static void matview_shmem_mark_dirty(Oid relid);
static void MatviewDirtyShmemRequest(void *arg);
static void MatviewDirtyShmemInit(void *arg);

const ShmemCallbacks MatviewDirtyShmemCallbacks = {
	.request_fn = MatviewDirtyShmemRequest,
	.init_fn = MatviewDirtyShmemInit,
};

/* ----------
 * MatviewDirtyShmemRequest
 * ----------
 */
static void
MatviewDirtyShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "MatviewDirtyState",
					   .size = sizeof(MatviewDirtyState),
					   .ptr = (void **) &matview_dirty_state,
		);
}

/* ----------
 * MatviewDirtyShmemInit
 * ----------
 */
static void
MatviewDirtyShmemInit(void *arg)
{
	SpinLockInit(&matview_dirty_state->lock);
	matview_dirty_state->nentries = 0;
}

/* ----------
 * matview_shmem_mark_dirty
 *   Called at commit time.  If relid is in the shmem array, sets dirty=true.
 * ----------
 */
static void
matview_shmem_mark_dirty(Oid relid)
{
	int			i;

	if (matview_dirty_state == NULL)
		return;

	SpinLockAcquire(&matview_dirty_state->lock);
	for (i = 0; i < matview_dirty_state->nentries; i++)
	{
		if (matview_dirty_state->entries[i].relid == relid)
		{
			matview_dirty_state->entries[i].dirty = true;
			break;
		}
	}
	SpinLockRelease(&matview_dirty_state->lock);
}

/* ----------
 * matview_dirty_xact_callback
 *
 * On commit (or prepare), flush xact_noted_rels to shmem.
 * On abort, discard without flushing.
 * Intermediate events (PRE_COMMIT etc.) are intentionally ignored so that
 * xact_noted_rels survives to the actual COMMIT event.
 * ----------
 */
static void
matview_dirty_xact_callback(XactEvent event, void *arg)
{
	int			i;

	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT ||
		event == XACT_EVENT_PREPARE)
	{
		for (i = 0; i < xact_noted_nrels; i++)
			matview_shmem_mark_dirty(xact_noted_rels[i]);
		xact_noted_nrels = 0;
	}
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
	{
		/* rolled back — discard without dirtying shmem */
		xact_noted_nrels = 0;
	}
}

/* ----------
 * MatviewDirtyRegisterCallback
 *   Register the xact callback once per backend session.
 * ----------
 */
void
MatviewDirtyRegisterCallback(void)
{
	if (!callback_registered)
	{
		RegisterXactCallback(matview_dirty_xact_callback, NULL);
		callback_registered = true;
	}
}

/* ----------
 * MatviewDirtyNote
 *   Called from heap write paths.  Records relid in the process-local list.
 *   No shared-memory access here — all flushing happens at commit.
 * ----------
 */
void
MatviewDirtyNote(Oid relid)
{
	int			i;

	if (matview_dirty_state == NULL)
		return;

	/* Ignore system catalogs — they are never matview source tables. */
	if (relid < FirstNormalObjectId)
		return;

	/* Ensure the xact callback is registered. */
	MatviewDirtyRegisterCallback();

	/* Dedup — don't add same relid twice. */
	for (i = 0; i < xact_noted_nrels; i++)
	{
		if (xact_noted_rels[i] == relid)
			return;
	}

	if (xact_noted_nrels < MATVIEW_DIRTY_MAX_RELS)
		xact_noted_rels[xact_noted_nrels++] = relid;
}

/* ----------
 * MatviewDirtyRegister
 *   Add 'relid' to the shared table with dirty=true.
 *   Safe to call even if already registered.
 * ----------
 */
void
MatviewDirtyRegister(Oid relid)
{
	int			i;

	if (matview_dirty_state == NULL)
		return;

	SpinLockAcquire(&matview_dirty_state->lock);

	/* Already present? */
	for (i = 0; i < matview_dirty_state->nentries; i++)
	{
		if (matview_dirty_state->entries[i].relid == relid)
		{
			SpinLockRelease(&matview_dirty_state->lock);
			return;
		}
	}

	/* Insert new entry if space available. */
	if (matview_dirty_state->nentries < MATVIEW_DIRTY_MAX_RELS)
	{
		MatviewDirtyEntry *e =
			&matview_dirty_state->entries[matview_dirty_state->nentries++];

		e->relid = relid;
		e->dirty = true;		/* conservative: first refresh always runs */
	}

	SpinLockRelease(&matview_dirty_state->lock);
}

/* ----------
 * MatviewDirtyIsClean
 *   Returns true only if relid is registered AND dirty=false.
 * ----------
 */
bool
MatviewDirtyIsClean(Oid relid)
{
	int			i;
	bool		result = false;

	if (matview_dirty_state == NULL)
		return false;

	SpinLockAcquire(&matview_dirty_state->lock);
	for (i = 0; i < matview_dirty_state->nentries; i++)
	{
		if (matview_dirty_state->entries[i].relid == relid)
		{
			result = !matview_dirty_state->entries[i].dirty;
			break;
		}
	}
	SpinLockRelease(&matview_dirty_state->lock);

	return result;
}

/* ----------
 * MatviewDirtyMarkClean
 *   Called after a successful refresh.
 * ----------
 */
void
MatviewDirtyMarkClean(Oid relid)
{
	int			i;

	if (matview_dirty_state == NULL)
		return;

	SpinLockAcquire(&matview_dirty_state->lock);
	for (i = 0; i < matview_dirty_state->nentries; i++)
	{
		if (matview_dirty_state->entries[i].relid == relid)
		{
			matview_dirty_state->entries[i].dirty = false;
			break;
		}
	}
	SpinLockRelease(&matview_dirty_state->lock);
}
