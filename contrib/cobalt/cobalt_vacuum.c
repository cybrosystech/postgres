/*-------------------------------------------------------------------------
 *
 * cobalt_vacuum.c
 *	  VACUUM support for cobalt.
 *
 * Dead heap tuples are not removed from the index yet.  This is safe because
 * every cobalt scan is lossy and the executor rechecks the qual against the
 * heap tuple found at each TID, so a stale posting (including one whose TID
 * has been reused) can only cost time, never produce a wrong row.  The space
 * is reclaimed by REINDEX.
 *
 * IDENTIFICATION
 *	  contrib/cobalt/cobalt_vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cobalt.h"
#include "commands/vacuum.h"

IndexBulkDeleteResult *
cobaltbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state)
{
	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);
	return stats;
}

IndexBulkDeleteResult *
cobaltvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	CobaltMetaPageData meta;

	if (info->analyze_only)
		return stats;

	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	cobalt_read_meta(info->index, &meta);
	stats->num_pages = RelationGetNumberOfBlocks(info->index);
	stats->num_index_tuples = (double) meta.ntuples;
	return stats;
}
