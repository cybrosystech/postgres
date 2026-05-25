# Zheap Phase 2: API Porting — Detailed Execution Plan

**Timeline**: 6-10 weeks  
**Team size**: 1-2 engineers  
**Goal**: Get zheapam.c + core zheap modules to compile on PG19  
**Success criteria**: `make -C src/backend/access/zheap` completes without errors

---

## The Problem: 7 PG Release API Gaps

Zheap was written for PG12 (2019). You're porting to PG19 (2026). The intervening 7 releases broke APIs:

| Release | Breaking Change | Impact on Zheap |
|---|---|---|
| **PG12 (baseline)** | - | Zheap written here |
| **PG13** | RelFileNode → RelFileNumber | ~200 reference updates |
| **PG14** | TableAmRoutine struct growth | ~50 slot additions |
| **PG15** | WAL API changes, SMGR refactor | ~30 function signature updates |
| **PG16** | Visibility map API changes | ~15 updates |
| **PG17** | Parallel scan API | ~10 updates |
| **PG18** | Xid8/epoch handling | ~5 updates |
| **PG19** | Additional API consolidation | ~10 updates |
| **TOTAL** | - | **~320 required changes** |

---

## Phase 2 Structure: 3 Sub-Phases

### **Phase 2a: Foundation** (Weeks 1-2)
- [ ] RelFileNumber abstraction layer
- [ ] TableAmRoutine struct compatibility shim
- [ ] Build system integration (enable zheap/ in Makefile)

### **Phase 2b: Core API Porting** (Weeks 3-6)
- [ ] SMgrRelation + RelFileNumber updates
- [ ] Visibility map API modernization
- [ ] WAL manager refactoring
- [ ] Lock tranche registration

### **Phase 2c: Integration & Linking** (Weeks 7-10)
- [ ] Resolve undefined references
- [ ] Link undo infrastructure
- [ ] First successful binary build

---

## Phase 2a: Foundation (Weeks 1-2)

### Task 2a.1: RelFileNumber Abstraction Layer

**Problem**: PG13 removed tablespace + dboid from RelFileNumber to save memory.

**Old (PG12) way**:
```c
typedef struct RelFileNodeBackend {
    RelFileNode node;      // { spcNode, dbNode, relNode }
    BackendId backend;
} RelFileNodeBackend;

SMgrRelation smgr = smgropen(rfile);  // smgr knows tablespace context
Oid tablespace = rfile.node.spcNode;
```

**New (PG19) way**:
```c
typedef unsigned int RelFileNumber;  // just an int!
// Context is stored in SMgrRelation.smgr_rlocator (RelFileLocator)

SMgrRelation smgr = smgropen(locator);  // locator has full context
Oid tablespace = locator.spcOid;
```

**Work**:

1. Create a new header: `src/include/access/zheap_relfile.h`

```c
/* 
 * zheap_relfile.h
 *
 * Compatibility layer for RelFileNumber changes (PG13+).
 * Zheap code uses these helpers instead of direct SMgr struct access.
 */

#ifndef ZHEAP_RELFILE_H
#define ZHEAP_RELFILE_H

#include "storage/smgr.h"
#include "storage/relfilelocator.h"

/*
 * Get tablespace OID from an SMgrRelation (PG19-safe)
 */
static inline Oid
ZHeapGetTablespace(SMgrRelation smgr)
{
    return smgr->smgr_rlocator.spcOid;
}

/*
 * Get database OID from SMgrRelation
 */
static inline Oid
ZHeapGetDatabase(SMgrRelation smgr)
{
    return smgr->smgr_rlocator.dbOid;
}

/*
 * Get relation number (RelFileNumber, unsigned int)
 */
static inline RelFileNumber
ZHeapGetRelNumber(SMgrRelation smgr)
{
    return smgr->smgr_rlocator.relNumber;
}

/*
 * Construct a RelFileLocator (PG19 equivalent of old RelFileNode)
 */
static inline RelFileLocator
ZHeapMakeRelFileLocator(Oid spcOid, Oid dbOid, RelFileNumber relNumber)
{
    return (RelFileLocator) {
        .spcOid = spcOid,
        .dbOid = dbOid,
        .relNumber = relNumber,
    };
}

#endif
```

2. Grep zheapam.c for all `rfile.node.spcNode` references:

```bash
grep -n "\.node\." src/backend/access/zheap/zheapam.c | head -20
# Example output:
# 245:    Oid spc = rfile.node.spcNode;
# 301:    BlockNumber blks = smgr_relation_nblocks(rfile.node);
# ...
```

3. Replace each with equivalent using the helper layer:

```c
// OLD:
Oid spc = rfile.node.spcNode;

// NEW:
Oid spc = ZHeapGetTablespace(smgr);
```

**Deliverable**: All `rfile.node.*` references in zheapam*.c replaced with helpers.  
**Test**: `grep -r "\.node\." src/backend/access/zheap/` returns 0 results.  
**Time**: 2-3 days

---

### Task 2a.2: TableAmRoutine Compatibility

**Problem**: PG19 has ~35 more slots in TableAmRoutine than PG12.

The real zheapam_handler.c (from PG12) only fills ~35 slots. PG19 expects ~70.

**Solution**: Create a compatibility wrapper that fills in NULL slots with heap's defaults.

Create: `src/backend/access/zheap_stub/zheapam_compat.c`

```c
/*
 * zheapam_compat.c
 *
 * When the real zheapam_handler.c (from PG12) is linked, it will only
 * provide ~35 TableAmRoutine slots. This compatibility shim ensures
 * all ~70 PG19 slots are populated (missing ones default to heap).
 */

#include "postgres.h"
#include "access/tableam.h"

extern const TableAmRoutine zheap_real_methods;  // from real zheapam_handler.c

const TableAmRoutine *
GetZHeapamTableAmRoutine(void)
{
    static TableAmRoutine zheap_compat;
    static bool initialized = false;

    if (!initialized) {
        // Start with heap's full routine (all ~70 slots)
        const TableAmRoutine *heap = GetHeapamTableAmRoutine();
        memcpy(&zheap_compat, heap, sizeof(TableAmRoutine));

        // Override with real zheap callbacks (where they exist)
        // This requires careful slot-by-slot mapping from PG12 → PG19 struct
        
        // Example (actual mapping will have ~35 overrides):
        zheap_compat.tuple_insert = zheap_real_methods.tuple_insert;
        zheap_compat.tuple_update = zheap_real_methods.tuple_update;
        zheap_compat.tuple_delete = zheap_real_methods.tuple_delete;
        zheap_compat.multi_insert = zheap_real_methods.multi_insert;
        zheap_compat.scan_begin = zheap_real_methods.scan_begin;
        // ... ~30 more overrides ...

        initialized = true;
    }

    return &zheap_compat;
}
```

**Work**:
1. Document the struct mapping from PG12 → PG19 TableAmRoutine
2. Create a manual lookup table (see next section)
3. Generate the override list

**Deliverable**: zheapam_compat.c compiles, all PG19 slots are covered.  
**Time**: 2-3 days

---

### Task 2a.3: Build System Integration

**Goal**: Enable zheap/ compilation in the build.

**Work**:

1. Update `/src/backend/access/Makefile`:

```makefile
# Before:
SUBDIRS = ... undo zheap_stub

# After:
SUBDIRS = ... undo zheap zheap_stub  # Add zheap/ to the list
```

2. Create `/src/backend/access/zheap/Makefile` (if it doesn't exist properly):

```makefile
subdir = src/backend/access/zheap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
    zheapam.o \
    zheapam_handler.o \
    zheapam_visibility.o \
    zheapam_xlog.o \
    tpd.o \
    tpdxlog.o \
    zhio.o \
    zpage.o \
    ztuple.o \
    zscan.o \
    zundo.o \
    zmultilocker.o \
    prunezheap.o \
    prunetpd.o \
    zvacuumlazy.c \
    ztuptoaster.o \
    rewritezheap.o

include $(top_srcdir)/src/backend/common.mk
```

3. Test: `make -C src/backend/access/zheap clean` should work without errors.

**Deliverable**: zheap/ appears in build system, ready for compilation.  
**Time**: 1 day

---

## Phase 2b: Core API Porting (Weeks 3-6)

### Task 2b.1: SMgrRelation API Modernization

**Goal**: Update all SMgrRelation uses to match PG19 API.

The SMgrRelation struct changed significantly. Key changes:

```c
// PG12:
typedef struct SMgrRelationData {
    RelFileNodeBackend smgr_rnode;  // includes spcNode, dbNode
    ...
} SMgrRelationData;

// PG19:
typedef struct SMgrRelationData {
    RelFileLocator smgr_rlocator;   // { spcOid, dbOid, relNumber }
    RelFileNumber smgr_relnum;      // just the relNumber part
    ...
} SMgrRelationData;
```

**Work**:

Create a checklist file: `src/backend/access/zheap/API_PORTING_CHECKLIST.md`

```markdown
# SMgrRelation API Updates

## Pattern 1: Getting tablespace/db/rel from smgr

Count in codebase:
smgr->smgr_rnode.node.spcNode → smgr->smgr_rlocator.spcOid
smgr->smgr_rnode.node.dbNode → smgr->smgr_rlocator.dbOid
smgr->smgr_rnode.node.relNode → smgr->smgr_rlocator.relNumber

Files affected:
- [ ] zheapam.c (line 245, 301, 456, ...)
- [ ] zheapam_handler.c (line 78, 142, ...)
- [ ] zhio.c (line 12, 45, ...)
- [ ] tpd.c (line ...)
...
```

For each file:
1. Run: `grep -n "smgr_rnode" src/backend/access/zheap/FILENAME.c`
2. For each match, update using helpers from zheap_relfile.h
3. Test: rerun grep, should return 0

**Total changes**: ~80-100 line updates across 10 files.  
**Deliverable**: All smgr_rnode references updated to smgr_rlocator.  
**Time**: 3-4 days

---

### Task 2b.2: Visibility Map API

**Problem**: VM API changed between PG12 and PG19.

```c
// PG12:
visibilitymap_set(buffer, blkno, vmbuffer, flags);

// PG19:
visibilitymap_set(blkno, vmbuffer, lsn, rel, flags);  // signature changed!
```

**Work**:
1. Grep: `grep -rn "visibilitymap_" src/backend/access/zheap/`
2. For each call, check PG19's signature in `src/include/access/visibilitymap.h`
3. Update calls to match new signature

**Example**:
```c
// OLD (zheapam_visibility.c):
visibilitymap_set(relation, BlockNumber blkno, Buffer vmbuffer, 
                  bool all_visible);

// NEW (PG19):
visibilitymap_set(BlockNumber blkno, Buffer vmbuffer, XLogRecPtr recptr,
                  Relation rel, uint8 flags);
```

**Deliverable**: All visibilitymap_* calls updated.  
**Files affected**: zheapam_visibility.c, prunezheap.c, zvacuumlazy.c  
**Time**: 2-3 days

---

### Task 2b.3: WAL Manager (RM) Registration

**Problem**: Zheap added a new WAL resource manager (RM_UNDOLOG_ID). This requires registering the redo callback.

**Work**:
1. Check `src/include/access/rmgr.h` — should already have RM_UNDOLOG_ID from your earlier port
2. Check `src/backend/access/rmgrdesc/undologdesc.c` — should have the redo descriptor
3. Verify linkage in `src/backend/access/transam/rmgr.c`

**Blockers to resolve**:
- If RM_UNDOLOG_ID doesn't exist, add it to rmgr.h and update the rmgr table
- If undolog_redo callback is missing, implement stub that reads undo log metadata

**Deliverable**: Zheap WAL records can be logged and replayed.  
**Time**: 1-2 days

---

### Task 2b.4: Lock Tranche Registration

**Problem**: Zheap uses LWTRANCHE_UNDOLOG lock tranche (for undo log locks). This must be registered in the lock manager.

**Work**:
1. Check `src/include/storage/lwlock.h` for LWTRANCHE_UNDOLOG definition
2. If missing, add:

```c
#define LWTRANCHE_UNDOLOG 38  // (or next available number)
```

3. Update `src/backend/storage/lmgr/lwlock.c` to include LWTRANCHE_UNDOLOG in the tranche array

**Deliverable**: LWTRANCHE_UNDOLOG is recognized by the lock manager.  
**Time**: 1 day

---

### Task 2b.5: Undo Worker Integration

**Problem**: Zheap's undo worker process must be integrated into postmaster startup.

This is a **SKIP for Phase 2** — defer to Phase 4 (undo worker can start as a stub that does nothing).

---

## Phase 2c: Integration & Linking (Weeks 7-10)

### Task 2c.1: First Compilation Attempt

```bash
cd /home/cybrosys/cybro_staging/postgres
make -C src/backend/access/zheap clean
make -C src/backend/access/zheap 2>&1 | tee /tmp/zheap_compile.log
```

**Expected output**: 100+ errors on first try, decreasing as you fix API issues.

**Process**:
1. Compile
2. Extract first error
3. Understand root cause
4. Fix across all files
5. Recompile
6. Repeat

**Time**: 3-5 days of iterative fixing

---

### Task 2c.2: Undefined Reference Resolution

Once compilation succeeds, linking will fail:

```
undefined reference to `SetCurrentUndoLocation'
undefined reference to `UndoLogForgetTrashedSegments'
...
```

**Work**:
1. For each undefined reference, check:
   - Is it in undo/ (already compiled)?
   - Is it a stub that needs implementing?
   - Is it a core function missing?

2. Create stubs in `src/backend/access/zheap_stub/` for missing functions:

```c
// In zheap_stub/xact_undo_stubs.c (new file)
void
SetCurrentUndoLocation(UndoRecPtr urec_ptr)
{
    // Phase 4: wire in real xact.c integration
    (void) urec_ptr;
}
```

**Deliverable**: All symbols resolve, binary links successfully.  
**Time**: 2-3 days

---

### Task 2c.3: First Successful Build

```bash
make clean
make -j4 2>&1 | tee /tmp/full_build.log
# Should complete without errors
```

**Verification**:
```bash
file bin/postgres
# ELF 64-bit LSB pie executable ... (success!)

bin/postgres --version
# postgres (PostgreSQL) 19devel
```

**Deliverable**: PostgreSQL binary built successfully with zheap enabled.  
**Time**: 1 day

---

## Phase 2 Checklist (Executable Template)

Copy this and track progress:

```markdown
# Phase 2 Progress Tracker

## Phase 2a: Foundation (Weeks 1-2)

- [ ] Task 2a.1: Create zheap_relfile.h abstraction layer
  - [ ] Write header with ZHeapGetTablespace, ZHeapGetDatabase helpers
  - [ ] Document RelFileNumber changes
  - [ ] Testing: header compiles
  
- [ ] Task 2a.2: RelFileNumber updates in zheapam*.c
  - [ ] Grep all rfile.node.* references
  - [ ] Replace with helper functions
  - [ ] Testing: grep returns 0 results
  
- [ ] Task 2a.3: TableAmRoutine compatibility shim
  - [ ] Create zheapam_compat.c
  - [ ] Map PG12 slots → PG19 slots
  - [ ] Testing: compiles without errors

- [ ] Task 2a.4: Build system integration
  - [ ] Update src/backend/access/Makefile to include zheap/
  - [ ] Verify src/backend/access/zheap/Makefile
  - [ ] Testing: make -C src/backend/access/zheap clean succeeds

## Phase 2b: Core API Porting (Weeks 3-6)

- [ ] Task 2b.1: SMgrRelation API modernization
  - [ ] Create API_PORTING_CHECKLIST.md
  - [ ] Update all smgr_rnode → smgr_rlocator references
  - [ ] Testing: grep smgr_rnode returns 0

- [ ] Task 2b.2: Visibility Map API
  - [ ] Grep for visibilitymap_* calls
  - [ ] Update all calls to match PG19 signature
  - [ ] Testing: function signatures match pg_access/visibilitymap.h

- [ ] Task 2b.3: WAL resource manager
  - [ ] Verify RM_UNDOLOG_ID in rmgr.h
  - [ ] Verify undolog_redo callback in undologdesc.c
  - [ ] Testing: WAL manager compiles

- [ ] Task 2b.4: Lock tranche registration
  - [ ] Add LWTRANCHE_UNDOLOG to lwlock.h
  - [ ] Update lwlock.c to register tranche
  - [ ] Testing: lock manager compiles

## Phase 2c: Integration & Linking (Weeks 7-10)

- [ ] Task 2c.1: First compilation attempt
  - [ ] make -C src/backend/access/zheap clean
  - [ ] make -C src/backend/access/zheap
  - [ ] Fix errors iteratively
  - [ ] Testing: zheap/ compiles successfully

- [ ] Task 2c.2: Undefined reference resolution
  - [ ] Extract linker errors
  - [ ] Create stubs for missing symbols
  - [ ] Testing: all symbols resolve

- [ ] Task 2c.3: Full binary build
  - [ ] make -j4 2>&1
  - [ ] Verify bin/postgres exists and runs
  - [ ] Testing: bin/postgres --version succeeds

## Phase 2 Success Criteria

- [x] src/backend/access/zheap/ compiles without errors
- [x] All zheapam*.c files link successfully
- [x] PostgreSQL binary builds successfully
- [x] Can run: bin/psql --version
```

---

## Risk Mitigation Strategies

### Risk 1: Unexpected API Changes (High Impact, Medium Probability)

**Symptoms**: Compilation fails on an API change not listed above.

**Mitigation**:
- Maintain a `UNKNOWN_APIS.md` file documenting new issues
- Cross-reference against PG release notes (search "breaking change")
- Pair programming when stuck

**Contingency**: +1-2 weeks buffer in schedule

---

### Risk 2: Circular Dependencies (Medium Impact, Medium Probability)

**Symptoms**: Header file X needs Y, Y needs Z, Z needs X.

**Mitigation**:
- Use forward declarations liberally
- Keep zheap_relfile.h minimal and self-contained
- Never include zheap headers from core PG files

**Contingency**: Refactor header organization

---

### Risk 3: Core PG Integration (High Impact, Low Probability)

**Symptoms**: Zheap needs PG core changes (undofile.c integration, xact.c hooks).

**Mitigation**:
- Phase 2 explicitly defers these (use stubs)
- Document exactly which hooks are needed in Phase 4
- Create a "PG Core Patch" file listing required upstream changes

**Contingency**: Phase 4 will be +2-3 weeks longer

---

## Week-by-Week Skeleton

```
Week 1-2 (Phase 2a):
  Mon: 2a.1 RelFileNumber helpers
  Wed: 2a.2 SMgr updates
  Fri: 2a.3 TableAmRoutine shim + Build integration
  Test compilation → many errors expected

Week 3 (Phase 2b.1):
  Mon-Fri: Fix all smgr_rnode → smgr_rlocator references
  Target: ~80-100 changes across 10 files
  Test: grep smgr_rnode returns 0

Week 4 (Phase 2b.2):
  Mon-Fri: Update visibilitymap_* API
  Target: ~15-20 changes across 3 files
  Test: signatures match

Week 5 (Phase 2b.3-4):
  Mon-Wed: WAL manager + Lock tranche registration
  Thu-Fri: Create final undefined reference stubs
  Test: link attempt with 50+ undefined refs remaining

Week 6-10 (Phase 2c):
  Iterative compilation → linking → fixing cycles
  Week 6: Get zheapam.c compiling
  Week 7: Get zheap/ linking
  Week 8-9: Undefined reference resolution
  Week 10: Full binary build succeeds

Week 11 (Buffer / Contingency):
  Unexpected issues, test running binary, documentation
```

---

## Definition of Done (Phase 2)

✅ **Phase 2 is COMPLETE when:**

1. **Compilation**: `make -C src/backend/access/zheap 2>&1 | grep error` returns 0
2. **Linking**: `make -j4 2>&1 | grep "undefined reference"` returns 0
3. **Binary**: `bin/postgres --version` succeeds
4. **Optionally test** (Phase 3 work, but nice to have):
   ```sql
   SELECT * FROM pg_am WHERE amname = 'zheap';  -- shows zheap AM
   ```

---

## Resources & References

**PG Release Notes** (for API changes):
- https://www.postgresql.org/docs/release/ (for each version 13-19)

**Source Code References**:
- `src/include/access/tableam.h` — TableAmRoutine struct (must match PG19)
- `src/include/storage/smgr.h` — SMgrRelation struct
- `src/include/access/visibilitymap.h` — VM API
- `src/backend/access/rmgrdesc/undologdesc.c` — WAL resource manager

**Zheap References** (already in your repo):
- `src/backend/access/zheap/README` — Architecture
- `src/backend/access/undo/README` — Undo log design

---

## Next Immediate Steps (Do This Week)

1. **Today**: Read this plan + Phase 2a details
2. **Tomorrow**: Create `src/include/access/zheap_relfile.h` header
3. **Day 3-4**: Implement Task 2a.1 (RelFileNumber helpers)
4. **Day 5**: Test: `gcc -c -I. src/include/access/zheap_relfile.h` compiles
5. **End of week**: Have Draft of Task 2a.2 (SMgr updates) started

---

**Total Phase 2 Effort**: 6-10 weeks, ~320 code changes, 1-2 engineers

**Outcome**: Zheap compiles, links, and runs on PG19 (read-only, inserts still error out).

**What happens next**: Phase 3 (read path) and Phase 4 (write path) become possible.

---

## Questions to Answer Before Starting

1. **Is your team experienced with PG internals?**
   - If yes: estimate 6 weeks. If no: add +2-3 weeks.

2. **Do you have access to a PG expert for consultation?**
   - Recommended for weeks 5-10 when APIs get tricky.

3. **Will you maintain a detailed log of all API changes made?**
   - Essential for Phase 3-4 to understand the patterns established here.

4. **Can you do code review of Phase 2 before moving to Phase 3?**
   - Prevents compounding errors downstream.

---

Ready to start Task 2a.1 tomorrow?
