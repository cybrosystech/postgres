# Zheap Phase 2 Progress Tracker

**Started**: 2026-05-25  
**Target completion**: 2026-07-04 (6 weeks) — 2026-07-18 (10 weeks)  
**Team**: 1-2 engineers  
**Current phase**: Phase 2a (Foundation)

---

## Phase 2a: Foundation (Target: Week 2)

### Task 2a.1: RelFileNumber Abstraction Layer

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Create `src/include/access/zheap_relfile.h`
- [ ] Implement `ZHeapGetTablespace()` helper
- [ ] Implement `ZHeapGetDatabase()` helper
- [ ] Implement `ZHeapGetRelNumber()` helper
- [ ] Implement `ZHeapMakeRelFileLocator()` helper
- [ ] Header compiles without errors
- [ ] Add to `src/backend/access/Makefile.am`

**Estimated effort**: 2-3 days  
**Owner**: TBD  
**Blocker**: None  
**Notes**: 
- Start here — this is the foundation for all other changes
- Keep helpers simple (inline functions preferred)
- Test: `gcc -c -I src/include src/include/access/zheap_relfile.h`

---

### Task 2a.2: SMgrRelation API Updates

**Status**: ⬜ NOT STARTED  

**Subtasks**:
- [ ] Grep for all `smgr_rnode` references in `src/backend/access/zheap/*.c`
- [ ] Create `src/backend/access/zheap/API_PORTING_CHECKLIST.md`
- [ ] Document which files have how many changes needed
- [ ] Update `zheapam.c` (estimated ~40 changes)
- [ ] Update `zheapam_handler.c` (estimated ~8 changes)
- [ ] Update `zheapam_visibility.c` (estimated ~5 changes)
- [ ] Update `tpd.c` (estimated ~12 changes)
- [ ] Update other zheap files (estimated ~15 changes)
- [ ] Final verification: `grep -r "smgr_rnode" src/backend/access/zheap/` returns 0

**Estimated effort**: 3-4 days  
**Owner**: TBD  
**Blocker**: 2a.1 must be complete  
**Notes**:
- Use helpers from zheap_relfile.h exclusively
- Document each change in the checklist so Phase 3 team can follow the pattern
- This is repetitive work — good for parallel processing if 2 engineers

---

### Task 2a.3: TableAmRoutine Compatibility

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Research PG12 TableAmRoutine struct (~35 slots)
- [ ] Research PG19 TableAmRoutine struct (~70 slots)
- [ ] Create slot mapping document
- [ ] Create `src/backend/access/zheap_stub/zheapam_compat.c`
- [ ] Implement `GetZHeapamTableAmRoutine()` function
- [ ] Verify all PG19 slots are covered (either real zheap or heap default)
- [ ] zheapam_compat.c compiles without errors

**Estimated effort**: 2-3 days  
**Owner**: TBD  
**Blocker**: None (but should be done before Phase 2c linking)  
**Notes**:
- This is a bridge — real zheapam_handler.c won't work until Phase 2 is complete
- The compat layer ensures we don't get silent NULL dereferences
- Key: document the mapping for future reference

---

### Task 2a.4: Build System Integration

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Update `src/backend/access/Makefile` to include `zheap/` in SUBDIRS
- [ ] Verify `src/backend/access/zheap/Makefile` (or create if needed)
- [ ] Add all 16 zheap/*.c files to OBJS in Makefile
- [ ] Test: `make -C src/backend/access/zheap clean` succeeds
- [ ] Test: `make -C src/backend/access/zheap` (will fail, expected)

**Estimated effort**: 1 day  
**Owner**: TBD  
**Blocker**: None  
**Notes**:
- Don't try to link yet — just set up the build infrastructure
- Expected output: many compilation errors (normal at this stage)

---

## Phase 2b: Core API Porting (Target: Week 6)

### Task 2b.1: SMgrRelation API Finalization

**Status**: ⬜ NOT STARTED (blocks on 2a.2)

**Subtasks**:
- [ ] All smgr_rnode references replaced (carry over from 2a.2)
- [ ] Test compilation of zheapam.c alone
- [ ] Verify no new smgr_rnode errors appear

**Estimated effort**: 1 day  
**Owner**: TBD  
**Blocker**: 2a.2 completion  

---

### Task 2b.2: Visibility Map API

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Reference PG19 `src/include/access/visibilitymap.h`
- [ ] Grep: `grep -rn "visibilitymap_" src/backend/access/zheap/*.c`
- [ ] Document all calls and their current signatures
- [ ] Update each call to match PG19 signature
- [ ] Focus on: zheapam_visibility.c, prunezheap.c, zvacuumlazy.c
- [ ] Test compilation

**Estimated effort**: 2-3 days  
**Owner**: TBD  
**Blocker**: 2b.1 should be progressing

---

### Task 2b.3: WAL Resource Manager

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Check if `RM_UNDOLOG_ID` exists in `src/include/access/rmgr.h`
- [ ] Check if `undolog_redo` callback exists in `src/backend/access/rmgrdesc/undologdesc.c`
- [ ] Verify rmgr table in `src/backend/access/transam/rmgr.c` includes undo
- [ ] If any missing, implement stub
- [ ] Test: rmgr compiles

**Estimated effort**: 1-2 days  
**Owner**: TBD  
**Blocker**: None (parallel with 2b.2)

---

### Task 2b.4: Lock Tranche Registration

**Status**: ⬜ NOT STARTED

**Subtasks**:
- [ ] Check if `LWTRANCHE_UNDOLOG` defined in `src/include/storage/lwlock.h`
- [ ] If missing, add definition (next available #define)
- [ ] Update `src/backend/storage/lmgr/lwlock.c` to register tranche
- [ ] Test: lock manager compiles

**Estimated effort**: 1 day  
**Owner**: TBD  
**Blocker**: None (parallel with 2b.2-3)

---

## Phase 2c: Integration & Linking (Target: Week 10)

### Task 2c.1: First Compilation Attempt

**Status**: ⬜ NOT STARTED (blocks on 2b completion)

**Subtasks**:
- [ ] `make -C src/backend/access/zheap clean`
- [ ] `make -C src/backend/access/zheap 2>&1 | tee /tmp/zheap_compile.log`
- [ ] Extract errors: `grep error /tmp/zheap_compile.log | sort | uniq -c`
- [ ] Fix first batch of errors (repeat)
- [ ] Target: 0 compilation errors in zheap/

**Estimated effort**: 3-5 days (iterative)  
**Owner**: TBD  
**Blocker**: All 2b tasks must be complete

---

### Task 2c.2: Undefined Reference Resolution

**Status**: ⬜ NOT STARTED (blocks on 2c.1 success)

**Subtasks**:
- [ ] `make -C src/backend/access 2>&1 | grep "undefined reference" | tee /tmp/undefined.log`
- [ ] For each undefined symbol, check: is it in undo/ or a stub?
- [ ] Create stubs in `src/backend/access/zheap_stub/` for missing core functions
- [ ] Target: 0 undefined references

**Estimated effort**: 2-3 days (iterative)  
**Owner**: TBD  
**Blocker**: 2c.1 success

---

### Task 2c.3: Full Binary Build

**Status**: ⬜ NOT STARTED (blocks on 2c.2 success)

**Subtasks**:
- [ ] `cd postgres && make clean && make -j4 2>&1 | tee /tmp/full_build.log`
- [ ] Check for errors: `grep error /tmp/full_build.log`
- [ ] Verify binary: `file bin/postgres`
- [ ] Test: `bin/postgres --version`
- [ ] Success criteria: binary runs without errors

**Estimated effort**: 1 day + testing  
**Owner**: TBD  
**Blocker**: 2c.2 success

---

## Risk Log

### Risk 1: API changes beyond ~320 documented

**Severity**: HIGH  
**Probability**: MEDIUM (maybe 30% chance)  
**Mitigation**: Maintain `UNKNOWN_APIS.md` file documenting surprises

**Entry**: [TBD]

---

### Risk 2: Circular header dependencies

**Severity**: MEDIUM  
**Probability**: LOW (maybe 20% chance)  
**Mitigation**: Keep zheap_relfile.h minimal, forward declarations

**Entry**: [TBD]

---

### Risk 3: Core PG integration needed earlier than Phase 4

**Severity**: HIGH  
**Probability**: LOW (maybe 15% chance)  
**Mitigation**: Document required hooks now, create stubs for Phase 2

**Entry**: [TBD]

---

## Decision Log

**Decision 1**: [2026-05-25] Commit to full zheap port (12-15 months)
- **Rationale**: High strategic value for Odoo workloads, justified investment
- **Approval**: User (engineer)
- **Contingency plan**: Monthly review; if progress < 50% at week 8, reassess

**Decision 2**: TBD

---

## Metrics & Milestones

### Phase 2 Milestones

- **Week 2 (2026-05-31)**: Phase 2a complete (build system, helpers, stubs)
- **Week 6 (2026-06-28)**: Phase 2b complete (API porting, compilation succeeds)
- **Week 10 (2026-07-25)**: Phase 2c complete (linking succeeds, binary built)

### Success Metrics

- [ ] **Code**: 320 API changes made across zheap/ files
- [ ] **Compilation**: `make -C src/backend/access/zheap` returns 0 errors
- [ ] **Linking**: `make -j4` completes with no undefined references
- [ ] **Binary**: `postgres --version` succeeds
- [ ] **Documentation**: Phase 2 API mapping documented for Phase 3

---

## Team Assignment

| Task | Owner | Backup | Notes |
|---|---|---|---|
| 2a.1 | TBD | TBD | Foundation — choose most experienced |
| 2a.2 | TBD | TBD | Repetitive — good for 2 engineers in parallel |
| 2a.3 | TBD | TBD | Deep PG knowledge required |
| 2a.4 | TBD | TBD | Quick task — anyone can do |
| 2b.1-4 | TBD | TBD | Ongoing iteration across all tasks |
| 2c.1-3 | TBD | TBD | Final push — full team debugging |

---

## Weekly Status Template

Copy and fill in each Friday:

```markdown
## Week N (2026-XX-XX to 2026-XX-XX)

### Completed
- [ ] Task 2a.1: ...
- [ ] Task 2a.2: ... (50% done)

### Blockers
- [x] Circular dependency in zheap_relfile.h ← RESOLVED
- [ ] Unknown API in tpd.c line 145 ← INVESTIGATING

### Burndown
- Planned: 4 days
- Actual: 3.5 days
- Variance: +0.5 days (ahead)

### Next Week Plan
- Finish 2a.2 (2 days remaining)
- Start 2b.1 (SMgrRelation updates)

### Notes
- Discovered that older branch had similar porting; found 50 lines of reference code
- Build system integration took less time than expected (0.5 days vs 1 day)
```

---

## How to Use This Document

1. **Weekly**: Fill in status, blockers, burndown each Friday
2. **On blocker**: Add entry to Risk Log or Decision Log
3. **On completion**: Check off subtasks, move to next task
4. **At phase end**: Document what was learned, create Phase 3 plan

---

## Phase 2 vs Phase 3 Handoff Checklist

When Phase 2 is complete, Phase 3 team needs:

- [ ] `ZHEAP_PHASE2_PLAN.md` (this document) marked 100% complete
- [ ] `API_PORTING_CHECKLIST.md` documenting all 320 changes
- [ ] `UNKNOWN_APIS.md` documenting any surprises
- [ ] Copy of `zheap_relfile.h` (the compatibility layer)
- [ ] Binary that compiles and runs (but zheap operations still error)
- [ ] List of remaining stubbed functions (for Phase 3-4 to implement)

---

**Phase 2 Target: WEEK 10 COMPLETION (2026-07-25)**

Good luck! 🚀
