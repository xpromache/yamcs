# On-board scheduler: developer notes

User documentation: `docs/server-manual/services/instance/onboard-scheduler.rst`. This file covers what is not
there: the design decisions, the internal model, the adapter SPI and the open work.

## Design decisions

- **Own store, not timeline items.** Entries are kept in their own table (`ScheduleEntryDb`), not as
  `TimelineItem`s: an entry has two independent axes (ground intent and on-board state) plus an upload history and a
  status history, which do not fit the timeline model. Showing the entries in a timeline band should be done by
  registering a read-only timeline source (not done yet).
- **Source of truth.** An entry stores the MDB command name and its argument values, not a binary. The command is
  encoded when it is uploaded; the binary actually sent is frozen in the command history record of that upload.
- **One service per on-board scheduler.** The service name is the scheduler name.
- **The ground model is the master**, but only for the entries it knows. Sync never deletes on-board items that Yamcs
  does not know (foreign items); it reports them. An option may later allow deleting them.
- **No button for what the ground can compute.** Pending changes are recomputed on the ground whenever an entry
  changes. Read back and sync are the only actions that talk to the spacecraft and they fail without a link.
- **One entry = one on-board activity.** An entry that is on board is never edited; it is replaced (new entry +
  deletion of the old one), so an entry has at most one current ref.

## Entry model

| Field | Meaning |
|---|---|
| `id` | UUID, stable for the life of the entry |
| `command`, `args` | qualified MDB command name, argument values |
| `releaseTime` | absolute release time (Yamcs time) |
| `options` | adapter-specific options, validated against `SchedulerAdapter.getEntryOptionsSpec()` (PUS: `subScheduleId`, `groupId`) |
| `description`, `tags` | free text |
| `revision` | incremented on each edit; `uploadedRevision` is the revision that is on board |
| `intent` | `PLANNED`, `DELETE_PENDING`, `DELETED` |
| `state` | on-board state (ordered, see the manual) |
| `ref` | adapter reference of the current on-board copy (PUS: `apid/seqCount` of the embedded TC) |
| `uploads` | upload history: command history id, ref, time, revision |
| `history` | state change log: time, reception time, source (`GROUND`, `REALTIME`, `DUMP`, `READBACK`), state, message |

Rules implemented in `ScheduleEntry` / `OnboardSchedulerService`:

- `applyStatus` only moves forward in the order
  `NOT_ONBOARD < UPLOAD_PENDING < ONBOARD < RELEASED < STARTED < COMPLETED | FAILED`; `UNKNOWN` can be upgraded by
  later reports. Reports that do not change the state are still recorded in the history.
- Only reconciliation after a read back can move an entry back (`ONBOARD -> NOT_ONBOARD` when it is missing before its
  release time).
- `cancelDeletionIfReleased`: a `DELETE_PENDING` entry that is reported released goes back to `PLANNED`.

## Adapter SPI

```java
public interface SchedulerAdapter {
    default Spec getSpec();                          // adapterArgs
    void init(SchedulerContext ctx, YConfiguration config) throws InitException;
    String getType();
    SchedulerCapabilities getCapabilities();         // readBack, entryLevelEdit, observableStates, minLeadTime
    default Spec getEntryOptionsSpec();              // entry options, with the defaults applied on board
    default Set<String> getFilterableOptions();      // options that partition the schedule (UI filters)
    default String getRefLabel();                    // column label in the UI, e.g. "APID/Seq"
    default String getRefDescription();
    void start();
    void stop();
    CompletableFuture<OnboardSnapshot> readOnboard(SyncContext ctx);
    SyncPlan planSync(List<ScheduleEntry> entries, OnboardSnapshot snapshot, long cutover);
}
```

- `SchedulerContext` is what the engine offers to the adapter for its whole life: instance, scheduler name,
  processor, executor, and `reportStatus(ref, time, source, state, message)` for status from telemetry. The engine
  resolves the ref to an entry (see below).
- `SyncContext` is offered during a read back or sync: the user who started it, `sendCommand`, `uploadEntry` (issues
  the entry's own command with extra options, so each upload has its own command history record with the MDB
  verifiers disabled), `setUploadRef`, `confirmDeleted`, `uploadFailed`.
- `SyncPlan` is a list of `SyncStep`s plus warnings. Each step has a human-readable description, the affected entry
  ids, and `execute(SyncContext)` returning a future. Steps run sequentially, each with `stepTimeout`; a failed step
  stops the sync.
- `planSync` is called with `snapshot == null` to compute the pending changes on the ground. It must not have side
  effects.

### Sync algorithm (`OnboardSchedulerService.sync`)

1. **Collect**: `readOnboard`.
2. **Reconcile** the snapshot with the ground entries:
   - entry whose ref is on board: `UPLOAD_PENDING` / `UNKNOWN` / `NOT_ONBOARD` -> `ONBOARD`;
   - `ONBOARD` entry whose ref is missing: release time in the past -> `UNKNOWN` unless a report already moved it
     on; in the future -> `NOT_ONBOARD` (it will be re-inserted);
   - on-board items without an entry -> foreign items in the `SyncInfo`.
3. **Plan**: entries due before `cutover = now + minLeadTime` that are not on board become `MISSED`; then `planSync`.
4. **Execute** the steps.
5. **Verify**: read back again and reconcile.

`SyncMode.READ_BACK` stops after step 2. Pending changes are step 3 without a snapshot and without marking `MISSED`.

### Ref resolution and plausibility

Refs are reused (PUS sequence counts wrap at 16384, counters restart after a reboot). `reportStatus` considers the
entries that carry the ref in their upload history and keeps those for which `ScheduleEntry.isPlausibleReport` holds:

- release reports: report time in `[releaseTime - releaseTolerance, releaseTime + releaseWindow]`;
- execution reports: same window, or any later time while the entry is `RELEASED` or `STARTED`.

Among those, the one with the closest release time wins. Without this, a TM(1,x) about a new command that reused a
seq count was attributed to an old entry.

## PUS ST[11] adapter internals (`org.yamcs.pus.Pus11SchedulerAdapter`)

- Insert: `uploadEntry` with `pus11ScheduleAt`, `pus11SubScheduleId`, `pus11GroupId`. The ref is
  `apid/seqCount` of the embedded TC, read from the command history (`ccsds-seqcount`, APID from the binary). The
  TC(1,7) for the outer TC(11,4) (`pus11Apid` / `pus11CcsdsSeqCount`) confirms the insert.
- Delete: TC(11,5) with the request ids (source id, APID, seq count; 2+2+2 bytes), at most
  `maxDeletesPerCommand` per TC.
- Read back: TC(11,17) -> TM(11,13), parsed raw: N (`reportCountBytes`), then per activity the sub-schedule id and
  group id (`subScheduleIdBytes`, `groupIdBytes`, may be 0), the release time (CUC, `pfield`/`epoch`) and the request
  id. The source id found there is remembered for later deletes.
- Status: TM(1,x) read raw from the realtime and dump streams; the request id is the first 4 bytes of the verified
  TC. The reported time is the packet generation time, so dumped reports carry the on-board time.
- An entry option is not offered when its field is absent from TM(11,13) (width 0).

## Notes for other adapters

All known frameworks can read back, so syncs can always start from the on-board truth and are idempotent:

- **C2A**: entry-level edit and a schedule report, close to PUS.
- **cFS SC**: no entry-level edit of a running ATS; the whole ATS is replaced (load the inactive ATS table, switch).
  Read back by dumping the ATS table; status from the SC housekeeping / event messages.
- **F´**: sequences are files; the whole sequence is replaced by uploading a new file. Read back by downlinking the
  sequence file; status from the sequencer events.

For whole-schedule adapters `entryLevelEdit` is false and `planSync` produces one step that rewrites the schedule.
Where these adapters will live (yamcs core or plugins) is not decided.

## Open work

- Automatic sync at AOS (currently manual).
- Timeline band (read-only timeline source).
- State icons in the UI (under discussion).
- PUS: time shift of an on-board entry with TC(11,7) instead of delete + insert.
- Optional deletion of foreign items.
- LOS reconstruction test: simulator LOS + ST[15] store dumped on `tm_dump`.
