# On-board scheduler model: design note

Status: draft (phase 0), 2026-11-27.

## Goal

Operators plan time-tagged commands in Yamcs at any time, including during LOS. At AOS, Yamcs:

1. collects evidence of what happened on board while out of contact (stored TM dumps, read-back of the on-board
   schedule) and updates the ground status of the commands that were due;
2. synchronises the on-board schedule with the ground plan: entries added on the ground are inserted on board,
   entries deleted on the ground are deleted on board, edited entries are replaced.

The model is generic. Framework-specific behaviour lives behind an adapter SPI. Phase 1 ships the PUS ST[11]
adapter; cFS SC, F´ and C2A adapters follow later (where they live is not decided yet). The research behind the
framework differences is summarised in the plan; the key point is that some frameworks can edit single entries and
read the schedule back (PUS 11, C2A), others can only replace the whole schedule (cFS SC, F´), and they differ in
what execution evidence they provide.

## Decisions

- **Own store, exposed to the timeline.** Entries are kept in their own table, not as timeline items: an entry has
  two independent state axes, upload history and evidence, which do not fit `TimelineItem`. The service registers a
  read-only timeline source so that entries can be shown in timeline bands.
- **Source of truth.** The entry stores the MDB command name plus its argument values. The command is encoded when
  it is uploaded; the binary that was actually sent is frozen in the command history of that upload.
- **One service per on-board scheduler.** `OnboardSchedulerService` is an instance service; the service name is the
  scheduler name (a spacecraft may have several, e.g. one per on-board computer).
- **Ground model is the master.** Sync never deletes on-board items that Yamcs does not know about ("foreign"
  items); it reports them. An option may later allow deleting them.

## Entry model

| Field | Meaning |
|---|---|
| `id` | UUID, stable for the life of the entry |
| `command`, `args` | qualified MDB command name, argument values |
| `releaseTime` | absolute release time (Yamcs time) |
| `options` | adapter-specific options, validated against the adapter's entry spec (PUS: `subScheduleId`, `groupId`) |
| `description`, `tags` | free text |
| `revision` | incremented on each edit; `uploadedRevision` is the revision that is on board |
| `intent` | `PLANNED`, `DELETE_PENDING`, `DELETED` |
| `state` | on-board state, see below |
| `ref` | adapter reference of the current on-board copy (PUS: `sourceId/apid/seqCount` of the embedded TC) |
| `uploads` | history of uploads: command history id, ref, time |
| `evidence` | log of state changes: time, source (`ground`, `realtime`, `dump`, `readback`), state, message |

On-board states, ordered:

```
NOT_ONBOARD < UPLOAD_PENDING < ONBOARD < RELEASED < STARTED < COMPLETED | FAILED
```

plus the side states `MISSED` (never got on board before its release time), `SKIPPED` (on board, not released, e.g.
disabled sub-schedule; only if the adapter can observe it) and `UNKNOWN` (should have been released, no evidence).

Evidence only moves an entry forward in this order, so TM arriving late or out of order (dumps) cannot regress it;
it is still recorded in the evidence log. `UNKNOWN` can be upgraded by later evidence. Reconciliation from a
read-back is the only thing that can move an entry back (e.g. `ONBOARD -> NOT_ONBOARD` when it is missing from the
on-board schedule before its release time, so that the next sync re-inserts it).

Editing is allowed while the entry has not been released. Deleting a released entry is rejected. Deleting an entry
that is not on board removes it immediately (`DELETED`); otherwise it becomes `DELETE_PENDING` until a sync
confirms the on-board deletion.

## Adapter SPI

```java
public interface SchedulerAdapter {
    void init(SchedulerContext ctx, YConfiguration config);
    SchedulerCapabilities getCapabilities();      // readBack, entryLevelEdit, observableStates, timeResolution, minLeadTime
    Spec getEntryOptionsSpec();                   // adapter-specific entry options, with their defaults
    Set<String> getFilterableOptions();           // options that partition the schedule (UI filters)
    CompletableFuture<OnboardSnapshot> readOnboard();          // only if capabilities.readBack
    SyncPlan planSync(List<ScheduleEntry> entries, OnboardSnapshot snapshot, long cutover);
    void start(); void stop();
}
```

- `SyncPlan` is a list of `SyncStep`s, each with a human-readable description (shown in the dry run) and an
  `execute(SyncExecutionContext)` returning a future. Steps run sequentially; a failed step stops the sync.
- `SchedulerContext` is what the engine offers to the adapter: the processor, sending commands as the user who
  started the sync, subscribing to TM streams, and reporting evidence
  (`reportEvidence(ref, time, state, source, message)`), which the engine resolves to an entry.
- Ref resolution: a ref may be reused over time (PUS sequence counts wrap at 16384), so the engine picks, among the
  entries that carry the ref in their upload history, the one whose release time is closest to the evidence time.

### Sync algorithm (engine)

1. **Collect**: if the adapter can read back, read the on-board schedule.
2. **Reconcile** the snapshot with the ground entries:
   - entry with its ref on board: `UPLOAD_PENDING`/`UNKNOWN`/`NOT_ONBOARD` -> `ONBOARD`;
   - entry `ONBOARD` whose ref is missing: release time in the past -> `UNKNOWN` unless evidence already moved it
     on; in the future -> `NOT_ONBOARD` (it will be re-inserted);
   - on-board items with no matching entry are reported as foreign.
3. **Diff and plan**: the adapter turns the reconciled entries into steps. Entries due before `cutover = now +
   minLeadTime` that are not on board become `MISSED`.
4. **Execute** the steps (or return them as a dry run).
5. **Verify** with a second read-back when available, and reconcile again.

For adapters with read-back this is naturally idempotent: an interrupted sync is fixed by running the next one.
Adapters without read-back (SC, F´) will need persisted step state; that is out of scope for phase 1.

### Command history

Each upload issues the entry's own command (for PUS with the `pus11ScheduleAt` option, so the post-processor wraps
it into a TC(11,4)), which gives each upload its own command history record. The command verifiers of the MDB are
disabled for these uploads (they would time out long before the release time). Instead the engine publishes
acknowledgements on that record as evidence arrives: `OnboardSchedule_Inserted`, `OnboardSchedule_Released`,
`OnboardSchedule_Started`, `OnboardSchedule_Completed`. The record also carries the attribute
`onboardScheduleEntryId`.

## PUS ST[11] adapter

- **Insert**: issue the entry's command with `pus11ScheduleAt`, `pus11SubScheduleId`, `pus11GroupId`. The ref is
  `sourceId/apid/seqCount` of the embedded TC, read from the command history (`ccsds-seqcount`, APID from the
  binary). The TC(1,7) for the outer TC(11,4) (APID/seq `pus11Apid`/`pus11CcsdsSeqCount`) confirms the insert.
- **Delete**: TC(11,5) with the request ids, batched.
- **Edit**: delete + insert (time-shift TC(11,7) could be used for a pure time change; later optimisation).
- **Read-back**: TC(11,17) summary report all -> TM(11,13), parsed raw: `N (2 bytes)`, then per activity sub-schedule
  id, group id (configured widths), release time (CUC, configured), request id (2+2+2 bytes).
- **Evidence**: PUS 1 reports are read raw from the configured TM streams (default `tm_realtime`, `tm_dump`). The
  request id in a TM(1,x) is the first 4 bytes of the verified TC: TM(1,1)/(1,2) -> `RELEASED`/`FAILED`,
  (1,3)/(1,4) -> `STARTED`/`FAILED`, (1,7)/(1,8) -> `COMPLETED`/`FAILED`. The evidence time is the packet
  generation time, so dumped reports carry the on-board time at which they were produced.
- Capabilities: read-back yes, entry-level edit yes, observable states all except `SKIPPED`.

## API

`/api/onboard-scheduler/{instance}/schedulers[/{scheduler}]` with entries CRUD, `:planSync` (dry run),
`:sync`, `:refresh` (read-back and reconcile only), and a WebSocket subscription to entry changes. Privileges:
`ReadOnboardSchedule`, `ControlOnboardSchedule`; uploads additionally require the usual commanding privileges of the
user who starts the sync.

## UI

An "On-board schedule" page per scheduler: entries table (release time, command, state, intent, ref, last
evidence), entry create/edit dialog using the command form, a sync panel showing the dry-run steps and foreign
items, and the evidence log per entry.

## Phases

0. This note.
1. Core model, storage, API, engine; PUS 11 adapter; minimal UI. Manual sync.
2. LOS reconstruction: simulator LOS + ST[15] store with dump on a separate stream; evidence from `tm_dump`.
3. Later: automatic sync at AOS, timeline band, SC / F´ / C2A adapters.
