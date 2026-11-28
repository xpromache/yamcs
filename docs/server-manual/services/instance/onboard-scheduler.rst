On-board Scheduler
==================

Keeps a ground model of an on-board time-tagged command schedule (such as the PUS ST[11] time-based schedule) and synchronises it with the spacecraft.

Operators plan commands at any time, including during LOS. When there is contact, Yamcs reads back the on-board schedule, works out what happened while out of contact, and inserts or deletes on-board entries so that the on-board schedule matches the ground plan. The status of each entry is then followed from telemetry, including telemetry recorded on board and dumped after the pass.

The service is generic. Everything that depends on the on-board software is delegated to an adapter. Yamcs ships an adapter for PUS ST[11] (see below).

A spacecraft may have several on-board schedulers (for example one per on-board computer). Each one is configured as a separate service; the service name is the scheduler name.


Entries
-------

An entry is a command of the Mission Database, with its argument values and an absolute release time. The command is encoded only when it is uploaded, so an entry always reflects the current MDB. An entry may also have adapter-specific options (for PUS: the sub-schedule and the group), a description and tags.

The ground keeps two things about each entry: what the ground wants (the intent) and what is known about the on-board copy (the state).

Intent:

PLANNED
    The entry has to be on board.

DELETE_PENDING
    The entry has been deleted on the ground. It is removed from the on-board schedule at the next sync.

DELETED
    The entry is deleted, on the ground and, if it was ever uploaded, on board. Deleted entries are hidden by default.

On-board state:

NOT_ONBOARD
    Not in the on-board schedule.

UPLOAD_PENDING
    The upload command was sent but not yet confirmed.

ONBOARD
    In the on-board schedule, waiting for its release time.

RELEASED
    Released from the on-board schedule.

STARTED
    Execution started on board.

COMPLETED
    Execution completed successfully on board.

FAILED
    Failed on board (release, start or completion failure).

MISSED
    Never got on board before its release time.

SKIPPED
    On board, but not released (for example because its sub-schedule was disabled). Only for adapters that can observe this.

UNKNOWN
    Should have been released, but nothing is known about what happened.

The states from ``NOT_ONBOARD`` to ``COMPLETED`` or ``FAILED`` are ordered. Telemetry only moves an entry forward in this order, so reports that arrive late or out of order (as is common with dumps) do not make an entry go back; they are still recorded in the history of the entry. Only a read back can move an entry back, for example from ``ONBOARD`` to ``NOT_ONBOARD`` when it has disappeared from the on-board schedule before its release time, so that the next sync inserts it again.

Each entry keeps its history: every state change with its time, its source (ground action, realtime telemetry, dumped telemetry or read back) and a message.


Editing, replacing and deleting
-------------------------------

An entry can be edited only while it is not on board (``NOT_ONBOARD`` or ``MISSED``).

An entry that is on board is not edited but replaced: a new entry is created and the old one is deleted. The next sync deletes the old activity on board and inserts the new one. In this way each entry corresponds to exactly one on-board activity.

An entry that is not on board is deleted immediately. An entry that is on board becomes ``DELETE_PENDING`` until a sync deletes it on board. An entry that has been released cannot be deleted. If an entry that is ``DELETE_PENDING`` is released on board before the deletion could be synced, the deletion is cancelled: the entry goes back to ``PLANNED`` and a warning is logged.


Pending changes, read back and sync
-----------------------------------

Pending changes
    The changes the next sync is expected to make on board: the entries to insert and to delete. They are computed on the ground only, from the ground entries and what is known from the latest read back, and are updated whenever an entry changes. Entries whose release time is too close to be uploaded (see the adapter's ``minLeadTime``) are reported here and will become ``MISSED``.

Read back
    Reads the on-board schedule and updates the state of the ground entries from it. Nothing is changed on board. Entries found on board become ``ONBOARD``. An on-board entry that is missing goes back to ``NOT_ONBOARD`` if its release time is in the future, or to ``UNKNOWN`` if it is in the past and no telemetry has reported its execution. Items found on board that Yamcs does not know about (foreign items) are reported but left untouched.

Sync
    Reads back the on-board schedule, marks as ``MISSED`` the entries that can no longer be uploaded in time, executes the pending changes and reads back again to verify them. A step that fails stops the sync. Because each sync starts from a read back, an interrupted sync is fixed by running the next one.

Read back and sync need contact with the spacecraft: they fail if the commanding link is not available. The pending changes need no contact.

The sync never deletes foreign items; the ground plan is the reference only for the entries that Yamcs knows about.

The commands of a sync are sent as the user who started the sync, so this user needs the usual commanding privileges in addition to ``ControlOnboardSchedule``.


Status from telemetry
---------------------

The adapter follows the telemetry streams and reports the execution of the on-board activities. Each report carries the on-board reference of the activity (for PUS: the APID and sequence count of the scheduled telecommand) and the on-board time it was generated at, so dumped reports are dated correctly.

On-board references are reused over time (PUS sequence counts wrap, counters restart after a reboot). A report is therefore attributed to an entry only if it is plausible for that entry:

* reports of the release are accepted if their time is within ``releaseTolerance`` before the release time and ``releaseWindow`` after it;
* reports of the execution (start, completion, failure) are accepted in the same window, or later while the entry is executing (``RELEASED`` or ``STARTED``).

Among the plausible entries, the one with the closest release time is chosen. Reports that are not plausible for any entry are ignored.


Command history
---------------

Each upload of an entry issues the command of the entry (for PUS wrapped in a TC(11,4) by the command post-processor), so each upload has its own command history record. The verifiers of the MDB are disabled for these commands, since they would time out long before the release time. Instead, the service adds the following acknowledgments to the record as the entry progresses on board:

* ``OnboardSchedule_Inserted``
* ``OnboardSchedule_Released``
* ``OnboardSchedule_Started``
* ``OnboardSchedule_Completed``
* ``OnboardSchedule_Deleted``

The record also has the attribute ``onboardScheduleEntryId`` with the identifier of the entry.


Web interface
-------------

Each scheduler has an On-board schedule page under Commanding. It shows the pending changes, the entries with their state and the result of the last read back or sync, including the foreign items. Entries can be filtered on the adapter options that partition the schedule (for PUS: sub-schedule and group). Entries are added and replaced with the same command form as in the command sender.


HTTP API
--------

The service is controlled through the :apidoc:`On-board Scheduler API <onboard-scheduler>`. Reading requires the system privilege ``ReadOnboardSchedule``; changing entries, reading back and syncing require ``ControlOnboardSchedule``.


Class Name
----------

:javadoc:`org.yamcs.obsched.OnboardSchedulerService`


Configuration
-------------

This service is defined in :file:`etc/yamcs.{instance}.yaml`. Example:

.. code-block:: yaml

    services:
      - class: org.yamcs.obsched.OnboardSchedulerService
        name: obsched
        args:
          adapter: pus11
          adapterArgs:
            realtimeStream: tm_realtime
            dumpStreams: [tm_dump]
            defaultSubScheduleId: 1
            defaultGroupId: 1


Configuration Options
---------------------

adapter (string)
    **Required.** The adapter: ``pus11`` or the class name of an implementation of :javadoc:`org.yamcs.obsched.SchedulerAdapter`.

adapterArgs (map)
    Configuration of the adapter. See below for the ``pus11`` adapter.

processor (string)
    Processor used for commanding and receiving telemetry. Default: ``realtime``.

stepTimeout (integer)
    Timeout in seconds for one step of a sync. Default: 60.

releaseTolerance (integer)
    Seconds of clock difference tolerated between the release time of an entry and the time of the telemetry reporting its release. Default: 5.

releaseWindow (integer)
    Seconds after the release time during which the release of an entry is expected. Reports about the reference of an entry outside this window are attributed to another command with the same reference and ignored, unless the entry is executing. Default: 60.


PUS ST[11] adapter
------------------

The ``pus11`` adapter works with the PUS time-based schedule:

* An entry is inserted by issuing its command with the ``pus11ScheduleAt`` option (and ``pus11SubScheduleId``, ``pus11GroupId`` from the entry options or the defaults). The command post-processor wraps it into a TC(11,4), see :doc:`../../links/command-postprocessor/pus`. The on-board reference is the APID and sequence count of the scheduled telecommand.
* Entries are deleted with TC(11,5), several per command.
* A replace is a delete followed by an insert.
* The schedule is read back with TC(11,17), the summary report of all activities, answered by TM(11,13).
* The status comes from the ST[1] verification reports of the scheduled telecommands, from the realtime and the dump streams: TM(1,1)/(1,2) give ``RELEASED``/``FAILED``, TM(1,3)/(1,4) ``STARTED``/``FAILED`` and TM(1,7)/(1,8) ``COMPLETED``/``FAILED``.
* ``SKIPPED`` cannot be observed.

Requirements:

* the ``pus11`` command post-processor options must be configured on the link (see :doc:`../../links/command-postprocessor/pus`);
* the ST[11] commands must be in the MDB (the PUS example uses ``mdb/pus11.xml``).

Adapter options:

realtimeStream (string)
    Stream with the realtime TM. Default: ``tm_realtime``.

dumpStreams (list of strings)
    Streams with TM recorded on board and dumped. Missing streams are ignored. Default: ``[tm_dump]``.

tmUserDataOffset (integer)
    Offset of the user data in the TM packets (primary and secondary header length). Default: 21.

deleteCommand (string)
    Command used to delete activities, TC(11,5). Default: ``/PUS11/DELETE_ACTIVITIES_BY_ID``.

summaryReportCommand (string)
    Command used to read back the schedule, TC(11,17). Default: ``/PUS11/GET_SUMMARY_REPORT``.

maxDeletesPerCommand (integer)
    Maximum number of activities deleted by one TC(11,5). Default: 32.

reportCountBytes (integer)
    Size in bytes of the number of activities in the TM(11,13). Default: 2.

subScheduleIdBytes (integer)
    Size in bytes of the sub-schedule identifier in the TM(11,13), 0 if absent. If 0, the sub-schedule is not offered as an entry option. Default: 1.

groupIdBytes (integer)
    Size in bytes of the group identifier in the TM(11,13), 0 if absent. If 0, the group is not offered as an entry option. Default: 1.

pfield (integer)
    CUC p-field of the release times in the TM(11,13), or -1 if the p-field is included in the time field. Default: -1.

epoch (string)
    Epoch of the release times in the TM(11,13). One of ``UNIX``, ``GPS``, ``TAI``, ``J2000``. Default: ``UNIX``.

defaultSubScheduleId (integer)
    Sub-schedule of the entries that do not set one. It should match the default of the command post-processor. If set, it is passed explicitly with each insert.

defaultGroupId (integer)
    Group of the entries that do not set one. It should match the default of the command post-processor. If set, it is passed explicitly with each insert.

minLeadTime (integer)
    Minimum number of seconds between the upload and the release time. Entries due earlier are not uploaded and become ``MISSED``. Default: 10.

ackTimeout (integer)
    Seconds to wait for the on-board completion of a schedule management TC. Default: 30.

sourceId (integer)
    Source identifier used in the request ids of TC(11,5) when it is not known from a read back. Default: 0.
