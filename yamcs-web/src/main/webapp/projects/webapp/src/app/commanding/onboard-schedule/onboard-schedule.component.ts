import { ChangeDetectionStrategy, Component, OnDestroy } from '@angular/core';
import { UntypedFormControl, UntypedFormGroup } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { Title } from '@angular/platform-browser';
import {
  EntryOptionInfo,
  MessageService,
  OnboardScheduler,
  PendingChangesInfo,
  ScheduleEntriesSubscription,
  ScheduleEntry,
  SyncInfo,
  utils,
  WebappSdkModule,
  YamcsService,
} from '@yamcs/webapp-sdk';
import { BehaviorSubject, Subject, Subscription, debounceTime } from 'rxjs';
import {
  EditScheduleEntryDialogComponent,
  EditScheduleEntryDialogData,
  ScheduleEntryResult,
} from './edit-schedule-entry-dialog/edit-schedule-entry-dialog.component';

/**
 * Ground model of an on-board time-tagged schedule: planned entries, their
 * on-board state and the synchronisation with the spacecraft.
 */
@Component({
  templateUrl: './onboard-schedule.component.html',
  styleUrl: './onboard-schedule.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [WebappSdkModule],
})
export class OnboardScheduleComponent implements OnDestroy {
  schedulers$ = new BehaviorSubject<OnboardScheduler[]>([]);
  scheduler$ = new BehaviorSubject<OnboardScheduler | null>(null);
  entries$ = new BehaviorSubject<ScheduleEntry[]>([]);
  syncResult$ = new BehaviorSubject<SyncInfo | null>(null);
  syncing$ = new BehaviorSubject<string | null>(null);
  expanded$ = new BehaviorSubject<ScheduleEntry | null>(null);
  /** Distinct effective values of each filterable option, among the loaded entries */
  filterValues$ = new BehaviorSubject<{ option: EntryOptionInfo; values: string[] }[]>([]);
  /** What the next sync would do, computed on the ground and kept up to date */
  pendingChanges$ = new BehaviorSubject<PendingChangesInfo | null>(null);

  filterForm = new UntypedFormGroup({});

  private entriesById = new Map<string, ScheduleEntry>();
  /** Descriptions of the pending steps affecting each entry */
  private pendingByEntry = new Map<string, string[]>();
  private subscription?: ScheduleEntriesSubscription;
  private pendingRefresh$ = new Subject<void>();
  private pendingRefreshSubscription: Subscription;
  private showDeleted = false;

  constructor(
    readonly yamcs: YamcsService,
    private messageService: MessageService,
    private dialog: MatDialog,
    title: Title,
  ) {
    title.setTitle('On-board schedule');

    // entry changes often come in bursts (a sync updates many entries)
    this.pendingRefreshSubscription = this.pendingRefresh$
      .pipe(debounceTime(300))
      .subscribe(() => this.loadPendingChanges());

    yamcs.yamcsClient
      .getOnboardSchedulers(yamcs.instance!)
      .then((schedulers) => {
        this.schedulers$.next(schedulers);
        if (schedulers.length) {
          this.selectScheduler(schedulers[0]);
        }
      })
      .catch((err) => messageService.showError(err));
  }

  selectScheduler(scheduler: OnboardScheduler) {
    this.subscription?.cancel();
    this.scheduler$.next(scheduler);
    this.filterForm = new UntypedFormGroup({});
    for (const opt of this.filterableOptions(scheduler)) {
      this.filterForm.addControl(opt.name, new UntypedFormControl(''));
    }
    this.filterForm.addControl('deleted', new UntypedFormControl(''));
    this.showDeleted = false;
    this.filterForm.valueChanges.subscribe((value) => {
      const showDeleted = value['deleted'] === 'show';
      if (showDeleted !== this.showDeleted) {
        // deleted entries are only loaded on request
        this.showDeleted = showDeleted;
        this.loadEntries();
      } else {
        this.emitEntries();
      }
    });
    this.syncResult$.next(scheduler.lastSync ?? null);
    this.loadEntries();
    this.subscription = this.yamcs.yamcsClient.createScheduleEntriesSubscription(
      { instance: this.yamcs.instance!, scheduler: scheduler.name },
      (entry) => {
        // the subscription only carries the last history record: keep details if expanded
        this.entriesById.set(entry.id, entry);
        this.emitEntries();
        this.pendingRefresh$.next();
        if (this.expanded$.value?.id === entry.id) {
          this.loadDetails(entry);
        }
      },
    );
  }

  private loadPendingChanges() {
    const scheduler = this.scheduler$.value;
    if (!scheduler) {
      return;
    }
    this.yamcs.yamcsClient
      .getPendingScheduleChanges(this.yamcs.instance!, scheduler.name)
      .then((pending) => {
        this.pendingByEntry.clear();
        for (const step of pending.steps || []) {
          for (const id of step.entryIds || []) {
            const descriptions = this.pendingByEntry.get(id) || [];
            descriptions.push(step.description);
            this.pendingByEntry.set(id, descriptions);
          }
        }
        this.pendingChanges$.next(pending);
        this.emitEntries(); // refresh the row markers
      })
      .catch((err) => this.messageService.showError(err));
  }

  /** Pending step(s) of the next sync affecting the entry, or null */
  pendingFor(entry: ScheduleEntry) {
    const descriptions = this.pendingByEntry.get(entry.id);
    return descriptions ? descriptions.join('\n') : null;
  }

  loadEntries() {
    const scheduler = this.scheduler$.value;
    if (!scheduler) {
      return;
    }
    this.yamcs.yamcsClient
      .getScheduleEntries(this.yamcs.instance!, scheduler.name, {
        includeDeleted: this.showDeleted,
      })
      .then((entries) => {
        this.entriesById.clear();
        for (const entry of entries) {
          this.entriesById.set(entry.id, entry);
        }
        this.emitEntries();
        this.pendingRefresh$.next();
      })
      .catch((err) => this.messageService.showError(err));
  }

  private emitEntries() {
    const visible = [...this.entriesById.values()].filter(
      (e) => this.showDeleted || e.intent !== 'DELETED',
    );
    const options = this.filterableOptions(this.scheduler$.value);

    // the filter choices come from all visible entries, before filtering
    this.filterValues$.next(
      options.map((option) => {
        const values = new Set(visible.map((e) => this.optionKey(e, option)));
        return { option, values: [...values].sort(compareKeys) };
      }),
    );

    const filter = this.filterForm.value;
    const entries = visible
      .filter((e) =>
        options.every((opt) => !filter[opt.name] || filter[opt.name] === this.optionKey(e, opt)),
      )
      .sort((a, b) => a.releaseTime.localeCompare(b.releaseTime));
    this.entries$.next(entries);
  }

  filterableOptions(scheduler: OnboardScheduler | null) {
    return (scheduler?.entryOptions || []).filter((opt) => opt.filterable);
  }

  /**
   * Effective value of an option: the one of the entry, or else the default of the scheduler.
   */
  private optionValue(entry: ScheduleEntry, option: EntryOptionInfo) {
    const value = entry.options?.[option.name];
    return value === undefined || value === null ? option.defaultValue : value;
  }

  private optionKey(entry: ScheduleEntry, option: EntryOptionInfo) {
    const value = this.optionValue(entry, option);
    return value === undefined || value === null ? UNSET : String(value);
  }

  optionLabel(key: string) {
    return key === UNSET ? '(not set)' : key;
  }

  /**
   * Options of an entry with their effective values, defaults included.
   */
  effectiveOptions(entry: ScheduleEntry) {
    const scheduler = this.scheduler$.value;
    const result: { name: string; value: string; isDefault: boolean }[] = [];
    const declared = new Set<string>();
    for (const opt of scheduler?.entryOptions || []) {
      declared.add(opt.name);
      const own = entry.options?.[opt.name];
      if (own !== undefined && own !== null) {
        result.push({ name: opt.name, value: this.formatValue(own), isDefault: false });
      } else if (opt.defaultValue !== undefined && opt.defaultValue !== null) {
        result.push({ name: opt.name, value: this.formatValue(opt.defaultValue), isDefault: true });
      }
    }
    for (const [name, value] of Object.entries(entry.options || {})) {
      if (!declared.has(name)) {
        result.push({ name, value: this.formatValue(value), isDefault: false });
      }
    }
    return result;
  }


  private openDialog(data: EditScheduleEntryDialogData) {
    return this.dialog
      .open<EditScheduleEntryDialogComponent, EditScheduleEntryDialogData, ScheduleEntryResult>(
        EditScheduleEntryDialogComponent,
        {
          width: '70%',
          height: '100%',
          autoFocus: false,
          position: { right: '0' },
          panelClass: 'dialog-full-size',
          data,
        },
      )
      .afterClosed();
  }

  addEntry() {
    const scheduler = this.scheduler$.value!;
    const releaseTime = new Date(this.yamcs.getMissionTime().getTime() + 5 * 60 * 1000);
    this.openDialog({ scheduler, releaseTime }).subscribe((result) => {
      if (!result) {
        return;
      }
      this.yamcs.yamcsClient
        .createScheduleEntry(this.yamcs.instance!, scheduler.name, result)
        .then((entry) => {
          this.entriesById.set(entry.id, entry);
          this.emitEntries();
        })
        .catch((err) => this.messageService.showError(err));
    });
  }

  /**
   * Changes an entry that is not on board, or replaces one that is (replace = true).
   */
  async editEntry(entry: ScheduleEntry, replace = false) {
    const scheduler = this.scheduler$.value!;
    let command;
    try {
      command = await this.yamcs.yamcsClient.getCommand(this.yamcs.instance!, entry.command);
    } catch (err: any) {
      this.messageService.showError(err);
      return;
    }
    this.openDialog({ scheduler, entry, command, replace }).subscribe((result) => {
      if (!result) {
        return;
      }
      const instance = this.yamcs.instance!;
      const call = replace
        ? this.yamcs.yamcsClient
            .replaceScheduleEntry(instance, scheduler.name, entry.id, result)
            .then((created) => {
              this.entriesById.set(created.id, created);
              // the old entry is now pending deletion (or deleted)
              return this.yamcs.yamcsClient.getScheduleEntry(instance, scheduler.name, entry.id);
            })
        : this.yamcs.yamcsClient.updateScheduleEntry(instance, scheduler.name, entry.id, {
            ...result,
            description: result.description ?? '',
          });
      call
        .then((updated) => {
          this.entriesById.set(updated.id, updated);
          this.emitEntries();
        })
        .catch((err) => this.messageService.showError(err));
    });
  }

  deleteEntry(entry: ScheduleEntry) {
    const onboard = entry.state === 'ONBOARD' || entry.state === 'UPLOAD_PENDING';
    const msg = onboard
      ? 'This entry is on board. It will be removed from the on-board schedule at the next sync. Continue?'
      : 'Delete this entry?';
    if (!confirm(msg)) {
      return;
    }
    const scheduler = this.scheduler$.value!;
    this.yamcs.yamcsClient
      .deleteScheduleEntry(this.yamcs.instance!, scheduler.name, entry.id)
      .then((updated) => {
        this.entriesById.set(updated.id, updated);
        this.emitEntries();
      })
      .catch((err) => this.messageService.showError(err));
  }

  toggleDetails(entry: ScheduleEntry) {
    if (this.expanded$.value?.id === entry.id) {
      this.expanded$.next(null);
    } else {
      this.loadDetails(entry);
    }
  }

  private loadDetails(entry: ScheduleEntry) {
    const scheduler = this.scheduler$.value!;
    this.yamcs.yamcsClient
      .getScheduleEntry(this.yamcs.instance!, scheduler.name, entry.id)
      .then((details) => this.expanded$.next(details))
      .catch((err) => this.messageService.showError(err));
  }

  /** Interaction with the spacecraft: readBack (read and reconcile) or sync */
  sync(mode: 'readBack' | 'sync') {
    const scheduler = this.scheduler$.value!;
    this.syncing$.next(mode);
    this.yamcs.yamcsClient
      .syncOnboardSchedule(this.yamcs.instance!, scheduler.name, mode)
      .then((result) => {
        this.syncResult$.next(result);
        this.loadEntries();
      })
      .catch((err) => this.messageService.showError(err))
      .finally(() => this.syncing$.next(null));
  }

  /** Entries can be changed only while they are not on board */
  canEdit(entry: ScheduleEntry) {
    return entry.intent === 'PLANNED' && ['NOT_ONBOARD', 'MISSED'].includes(entry.state);
  }

  /** Entries on board are replaced by a new entry instead of being changed */
  canReplace(entry: ScheduleEntry) {
    return entry.intent === 'PLANNED' && ['UPLOAD_PENDING', 'ONBOARD'].includes(entry.state);
  }

  /** Any entry not yet released on board can be deleted */
  canDelete(entry: ScheduleEntry) {
    return (
      entry.intent === 'PLANNED' &&
      ['NOT_ONBOARD', 'UPLOAD_PENDING', 'ONBOARD', 'MISSED', 'UNKNOWN'].includes(entry.state)
    );
  }

  hasValues(values?: { [key: string]: any } | null) {
    return !!values && Object.keys(values).length > 0;
  }

  entries(values?: { [key: string]: any } | null) {
    return Object.entries(values || {});
  }

  formatValue(value: any): string {
    if (value === null || value === undefined) {
      return '';
    }
    return typeof value === 'object' ? JSON.stringify(value) : String(value);
  }

  /**
   * Formats assignments as "name: value, ...", limited to the first max ones.
   */
  formatValues(values?: { [key: string]: any } | null, max?: number) {
    const all = Object.entries(values || {});
    const shown = max === undefined ? all : all.slice(0, max);
    let result = shown.map(([k, v]) => `${k}: ${this.formatValue(v)}`).join(', ');
    if (all.length > shown.length) {
      result += `, … (+${all.length - shown.length})`;
    }
    return result;
  }

  lastRecord(entry: ScheduleEntry) {
    const history = entry.history || [];
    return history.length ? history[history.length - 1] : null;
  }

  stateClass(entry: ScheduleEntry) {
    switch (entry.state) {
      case 'COMPLETED':
        return 'ok';
      case 'FAILED':
      case 'MISSED':
        return 'error';
      case 'UNKNOWN':
      case 'SKIPPED':
        return 'warning';
      case 'ONBOARD':
      case 'RELEASED':
      case 'STARTED':
        return 'progress';
      default:
        return 'neutral';
    }
  }

  ngOnDestroy() {
    this.subscription?.cancel();
    this.pendingRefreshSubscription.unsubscribe();
  }
}

const UNSET = '__unset__';

/** Numeric order for numeric option values, text order otherwise; unset last */
function compareKeys(a: string, b: string) {
  if (a === UNSET || b === UNSET) {
    return a === b ? 0 : a === UNSET ? 1 : -1;
  }
  const na = Number(a);
  const nb = Number(b);
  if (!isNaN(na) && !isNaN(nb)) {
    return na - nb;
  }
  return a.localeCompare(b);
}
