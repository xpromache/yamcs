import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  Inject,
  ViewChild,
} from '@angular/core';
import {
  UntypedFormBuilder,
  UntypedFormControl,
  UntypedFormGroup,
  Validators,
} from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import {
  Command,
  EntryOptionInfo,
  OnboardScheduler,
  ScheduleEntry,
  utils,
  WebappSdkModule,
  YamcsService,
} from '@yamcs/webapp-sdk';
import { BehaviorSubject } from 'rxjs';
import { CommandSelectorComponent } from '../../../shared/command-selector/command-selector.component';
import { CommandFormComponent } from '../../command-sender/command-form/command-form.component';
import { ScheduleEntryTemplateProvider } from './ScheduleEntryTemplateProvider';

export interface EditScheduleEntryDialogData {
  scheduler: OnboardScheduler;
  /** Entry to edit. Absent when adding a new entry. */
  entry?: ScheduleEntry;
  /** Command of the edited entry, already resolved. */
  command?: Command;
  /** The entry is on board: the result replaces it by a new entry instead of changing it */
  replace?: boolean;
  /** Initial release time for a new entry */
  releaseTime?: Date;
}

export interface ScheduleEntryResult {
  command: string;
  args: { [key: string]: any };
  releaseTime: string;
  options: { [key: string]: any };
  description?: string;
}

/**
 * Add or edit an on-board schedule entry: command selection and arguments use
 * the same form as when sending a command; the release time, description and
 * scheduler-specific options form an extra step.
 */
@Component({
  templateUrl: './edit-schedule-entry-dialog.component.html',
  styleUrl: './edit-schedule-entry-dialog.component.css',
  changeDetection: ChangeDetectionStrategy.Eager,
  imports: [CommandFormComponent, CommandSelectorComponent, WebappSdkModule],
})
export class EditScheduleEntryDialogComponent {
  @ViewChild('commandSelector')
  commandSelector: CommandSelectorComponent;

  @ViewChild('commandForm')
  commandForm: CommandFormComponent;

  selectCommandForm: UntypedFormGroup;
  scheduleForm: UntypedFormGroup;

  selectedCommand$ = new BehaviorSubject<Command | null>(null);
  templateProvider: ScheduleEntryTemplateProvider | null = null;
  entryOptions: EntryOptionInfo[];

  constructor(
    private dialogRef: MatDialogRef<EditScheduleEntryDialogComponent>,
    readonly yamcs: YamcsService,
    formBuilder: UntypedFormBuilder,
    private changeDetection: ChangeDetectorRef,
    @Inject(MAT_DIALOG_DATA) readonly data: EditScheduleEntryDialogData,
  ) {
    this.entryOptions = data.scheduler.entryOptions || [];
    const entry = data.entry;
    if (entry) {
      this.templateProvider = new ScheduleEntryTemplateProvider(entry);
      this.selectedCommand$.next(data.command ?? null);
    }

    this.selectCommandForm = formBuilder.group({
      command: ['', Validators.required],
    });
    this.selectCommandForm.valueChanges.subscribe(() => {
      const command = this.selectCommandForm.value['command'];
      this.selectedCommand$.next(command || null);
    });

    const releaseTime = entry ? entry.releaseTime : utils.toISOString(data.releaseTime ?? new Date());
    this.scheduleForm = formBuilder.group({
      releaseTime: new UntypedFormControl(releaseTime, [Validators.required]),
      description: new UntypedFormControl(entry?.description ?? ''),
    });
    for (const opt of this.entryOptions) {
      const value = entry?.options?.[opt.name];
      this.scheduleForm.addControl(
        'opt_' + opt.name,
        new UntypedFormControl(value === undefined || value === null ? '' : String(value)),
      );
    }
  }

  isNumeric(opt: EntryOptionInfo) {
    return opt.type === 'INTEGER' || opt.type === 'FLOAT';
  }

  handleOK() {
    const commandConfig = this.commandForm.getResult();
    const options: { [key: string]: any } = {};
    for (const opt of this.entryOptions) {
      const raw = this.scheduleForm.value['opt_' + opt.name];
      if (raw === '' || raw === null || raw === undefined) {
        continue;
      }
      if (this.isNumeric(opt)) {
        options[opt.name] = Number(raw);
      } else if (opt.type === 'BOOLEAN') {
        options[opt.name] = raw === 'true';
      } else {
        options[opt.name] = raw;
      }
    }
    const result: ScheduleEntryResult = {
      command: this.selectedCommand$.value!.qualifiedName,
      args: commandConfig.args,
      releaseTime: utils.toISOString(this.scheduleForm.value.releaseTime),
      options,
      description: this.scheduleForm.value.description || undefined,
    };
    this.dialogRef.close(result);
  }

  returnToList(system: string) {
    this.templateProvider = null;
    this.selectedCommand$.next(null);
    this.changeDetection.detectChanges(); // Ensure #commandSelector is set
    this.selectCommandForm.reset();
    this.commandSelector.changeSystem(system === '/' ? '' : system);
  }
}
