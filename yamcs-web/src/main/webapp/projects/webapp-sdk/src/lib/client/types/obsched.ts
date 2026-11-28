import { WebSocketCall } from '../WebSocketCall';

export type EntryIntent = 'PLANNED' | 'DELETE_PENDING' | 'DELETED';

export type EntryState =
  | 'NOT_ONBOARD'
  | 'UPLOAD_PENDING'
  | 'ONBOARD'
  | 'RELEASED'
  | 'STARTED'
  | 'COMPLETED'
  | 'FAILED'
  | 'MISSED'
  | 'SKIPPED'
  | 'UNKNOWN';

export type RecordSource = 'GROUND' | 'REALTIME' | 'DUMP' | 'READBACK';

export interface HistoryRecord {
  time: string;
  receptionTime: string;
  source: RecordSource;
  state?: EntryState;
  message?: string;
}

export interface EntryUpload {
  commandId: string;
  ref?: string;
  time: string;
  revision: number;
}

export interface ScheduleEntry {
  id: string;
  scheduler: string;
  command: string;
  args?: { [key: string]: any };
  releaseTime: string;
  options?: { [key: string]: any };
  description?: string;
  tags?: string[];
  revision: number;
  uploadedRevision: number;
  intent: EntryIntent;
  state: EntryState;
  ref?: string;
  stateMessage?: string;
  created?: string;
  createdBy?: string;
  modified?: string;
  modifiedBy?: string;
  uploads?: EntryUpload[];
  history?: HistoryRecord[];
}

export interface SchedulerCapabilities {
  readBack: boolean;
  entryLevelEdit: boolean;
  observableStates: EntryState[];
  minLeadTime: string;
}

export interface EntryOptionInfo {
  name: string;
  type: string;
  description?: string;
  /** Value applied on board when an entry does not set the option */
  defaultValue?: any;
  /** Whether the option partitions the schedule and is worth filtering on */
  filterable?: boolean;
}

export interface SyncStep {
  description: string;
  entryIds?: string[];
  done?: boolean;
  error?: string;
}

export interface ForeignItem {
  ref: string;
  releaseTime: string;
  description?: string;
}

export interface SyncInfo {
  id: string;
  status: 'SYNC_RUNNING' | 'SYNC_SUCCEEDED' | 'SYNC_FAILED';
  type: 'READ_BACK' | 'SYNC';
  start: string;
  stop?: string;
  user: string;
  steps?: SyncStep[];
  foreignItems?: ForeignItem[];
  warnings?: string[];
  failureReason?: string;
}

/** What the next sync would do, computed on the ground only */
export interface PendingChangesInfo {
  time: string;
  steps?: SyncStep[];
  warnings?: string[];
}

export interface OnboardScheduler {
  name: string;
  adapter: string;
  processor: string;
  capabilities: SchedulerCapabilities;
  lastSync?: SyncInfo;
  entryOptions?: EntryOptionInfo[];
  /** Short name of the on-board reference of the entries (e.g. "APID/Seq" for PUS) */
  refLabel?: string;
  /** Explanation of the on-board reference */
  refDescription?: string;
}

export interface ListOnboardSchedulersResponse {
  schedulers?: OnboardScheduler[];
}

export interface ListScheduleEntriesOptions {
  start?: string;
  stop?: string;
  includeDeleted?: boolean;
  details?: boolean;
}

export interface ListScheduleEntriesResponse {
  entries?: ScheduleEntry[];
}

export interface CreateScheduleEntryRequest {
  command: string;
  args?: { [key: string]: any };
  releaseTime: string;
  options?: { [key: string]: any };
  description?: string;
  tags?: string[];
}

export interface UpdateScheduleEntryRequest {
  command?: string;
  args?: { [key: string]: any };
  releaseTime?: string;
  options?: { [key: string]: any };
  description?: string;
  tags?: { tags: string[] };
}

export interface SubscribeScheduleEntriesRequest {
  instance: string;
  scheduler: string;
}

export type ScheduleEntriesSubscription = WebSocketCall<
  SubscribeScheduleEntriesRequest,
  ScheduleEntry
>;
