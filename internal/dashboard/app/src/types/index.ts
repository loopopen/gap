export interface Pagination {
  page: number;
  perPage: number;
  total: number;
  totalPage: number;
}

export interface PagedResult<T extends Object> {
  data: T[];
  pagination: Pagination;
}

export const MsgStatus = {
  Succeeded: 'Succeeded',
  Processing: 'Processing',
  Pending: 'Pending',
  Failed: 'Failed',
} as const;

export type MsgStatus = typeof MsgStatus[keyof typeof MsgStatus];

export const MsgStatusColors: Record<MsgStatus, string> = {
  [MsgStatus.Succeeded]: 'green',
  [MsgStatus.Processing]: 'blue',
  [MsgStatus.Pending]: 'orange',
  [MsgStatus.Failed]: 'red',
};


export interface Meta {
  type: string;
  plugin: string;
  version: string;
}

export interface Msg {
  id: string;
  createdAt: string;
  version: string;
  topic: string;
  status: MsgStatus;
  headers: string;
  payload: string;
  retries: number;
  group: string;
}

