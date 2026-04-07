import { useState, useEffect } from 'react';
import axios from 'axios';
import type { MsgStatus, PagedResult } from '../types';
import type { Msg } from '../types';
import { message } from 'antd';

export interface MsgQueryParams {
  status?: MsgStatus;
  topic?: string;
  page?: number;
  per_page?: number;
}

export type PubMsgQueryParams = MsgQueryParams;

export interface RecMsgQueryParams extends MsgQueryParams {
  group?: string;
}

export const usePubMsgs = (params: PubMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {};

  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!enabled) return
    axios.get<PagedResult<Msg>>(`/messages/published`, { params })
      .then(resp => {
        setMsgs(resp.data?.data || []);
        setTotal(resp.data?.pagination?.total || 0);
      }).catch(err => {
        message.error(err);
      }).finally(() => {
        setLoading(false);
      })
  }, [JSON.stringify(params), enabled]);

  return { msgs, total, loading };
}

export const useRecMsgs = (params: RecMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {};

  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!enabled) return
    axios.get<PagedResult<Msg>>(`/messages/received`, { params })
      .then(resp => {
        setMsgs(resp.data?.data || []);
        setTotal(resp.data?.pagination?.total || 0);
      }).catch(err => {
        message.error(err.message);
      }).finally(() => {
        setLoading(false);
      })
  }, [JSON.stringify(params), enabled]);

  return { msgs, total, loading };
}