import { useState, useEffect } from 'react';
import axios from 'axios';
import type { MsgStatus, PagedResult } from '../types';
import type { Msg } from '../types';
import { message } from 'antd';

interface MsgQueryParams {
  status?: MsgStatus;
  topic?: string;
  page?: number;
  per_page?: number;
}

type PubMsgQueryParams = MsgQueryParams;

interface RecMsgQueryParams extends MsgQueryParams {
  group?: string;
}

export const usePubMsgs = (params: PubMsgQueryParams) => {
  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get<PagedResult<Msg>>(`/messages/published`, { params })
      .then(resp => {
        setMsgs(resp.data?.data || []);
        setTotal(resp.data?.pagination?.total || 0);
      }).catch(err => {
        message.error(err);
      }).finally(() => {
        setLoading(false);
      })
  }, [JSON.stringify(params)]);

  return { msgs, total, loading };
}

export const useRecMsgs = (params: RecMsgQueryParams) => {
  const [msgs, setMsgs] = useState<Msg[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get<PagedResult<Msg>>(`/messages/received`, { params })
      .then(resp => {
        setMsgs(resp.data?.data || []);
        setTotal(resp.data?.pagination?.total || 0);
      }).catch(err => {
        message.error(err);
      }).finally(() => {
        setLoading(false);
      })
  }, [params]);

  return { msgs, total, loading };
}