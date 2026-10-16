import { useEffect, useState } from 'react'
import axios from 'axios'
import type { Msg, MsgStatus, PagedResult } from '../types'

export interface MsgQueryParams {
  status?: MsgStatus
  topic?: string
  page?: number
  per_page?: number
}

export type PubMsgQueryParams = MsgQueryParams

export interface RecMsgQueryParams extends MsgQueryParams {
  group?: string
}

const useMsgs = <T extends MsgQueryParams>(url: string, params: T, enabled: boolean) => {
  const [msgs, setMsgs] = useState<Msg[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()

  useEffect(() => {
    if (!enabled) return
    axios
      .get<PagedResult<Msg>>(url, { params })
      .then((resp) => {
        setMsgs(resp.data?.data || [])
        setTotal(resp.data?.pagination?.total || 0)
        setError(undefined)
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'failed to load messages')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [url, JSON.stringify(params), enabled])

  return { msgs, total, loading, error }
}

export const usePubMsgs = (params: PubMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {}
  return useMsgs('/messages/published', params, enabled)
}

export const useRecMsgs = (params: RecMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {}
  return useMsgs('/messages/received', params, enabled)
}
