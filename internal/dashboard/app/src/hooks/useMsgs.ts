import { useEffect, useState } from 'react'
import axios from 'axios'
import type { Msg, MsgStatus, PagedResult } from '../types'

export interface MsgQueryParams {
  id?: string
  status?: MsgStatus
  topic?: string
  created_from?: string
  created_to?: string
  payload?: string
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
  const [refreshKey, setRefreshKey] = useState(0)
  const serializedParams = JSON.stringify(params)

  useEffect(() => {
    if (!enabled) return
    axios
      .get<PagedResult<Msg>>(url, { params: JSON.parse(serializedParams) as T })
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
  }, [url, serializedParams, enabled, refreshKey])

  const refetch = () => {
    setLoading(true)
    setRefreshKey((current) => current + 1)
  }

  return { msgs, total, loading, error, refetch }
}

export const usePubMsgs = (params: PubMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {}
  return useMsgs('/messages/published', params, enabled)
}

export const useRecMsgs = (params: RecMsgQueryParams, options?: { enabled: boolean }) => {
  const { enabled = true } = options || {}
  return useMsgs('/messages/received', params, enabled)
}
