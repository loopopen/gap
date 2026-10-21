import { useCallback, useEffect, useState } from 'react'
import axios from 'axios'

import type { DashboardStats } from '../types'

export const DASHBOARD_REFRESH_INTERVALS_MS = [1_000, 5_000, 15_000]
export const DASHBOARD_REFRESH_INTERVAL_MS = 5_000

export const useDashboardStats = () => {
  const [stats, setStats] = useState<DashboardStats>()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()
  const [updatedAt, setUpdatedAt] = useState<Date>()
  const [cycleStartedAt, setCycleStartedAt] = useState(0)
  const [cycle, setCycle] = useState(0)
  const [refreshIntervalMs, setRefreshIntervalMs] = useState(DASHBOARD_REFRESH_INTERVAL_MS)

  const load = useCallback(() => {
    return axios
      .get<DashboardStats>('/stats')
      .then((response) => {
        setStats(response.data)
        setUpdatedAt(new Date())
        setError(undefined)
      })
      .catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : 'failed to load dashboard statistics')
      })
      .finally(() => setLoading(false))
  }, [])

  const refetch = useCallback(() => {
    setLoading(true)
    setCycle((current) => current + 1)
  }, [])

  const cycleRefreshInterval = useCallback(() => {
    setRefreshIntervalMs((current) => {
      const index = DASHBOARD_REFRESH_INTERVALS_MS.indexOf(current)
      return DASHBOARD_REFRESH_INTERVALS_MS[(index + 1) % DASHBOARD_REFRESH_INTERVALS_MS.length]
    })
  }, [])

  useEffect(() => {
    let timer = 0
    const arm = () => {
      setCycleStartedAt(Date.now())
      void load()
      timer = window.setTimeout(arm, refreshIntervalMs)
    }
    arm()
    return () => window.clearTimeout(timer)
  }, [load, cycle, refreshIntervalMs])

  return { stats, loading, error, updatedAt, refetch, cycleStartedAt, refreshIntervalMs, cycleRefreshInterval }
}
