import { useCallback, useEffect, useState } from 'react'
import axios from 'axios'

import type { DashboardStats } from '../types'

export const useDashboardStats = () => {
  const [stats, setStats] = useState<DashboardStats>()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()
  const [updatedAt, setUpdatedAt] = useState<Date>()

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
    void load()
  }, [load])

  useEffect(() => {
    void load()
    const timer = window.setInterval(load, 15_000)
    return () => window.clearInterval(timer)
  }, [load])

  return { stats, loading, error, updatedAt, refetch }
}
