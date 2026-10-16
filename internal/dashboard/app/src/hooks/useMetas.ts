import { useEffect, useState } from 'react'
import axios from 'axios'
import type { Meta } from '../types'

export const useMetas = () => {
  const [metas, setMetas] = useState<Meta[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()

  useEffect(() => {
    axios
      .get<Meta[]>(`/metas`)
      .then((resp) => {
        setMetas(resp.data)
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'failed to load metas')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [])

  return { metas, loading, error }
}
