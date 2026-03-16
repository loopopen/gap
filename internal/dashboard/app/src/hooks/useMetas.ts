import { useState, useEffect } from 'react';
import axios from 'axios';
import type { Meta } from '../types';
import { message } from 'antd';

export const useMetas = () => {
  const [metas, setMetas] = useState<Meta[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get<Meta[]>(`/metas`)
      .then(resp => {
        setMetas(resp.data);
      }).catch(err => {
        message.error(err);
      }).finally(() => {
        setLoading(false);
      })
  }, []);

  return { metas, loading };
}