import { useCallback, useEffect, useState } from 'react';
import { listStrategies } from '@/api/strategy-catalog';
import type { StrategyCatalogItem } from '@/types/strategy';

export function useStrategyCatalog() {
  const [strategies, setStrategies] = useState<StrategyCatalogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await listStrategies();
      setStrategies(list);
    } catch (err) {
      setError(err instanceof Error ? err.message : '加载策略目录失败');
      setStrategies([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  return { strategies, loading, error, reload };
}
