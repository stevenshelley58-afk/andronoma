import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { useAuth } from "./useAuth";
import { AssetRecord, getRun, listRunAssets, listRuns, Run } from "../lib/api";

type UseLatestRunOptions = {
  refreshInterval?: number;
};

type UseLatestRunState = {
  run: Run | null;
  assets: AssetRecord[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
};

function selectLatestRun(runs: Run[]): Run | null {
  if (!runs.length) {
    return null;
  }
  return runs
    .slice()
    .sort((a, b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime())[0];
}

export function useLatestRun(options: UseLatestRunOptions = {}): UseLatestRunState {
  const { token } = useAuth();
  const [run, setRun] = useState<Run | null>(null);
  const [assets, setAssets] = useState<AssetRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const refreshInterval = options.refreshInterval ?? 0;
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const fetchLatest = useCallback(async () => {
    if (!token) {
      if (!mountedRef.current) return;
      setRun(null);
      setAssets([]);
      setError(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const runList = await listRuns(token);
      const latest = selectLatestRun(runList.runs);
      if (!latest) {
        if (!mountedRef.current) return;
        setRun(null);
        setAssets([]);
        setLoading(false);
        return;
      }

      const [runDetail, assetResponse] = await Promise.all([
        getRun(token, latest.id),
        listRunAssets(token, latest.id),
      ]);

      if (!mountedRef.current) return;
      setRun(runDetail);
      setAssets(assetResponse.assets);
    } catch (err) {
      if (!mountedRef.current) return;
      setError((err as Error).message);
      setRun(null);
      setAssets([]);
    } finally {
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [token]);

  useEffect(() => {
    fetchLatest();
    if (refreshInterval > 0 && typeof window !== "undefined") {
      const id = window.setInterval(fetchLatest, refreshInterval);
      return () => window.clearInterval(id);
    }
    return undefined;
  }, [fetchLatest, refreshInterval]);

  const state = useMemo<UseLatestRunState>(
    () => ({ run, assets, loading, error, refresh: fetchLatest }),
    [run, assets, loading, error, fetchLatest],
  );

  return state;
}
