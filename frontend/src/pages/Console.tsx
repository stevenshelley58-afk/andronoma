import { useEffect, useState } from "react";

import StageBadge from "../components/StageBadge";
import { useAuth } from "../hooks/useAuth";
import { listRuns, startRun } from "../lib/api";

type Stage = {
  name: string;
  status: string;
};

type Run = {
  id: string;
  status: string;
  created_at: string;
  stages: Stage[];
};

export default function Console() {
  const { token } = useAuth();
  const [runs, setRuns] = useState<Run[]>([]);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    if (!token) return;
    try {
      const response = await listRuns(token);
      setRuns(response.runs);
    } catch (err) {
      setError((err as Error).message);
    }
  };

  useEffect(() => {
    if (!token) {
      setRuns([]);
      return;
    }
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [token]);

  const handleStart = async (id: string) => {
    if (!token) return;
    await startRun(token, id);
    refresh();
  };

  return (
    <div>
      <div className="card">
        <h2>Run Console</h2>
        <p>Monitor orchestration state and manually trigger restarts.</p>
        <button className="button" onClick={refresh} disabled={!token}>
          Refresh
        </button>
        {error && <p>{error}</p>}
      </div>

      <div className="card">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Status</th>
              <th>Created</th>
              <th>Stages</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.id}>
                <td>{run.id}</td>
                <td>{run.status}</td>
                <td>{new Date(run.created_at).toLocaleString()}</td>
                <td style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
                  {run.stages.map((stage) => (
                    <StageBadge key={stage.name} name={stage.name} status={stage.status} />
                  ))}
                </td>
                <td>
                  <button className="button secondary" onClick={() => handleStart(run.id)} disabled={!token}>
                    Restart
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
