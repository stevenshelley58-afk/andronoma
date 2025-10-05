import { useEffect, useState } from "react";

import { useAuth } from "../hooks/useAuth";
import { getPipeline, getSettings } from "../lib/api";

export default function Insights() {
  const { token } = useAuth();
  const [stages, setStages] = useState<string[]>([]);
  const [settings, setSettings] = useState<Record<string, unknown>>({});

  useEffect(() => {
    getPipeline(token ?? null).then((response) => setStages(response.stages));
    getSettings(token ?? null).then((response) => setSettings(response));
  }, [token]);

  return (
    <div className="card">
      <h2>Pipeline Insights</h2>
      <p>Understand how the orchestrator is configured.</p>
      <div className="stage-grid">
        {stages.map((stage) => (
          <div key={stage} className="stage-card">
            <h3>{stage}</h3>
            <p>Budget and telemetry details become visible once a run is active.</p>
          </div>
        ))}
      </div>
      <h3>Environment</h3>
      <pre>{JSON.stringify(settings, null, 2)}</pre>
    </div>
  );
}
