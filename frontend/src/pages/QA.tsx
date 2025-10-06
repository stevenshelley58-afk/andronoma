import { useMemo } from "react";

import StageBadge from "../components/StageBadge";
import { useLatestRun } from "../hooks/useLatestRun";

type QACheck = {
  name: string;
  severity: string;
  message: string;
  remediation?: string;
  details?: unknown;
};

const severityLabels: Record<string, string> = {
  pass: "Pass",
  warning: "Warning",
  blocker: "Blocker",
};

function normalizeChecks(value: unknown): QACheck[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => {
      if (!item || typeof item !== "object") return null;
      const record = item as Record<string, unknown>;
      const name = typeof record["name"] === "string" ? record["name"] : null;
      if (!name) return null;
      const severity = typeof record["severity"] === "string" ? record["severity"] : "unknown";
      const message = typeof record["message"] === "string" ? record["message"] : "";
      const remediation = typeof record["remediation"] === "string" ? record["remediation"] : undefined;
      const details = record["details"];
      return { name, severity, message, remediation, details };
    })
    .filter((item): item is QACheck => Boolean(item));
}

function formatDetails(details: unknown): string | null {
  if (!details || typeof details !== "object") {
    return null;
  }
  try {
    return JSON.stringify(details, null, 2);
  } catch (error) {
    return null;
  }
}

export default function QA() {
  const { run, loading, error, refresh } = useLatestRun({ refreshInterval: 15000 });

  const stage = run?.stages.find((item) => item.name === "qa");
  const telemetry = useMemo(() => {
    if (stage?.telemetry && Object.keys(stage.telemetry).length > 0) {
      return stage.telemetry as Record<string, unknown>;
    }
    if (run?.telemetry && typeof run.telemetry === "object") {
      const qaTelemetry = (run.telemetry as Record<string, unknown>).qa;
      if (qaTelemetry && typeof qaTelemetry === "object") {
        return qaTelemetry as Record<string, unknown>;
      }
    }
    return {} as Record<string, unknown>;
  }, [stage?.telemetry, run?.telemetry]);

  const checks = useMemo(() => normalizeChecks(telemetry["checks"]), [telemetry]);
  const counts = telemetry["counts"] && typeof telemetry["counts"] === "object" ? (telemetry["counts"] as Record<string, unknown>) : {};
  const failureBreakdown = telemetry["failure_breakdown"] && typeof telemetry["failure_breakdown"] === "object" ? (telemetry["failure_breakdown"] as Record<string, unknown>) : {};
  const notes = typeof telemetry["notes"] === "string" ? (telemetry["notes"] as string) : stage?.notes;

  return (
    <div className="card">
      <h2>Quality Automation</h2>
      <p>Every run is gated by the QA checklist before export.</p>

      <div style={{ display: "flex", gap: "1rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="button secondary" onClick={refresh} disabled={loading}>
          Refresh
        </button>
        {stage && <StageBadge name={stage.name} status={stage.status} />}
        {loading && <span>Loading QA report…</span>}
        {error && <span style={{ color: "var(--danger)" }}>{error}</span>}
      </div>

      {!run && !loading && <p>No pipeline runs found. Launch a run from the wizard to populate this view.</p>}

      {run && (
        <>
          <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
            <div className="stage-card">
              <h3>Total Checks</h3>
              <p>{typeof counts["total"] === "number" ? (counts["total"] as number) : checks.length}</p>
            </div>
            <div className="stage-card">
              <h3>Warnings</h3>
              <p>{typeof counts["warnings"] === "number" ? (counts["warnings"] as number) : "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Blockers</h3>
              <p>{typeof counts["blockers"] === "number" ? (counts["blockers"] as number) : "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Last Updated</h3>
              <p>{stage?.finished_at ? new Date(stage.finished_at).toLocaleString() : new Date(run.updated_at).toLocaleString()}</p>
            </div>
          </div>

          {Object.keys(failureBreakdown).length > 0 && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Failure Breakdown</h3>
              <ul>
                {Object.entries(failureBreakdown).map(([key, value]) => (
                  <li key={key}>
                    <strong>{key}:</strong> {typeof value === "number" ? value : JSON.stringify(value)}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {notes && <p>{notes}</p>}

          {checks.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table>
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>Severity</th>
                    <th>Message</th>
                    <th>Remediation</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {checks.map((check) => {
                    const severityLabel = severityLabels[check.severity] ?? check.severity;
                    const detailText = formatDetails(check.details);
                    return (
                      <tr key={check.name}>
                        <td>{check.name}</td>
                        <td>{severityLabel}</td>
                        <td>{check.message}</td>
                        <td>{check.remediation || ""}</td>
                        <td>{detailText ? <pre style={{ whiteSpace: "pre-wrap" }}>{detailText}</pre> : ""}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p>No QA checks have been recorded yet for the latest run.</p>
          )}
        </>
      )}
    </div>
  );
}
