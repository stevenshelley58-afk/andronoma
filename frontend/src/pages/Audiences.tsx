import { useMemo } from "react";

import StageBadge from "../components/StageBadge";
import { useLatestRun } from "../hooks/useLatestRun";
import { findRecordsByColumnKeywords, StringRecord, uniqueColumns } from "../lib/dataTransforms";

function coalesceAudienceTelemetry(runTelemetry: Record<string, unknown> | null | undefined, stageTelemetry: Record<string, unknown> | undefined) {
  if (stageTelemetry && Object.keys(stageTelemetry).length > 0) {
    return stageTelemetry;
  }
  if (runTelemetry && typeof runTelemetry === "object") {
    const audienceData = (runTelemetry as Record<string, unknown>).audiences;
    if (audienceData && typeof audienceData === "object") {
      return audienceData as Record<string, unknown>;
    }
  }
  return {} as Record<string, unknown>;
}

function extractAudienceRows(source: Record<string, unknown>, fallbackRoot: Record<string, unknown> | null | undefined): StringRecord[] {
  const primary = findRecordsByColumnKeywords(source, ["audience", "name"]);
  if (primary.length) {
    return primary;
  }
  return findRecordsByColumnKeywords(fallbackRoot, ["audience", "name"]);
}

export default function Audiences() {
  const { run, loading, error, refresh } = useLatestRun({ refreshInterval: 15000 });

  const stage = run?.stages.find((item) => item.name === "audiences");
  const stageTelemetry = useMemo<Record<string, unknown>>(
    () => coalesceAudienceTelemetry(run?.telemetry, stage?.telemetry),
    [run?.telemetry, stage?.telemetry],
  );

  const rows = useMemo(
    () => extractAudienceRows(stageTelemetry, run?.telemetry as Record<string, unknown> | null | undefined),
    [stageTelemetry, run?.telemetry],
  );
  const limitedRows = rows.slice(0, 12);
  const columns = useMemo(() => uniqueColumns(limitedRows), [limitedRows]);

  const segmentsValue = stageTelemetry["segments"];
  const segments = typeof segmentsValue === "number" ? segmentsValue : undefined;
  const personasValue = stageTelemetry["personas"];
  const personas = typeof personasValue === "number" ? personasValue : undefined;
  const recordsCountValue = stageTelemetry["records_count"];
  const recordsCount = typeof recordsCountValue === "number" ? recordsCountValue : undefined;
  const notesValue = stageTelemetry["notes"];
  const notes = typeof notesValue === "string" ? notesValue : stage?.notes;

  return (
    <div className="card">
      <h2>Audience Intelligence</h2>
      <p>Review the personas and segments generated during the NLP stages.</p>

      <div style={{ display: "flex", gap: "1rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="button secondary" onClick={refresh} disabled={loading}>
          Refresh
        </button>
        {stage && <StageBadge name={stage.name} status={stage.status} />}
        {loading && <span>Loading latest audiences…</span>}
        {error && <span style={{ color: "var(--danger)" }}>{error}</span>}
      </div>

      {!run && !loading && <p>No pipeline runs found. Launch a run from the wizard to populate this view.</p>}

      {run && (
        <>
          <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
            <div className="stage-card">
              <h3>Segments</h3>
              <p>{segments ?? recordsCount ?? rows.length || "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Personas</h3>
              <p>{personas ?? "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Last Updated</h3>
              <p>{stage?.finished_at ? new Date(stage.finished_at).toLocaleString() : new Date(run.updated_at).toLocaleString()}</p>
            </div>
          </div>

          {notes && <p>{notes}</p>}

          {limitedRows.length > 0 ? (
            <div style={{ overflowX: "auto" }}>
              <table>
                <thead>
                  <tr>
                    {columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {limitedRows.map((row, index) => (
                    <tr key={index}>
                      {columns.map((column) => (
                        <td key={column}>{row[column] ?? ""}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {rows.length > limitedRows.length && (
                <p>Showing {limitedRows.length} of {rows.length} rows detected from the latest audience CSV.</p>
              )}
            </div>
          ) : (
            <p>No audience records available yet. The pipeline will populate this view once the audiences stage finishes.</p>
          )}
        </>
      )}
    </div>
  );
}
