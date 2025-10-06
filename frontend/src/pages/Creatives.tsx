import { useMemo } from "react";

import StageBadge from "../components/StageBadge";
import { useLatestRun } from "../hooks/useLatestRun";
import { findRecordsByColumnKeywords, StringRecord, uniqueColumns } from "../lib/dataTransforms";

function coalesceCreativeTelemetry(runTelemetry: Record<string, unknown> | null | undefined, stageTelemetry: Record<string, unknown> | undefined) {
  if (stageTelemetry && Object.keys(stageTelemetry).length > 0) {
    return stageTelemetry;
  }
  if (runTelemetry && typeof runTelemetry === "object") {
    const creativeData = (runTelemetry as Record<string, unknown>).creatives;
    if (creativeData && typeof creativeData === "object") {
      return creativeData as Record<string, unknown>;
    }
  }
  return {} as Record<string, unknown>;
}

function extractCreativeRows(source: Record<string, unknown>, fallbackRoot: Record<string, unknown> | null | undefined): StringRecord[] {
  const primary = findRecordsByColumnKeywords(source, ["headline"]);
  if (primary.length) {
    return primary;
  }
  return findRecordsByColumnKeywords(fallbackRoot, ["headline"]);
}

function toReadableEntries(value: unknown): { key: string; value: string }[] {
  if (!value || typeof value !== "object") {
    return [];
  }
  return Object.entries(value as Record<string, unknown>).map(([key, raw]) => {
    if (raw === null || raw === undefined) {
      return { key, value: "" };
    }
    if (typeof raw === "number") {
      return { key, value: raw.toLocaleString() };
    }
    if (typeof raw === "string") {
      return { key, value: raw };
    }
    if (Array.isArray(raw)) {
      return { key, value: raw.join(", ") };
    }
    return { key, value: JSON.stringify(raw) };
  });
}

export default function Creatives() {
  const { run, loading, error, refresh } = useLatestRun({ refreshInterval: 15000 });

  const stage = run?.stages.find((item) => item.name === "creatives");
  const stageTelemetry = useMemo<Record<string, unknown>>(
    () => coalesceCreativeTelemetry(run?.telemetry, stage?.telemetry),
    [run?.telemetry, stage?.telemetry],
  );

  const rows = useMemo(
    () => extractCreativeRows(stageTelemetry, run?.telemetry as Record<string, unknown> | null | undefined),
    [stageTelemetry, run?.telemetry],
  );
  const limitedRows = rows.slice(0, 12);
  const columns = useMemo(() => uniqueColumns(limitedRows), [limitedRows]);

  const bucketEntries = toReadableEntries(stageTelemetry["bucket_counts"]);
  const blockerEntries = toReadableEntries(stageTelemetry["blocker_counts"]);
  const toneEntries = toReadableEntries(stageTelemetry["brand_tone"]);
  const duplicateStats = stageTelemetry["duplicate_guard"];
  const conceptsGeneratedValue = stageTelemetry["concepts_generated"];
  const conceptsGenerated = typeof conceptsGeneratedValue === "number" ? conceptsGeneratedValue : undefined;
  const uniqueAudienceValue = stageTelemetry["audience_unique"];
  const uniqueAudience = typeof uniqueAudienceValue === "number" ? uniqueAudienceValue : undefined;

  return (
    <div className="card">
      <h2>Creative Variations</h2>
      <p>Review copy exploration coming out of the generation stage.</p>

      <div style={{ display: "flex", gap: "1rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="button secondary" onClick={refresh} disabled={loading}>
          Refresh
        </button>
        {stage && <StageBadge name={stage.name} status={stage.status} />}
        {loading && <span>Loading creative output…</span>}
        {error && <span style={{ color: "var(--danger)" }}>{error}</span>}
      </div>

      {!run && !loading && <p>No pipeline runs found. Launch a run from the wizard to populate this view.</p>}

      {run && (
        <>
          <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
            <div className="stage-card">
              <h3>Concepts Generated</h3>
              <p>{conceptsGenerated !== undefined ? conceptsGenerated.toLocaleString() : rows.length || "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Unique Audiences</h3>
              <p>{uniqueAudience !== undefined ? uniqueAudience.toLocaleString() : "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Last Updated</h3>
              <p>{stage?.finished_at ? new Date(stage.finished_at).toLocaleString() : new Date(run.updated_at).toLocaleString()}</p>
            </div>
          </div>

          {toneEntries.length > 0 && (
            <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
              {toneEntries.map((entry) => (
                <div key={entry.key} className="stage-card">
                  <h3>{entry.key}</h3>
                  <p>{entry.value}</p>
                </div>
              ))}
            </div>
          )}

          {bucketEntries.length > 0 && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Bucket Coverage</h3>
              <ul>
                {bucketEntries.map((entry) => (
                  <li key={entry.key}>
                    <strong>{entry.key}:</strong> {entry.value}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {blockerEntries.length > 0 && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Blocker Coverage</h3>
              <ul>
                {blockerEntries.map((entry) => (
                  <li key={entry.key}>
                    <strong>{entry.key}:</strong> {entry.value}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {duplicateStats && typeof duplicateStats === "object" && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Duplicate Guard</h3>
              <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(duplicateStats, null, 2)}</pre>
            </div>
          )}

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
                <p>Showing {limitedRows.length} of {rows.length} creative concepts detected from the latest CSV.</p>
              )}
            </div>
          ) : (
            <p>No creative CSV rows available yet. The pipeline will populate this view after the creatives stage completes.</p>
          )}
        </>
      )}
    </div>
  );
}
