import { useMemo } from "react";

import StageBadge from "../components/StageBadge";
import { useLatestRun } from "../hooks/useLatestRun";
import { AssetRecord } from "../lib/api";

type ImageCard = {
  concept: string;
  path: string;
  provider?: string;
  cost?: string;
  contrast?: string;
};

function toImageCards(telemetryAssets: unknown, assetRecords: AssetRecord[]): ImageCard[] {
  const cards: ImageCard[] = [];
  if (Array.isArray(telemetryAssets)) {
    for (const item of telemetryAssets) {
      if (!item || typeof item !== "object") continue;
      const record = item as Record<string, unknown>;
      const conceptId = typeof record["concept_id"] === "string" ? record["concept_id"] : undefined;
      const path = typeof record["path"] === "string" ? record["path"] : undefined;
      if (!conceptId || !path) continue;
      const provider = typeof record["provider"] === "string" ? record["provider"] : undefined;
      const costValue = record["cost"];
      const cost = typeof costValue === "number" ? `$${costValue.toFixed(2)}` : undefined;
      const contrast = typeof record["contrast_ratio"] === "number" ? record["contrast_ratio"].toFixed(2) : undefined;
      cards.push({ concept: conceptId, path, provider, cost, contrast });
    }
  }

  const recorded = assetRecords
    .filter((asset) => asset.stage === "images")
    .map((asset) => {
      const metadata = asset.metadata || {};
      const concept = typeof metadata["concept_id"] === "string" ? metadata["concept_id"] : asset.asset_type;
      const provider = typeof metadata["render"] === "object" && metadata["render"] && typeof (metadata["render"] as Record<string, unknown>)["mode"] === "string"
        ? ((metadata["render"] as Record<string, unknown>)["mode"] as string)
        : undefined;
      const costValue = typeof metadata["cost"] === "number" ? metadata["cost"] : undefined;
      const contrastValue =
        typeof metadata["overlay"] === "object" && metadata["overlay"] && typeof (metadata["overlay"] as Record<string, unknown>)["contrast"] === "object"
          ? ((metadata["overlay"] as Record<string, unknown>)["contrast"] as Record<string, unknown>)["ratio"]
          : undefined;
      const contrast = typeof contrastValue === "number" ? contrastValue.toFixed(2) : undefined;
      return {
        concept,
        path: asset.storage_key,
        provider,
        cost: costValue !== undefined ? `$${costValue.toFixed(2)}` : undefined,
        contrast,
      };
    });

  const combined = [...cards];
  for (const record of recorded) {
    if (!combined.some((card) => card.path === record.path)) {
      combined.push(record);
    }
  }
  return combined;
}

export default function Images() {
  const { run, assets, loading, error, refresh } = useLatestRun({ refreshInterval: 15000 });

  const stage = run?.stages.find((item) => item.name === "images");
  const stageTelemetry = (stage?.telemetry && typeof stage.telemetry === "object" ? stage.telemetry : {}) as Record<string, unknown>;

  const cards = useMemo(() => toImageCards(stageTelemetry["assets"], assets), [stageTelemetry, assets]);

  const provider = typeof stageTelemetry["provider"] === "string" ? (stageTelemetry["provider"] as string) : undefined;
  const renderedCount = typeof stageTelemetry["rendered"] === "number" ? (stageTelemetry["rendered"] as number) : cards.length;
  const requestedCount = typeof stageTelemetry["requested"] === "number" ? (stageTelemetry["requested"] as number) : undefined;
  const costValue = typeof stageTelemetry["cost"] === "number" ? (stageTelemetry["cost"] as number) : undefined;

  return (
    <div className="card">
      <h2>Image Generation</h2>
      <p>Curated visual options ready for QA review.</p>

      <div style={{ display: "flex", gap: "1rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="button secondary" onClick={refresh} disabled={loading}>
          Refresh
        </button>
        {stage && <StageBadge name={stage.name} status={stage.status} />}
        {loading && <span>Loading image telemetry…</span>}
        {error && <span style={{ color: "var(--danger)" }}>{error}</span>}
      </div>

      {!run && !loading && <p>No pipeline runs found. Launch a run from the wizard to populate this view.</p>}

      {run && (
        <>
          <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
            <div className="stage-card">
              <h3>Provider</h3>
              <p>{provider ?? "Unknown"}</p>
            </div>
            <div className="stage-card">
              <h3>Rendered</h3>
              <p>{renderedCount}</p>
            </div>
            <div className="stage-card">
              <h3>Requested</h3>
              <p>{requestedCount ?? "–"}</p>
            </div>
            <div className="stage-card">
              <h3>Total Cost</h3>
              <p>{costValue !== undefined ? `$${costValue.toFixed(2)}` : "–"}</p>
            </div>
          </div>

          {cards.length > 0 ? (
            <div className="stage-grid">
              {cards.map((card) => (
                <div key={`${card.concept}-${card.path}`} className="stage-card">
                  <h3>{card.concept}</h3>
                  <p>{card.provider ? `Provider: ${card.provider}` : ""}</p>
                  {card.cost && <p>Cost: {card.cost}</p>}
                  {card.contrast && <p>Contrast ratio: {card.contrast}</p>}
                  <p style={{ wordBreak: "break-all" }}>{card.path}</p>
                </div>
              ))}
            </div>
          ) : (
            <p>No rendered images detected yet. They will appear here when the images stage completes.</p>
          )}
        </>
      )}
    </div>
  );
}
