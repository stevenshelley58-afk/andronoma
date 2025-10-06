import { useMemo } from "react";

import StageBadge from "../components/StageBadge";
import { useLatestRun } from "../hooks/useLatestRun";
import { AssetRecord } from "../lib/api";

type ExportLink = {
  label: string;
  url: string;
};

function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || Number.isNaN(bytes)) {
    return "–";
  }
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / 1024 ** index;
  return `${value.toFixed(1)} ${units[index]}`;
}

function linkFromTelemetry(entry: Record<string, unknown> | undefined, fallbackLabel: string): ExportLink | null {
  if (!entry) return null;
  const signedUrl = typeof entry["signed_url"] === "string" ? entry["signed_url"] : null;
  const storageKey = typeof entry["storage_key"] === "string" ? entry["storage_key"] : null;
  const label = typeof entry["label"] === "string" ? entry["label"] : fallbackLabel;
  const url = signedUrl || storageKey;
  if (!url) return null;
  return { label, url };
}

function buildExportLinks(telemetry: Record<string, unknown>): ExportLink[] {
  const links: ExportLink[] = [];
  const bundle = telemetry["bundle"] && typeof telemetry["bundle"] === "object" ? (telemetry["bundle"] as Record<string, unknown>) : undefined;
  const manifest = telemetry["manifest"] && typeof telemetry["manifest"] === "object" ? (telemetry["manifest"] as Record<string, unknown>) : undefined;
  const optionalExports = telemetry["optional_exports"] && typeof telemetry["optional_exports"] === "object" ? (telemetry["optional_exports"] as Record<string, unknown>) : undefined;

  const bundleLink = linkFromTelemetry(bundle, "Export bundle");
  if (bundleLink) {
    links.push(bundleLink);
  }

  const manifestLink = linkFromTelemetry(manifest, "Manifest JSON");
  if (manifestLink) {
    links.push(manifestLink);
  }

  if (optionalExports) {
    const google = optionalExports["google_sheets"];
    if (google && typeof google === "object" && typeof (google as Record<string, unknown>)["workbook_url"] === "string") {
      links.push({ label: "Google Sheets Workbook", url: (google as Record<string, unknown>)["workbook_url"] as string });
    }
    const meta = optionalExports["meta"];
    if (meta && typeof meta === "object" && typeof (meta as Record<string, unknown>)["download_url"] === "string") {
      links.push({ label: "Meta Ads CSV", url: (meta as Record<string, unknown>)["download_url"] as string });
    }
  }

  return links;
}

function exportAssetsFromRecords(records: AssetRecord[]): AssetRecord[] {
  return records.filter((asset) => asset.stage === "export");
}

export default function ExportPage() {
  const { run, assets, loading, error, refresh } = useLatestRun({ refreshInterval: 15000 });

  const stage = run?.stages.find((item) => item.name === "export");
  const telemetry = useMemo(() => {
    if (stage?.telemetry && Object.keys(stage.telemetry).length > 0) {
      return stage.telemetry as Record<string, unknown>;
    }
    if (run?.telemetry && typeof run.telemetry === "object") {
      const exportTelemetry = (run.telemetry as Record<string, unknown>).export;
      if (exportTelemetry && typeof exportTelemetry === "object") {
        return exportTelemetry as Record<string, unknown>;
      }
    }
    return {} as Record<string, unknown>;
  }, [stage?.telemetry, run?.telemetry]);

  const exportLinks = useMemo(() => buildExportLinks(telemetry), [telemetry]);
  const exportAssets = useMemo(() => exportAssetsFromRecords(assets), [assets]);

  const bundleInfo = telemetry["bundle"] && typeof telemetry["bundle"] === "object" ? (telemetry["bundle"] as Record<string, unknown>) : undefined;
  const manifestInfo = telemetry["manifest"] && typeof telemetry["manifest"] === "object" ? (telemetry["manifest"] as Record<string, unknown>) : undefined;
  const assetCounts = telemetry["asset_counts"] && typeof telemetry["asset_counts"] === "object" ? (telemetry["asset_counts"] as Record<string, unknown>) : {};
  const readmeEntries = telemetry["readme_map"] && Array.isArray(telemetry["readme_map"]) ? (telemetry["readme_map"] as Array<Record<string, unknown>>) : [];

  const bundleSizeValue = bundleInfo ? bundleInfo["size_bytes"] : undefined;
  const bundleSize = typeof bundleSizeValue === "number" ? bundleSizeValue : undefined;
  const manifestSizeValue = manifestInfo ? manifestInfo["size_bytes"] : undefined;
  const manifestSize = typeof manifestSizeValue === "number" ? manifestSizeValue : undefined;

  return (
    <div className="card">
      <h2>Export Center</h2>
      <p>Download campaign artifacts once QA approves the run.</p>

      <div style={{ display: "flex", gap: "1rem", alignItems: "center", marginBottom: "1rem" }}>
        <button className="button secondary" onClick={refresh} disabled={loading}>
          Refresh
        </button>
        {stage && <StageBadge name={stage.name} status={stage.status} />}
        {loading && <span>Loading export metadata…</span>}
        {error && <span style={{ color: "var(--danger)" }}>{error}</span>}
      </div>

      {!run && !loading && <p>No pipeline runs found. Launch a run from the wizard to populate this view.</p>}

      {run && (
        <>
          <div className="stage-grid" style={{ marginBottom: "1.5rem" }}>
            <div className="stage-card">
              <h3>Bundle Size</h3>
              <p>{formatBytes(bundleSize)}</p>
            </div>
            <div className="stage-card">
              <h3>Manifest Size</h3>
              <p>{formatBytes(manifestSize)}</p>
            </div>
            <div className="stage-card">
              <h3>Assets Included</h3>
              <p>
                {Object.values(assetCounts).length > 0
                  ? Object.entries(assetCounts)
                      .map(([key, value]) => {
                        if (typeof value === "number") return `${key}: ${value}`;
                        if (typeof value === "string") return `${key}: ${value}`;
                        return `${key}: ${JSON.stringify(value)}`;
                      })
                      .join(", ")
                  : "–"}
              </p>
            </div>
            <div className="stage-card">
              <h3>Last Updated</h3>
              <p>{stage?.finished_at ? new Date(stage.finished_at).toLocaleString() : new Date(run.updated_at).toLocaleString()}</p>
            </div>
          </div>

          {exportLinks.length > 0 && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Download Links</h3>
              <ul>
                {exportLinks.map((link) => (
                  <li key={link.url}>
                    <a href={link.url} target="_blank" rel="noreferrer">
                      {link.label}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {readmeEntries.length > 0 && (
            <div style={{ marginBottom: "1.5rem" }}>
              <h3>Bundle Manifest</h3>
              <ul>
                {readmeEntries.map((entry, index) => {
                  const title = typeof entry["title"] === "string" ? entry["title"] : `Entry ${index + 1}`;
                  const description = typeof entry["description"] === "string" ? entry["description"] : "";
                  const path = typeof entry["path"] === "string" ? entry["path"] : undefined;
                  const url = typeof entry["url"] === "string" ? entry["url"] : undefined;
                  return (
                    <li key={`${title}-${index}`}>
                      <strong>{title}</strong>
                      {path && <span> — {path}</span>}
                      {url && (
                        <span>
                          {" "}
                          (<a href={url} target="_blank" rel="noreferrer">external</a>)
                        </span>
                      )}
                      {description && <div>{description}</div>}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}

          <div style={{ overflowX: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th>Asset</th>
                  <th>Storage Key</th>
                  <th>Metadata</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {exportAssets.length > 0 ? (
                  exportAssets.map((asset) => (
                    <tr key={asset.id}>
                      <td>{asset.asset_type}</td>
                      <td style={{ wordBreak: "break-all" }}>{asset.storage_key}</td>
                      <td>
                        <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(asset.metadata, null, 2)}</pre>
                      </td>
                      <td>{new Date(asset.created_at).toLocaleString()}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={4}>No export assets recorded yet.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
