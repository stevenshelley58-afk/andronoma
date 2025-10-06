const API_URL = (globalThis as any).__API_URL__ as string;

type RequestOptions = RequestInit & { token?: string | null };

export type StageStatus = "pending" | "running" | "completed" | "failed" | "skipped";

export interface StageTelemetry {
  name: string;
  status: StageStatus;
  started_at: string | null;
  finished_at: string | null;
  telemetry: Record<string, unknown>;
  budget_spent: number;
  notes: string;
}

export interface Run {
  id: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  input_payload: Record<string, unknown> | null;
  budgets: Record<string, number> | null;
  telemetry: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
  stages: StageTelemetry[];
}

export interface RunListResponse {
  runs: Run[];
}

export interface AssetRecord {
  id: string;
  run_id: string;
  stage: string;
  asset_type: string;
  storage_key: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface AssetListResponse {
  assets: AssetRecord[];
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(options.headers || {})
  };
  if (options.token) {
    headers["Authorization"] = `Bearer ${options.token}`;
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || response.statusText);
  }

  if (response.status === 204) {
    return {} as T;
  }

  return (await response.json()) as T;
}

export async function loginRequest(email: string, password: string) {
  return request<{ access_token: string }>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export async function registerRequest(email: string, password: string) {
  return request("/auth/register", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export async function listRuns(token: string) {
  return request<RunListResponse>("/runs", { token });
}

export async function createRun(token: string, payload: any) {
  return request<Run>("/runs", { method: "POST", token, body: JSON.stringify(payload) });
}

export async function startRun(token: string, id: string) {
  return request<Run>(`/runs/${id}/start`, { method: "POST", token });
}

export async function getPipeline(token: string | null) {
  return request<{ stages: string[] }>("/pipeline", { token: token ?? undefined });
}

export async function getSettings(token: string | null) {
  return request<Record<string, unknown>>("/settings", { token: token ?? undefined });
}

export async function getRun(token: string, id: string) {
  return request<Run>(`/runs/${id}`, { token });
}

export async function listRunAssets(token: string, id: string) {
  return request<AssetListResponse>(`/runs/${id}/assets`, { token });
}
