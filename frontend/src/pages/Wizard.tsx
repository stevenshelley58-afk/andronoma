import { FormEvent, useState } from "react";

import { useAuth } from "../hooks/useAuth";
import { createRun, registerRequest, startRun } from "../lib/api";

const defaultBudgets = {
  scrape: 100,
  process: 150,
  audiences: 200,
  creatives: 200,
  images: 150,
  qa: 100,
  export: 100,
};

export default function Wizard() {
  const { token, login } = useAuth();
  const [form, setForm] = useState({
    name: "New Campaign",
    objective: "Increase brand awareness",
    audience: "Tech decision makers",
    notes: "",
  });
  const [credentials, setCredentials] = useState({ email: "demo@example.com", password: "password" });
  const [message, setMessage] = useState<string | null>(null);

  const handleLogin = async (event: FormEvent) => {
    event.preventDefault();
    try {
      await login(credentials.email, credentials.password);
      setMessage("Authenticated successfully.");
    } catch (error) {
      setMessage((error as Error).message);
    }
  };

  const handleRegister = async () => {
    try {
      await registerRequest(credentials.email, credentials.password);
      setMessage("Account created. Please log in.");
    } catch (error) {
      setMessage((error as Error).message);
    }
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    if (!token) {
      setMessage("Please authenticate first.");
      return;
    }
    try {
      const payload = {
        config: {
          name: form.name,
          objectives: [form.objective],
          target_markets: [form.audience],
          metadata: { notes: form.notes },
        },
        budgets: defaultBudgets,
      };
      const run = await createRun(token, payload);
      await startRun(token, run.id);
      setMessage(`Run ${run.id} started.`);
    } catch (error) {
      setMessage((error as Error).message);
    }
  };

  return (
    <div className="card">
      <h2>Campaign Wizard</h2>
      <p>Authenticate, configure, and launch a full pipeline run.</p>

      <form onSubmit={handleLogin}>
        <h3>Authentication</h3>
        <label>
          Email
          <input
            value={credentials.email}
            onChange={(event) => setCredentials({ ...credentials, email: event.target.value })}
            type="email"
            required
          />
        </label>
        <label>
          Password
          <input
            value={credentials.password}
            onChange={(event) => setCredentials({ ...credentials, password: event.target.value })}
            type="password"
            required
          />
        </label>
        <div style={{ display: "flex", gap: "1rem" }}>
          <button type="submit" className="button">
            Log in
          </button>
          <button type="button" className="button secondary" onClick={handleRegister}>
            Register
          </button>
        </div>
      </form>

      <form onSubmit={handleSubmit}>
        <h3>Pipeline configuration</h3>
        <label>
          Campaign name
          <input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} />
        </label>
        <label>
          Primary objective
          <input value={form.objective} onChange={(event) => setForm({ ...form, objective: event.target.value })} />
        </label>
        <label>
          Target audience
          <input value={form.audience} onChange={(event) => setForm({ ...form, audience: event.target.value })} />
        </label>
        <label>
          Notes
          <textarea value={form.notes} onChange={(event) => setForm({ ...form, notes: event.target.value })} />
        </label>
        <button type="submit" className="button">
          Launch pipeline
        </button>
      </form>

      {message && <p>{message}</p>}
    </div>
  );
}
