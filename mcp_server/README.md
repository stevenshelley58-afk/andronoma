# Andronoma MCP Server

This service exposes the Playwright-based crawler behind an [MCP](https://github.com/modelcontextprotocol) compatible HTTP API.

## Running locally

```bash
pip install -r requirements.txt
uvicorn mcp_server.app:app --host 0.0.0.0 --port 8080
```

Open `http://localhost:8080/healthz` to confirm the server is ready. MCP clients should target `http://localhost:8080/mcp` for tool discovery and invocation.

## Deploying

### Render
1. Create a new **Web Service** and connect this repository.
2. Set the build command to `pip install -r requirements.txt`.
3. Use the start command `uvicorn mcp_server.app:app --host 0.0.0.0 --port $PORT`.
4. Ensure the service is on a free instance type and add any required environment variables (e.g. `REDIS_URL`).

### Railway
1. Create a new **Service** from this repository.
2. Under **Deployments**, set the Nixpacks/Build command to `pip install -r requirements.txt` (Railway will infer Python when `requirements.txt` is present).
3. Set the start command to `uvicorn mcp_server.app:app --host 0.0.0.0 --port $PORT`.
4. Expose the generated domain and configure environment variables as needed.

Both platforms automatically inject the `PORT` environment variable used above. Once deployed, update `mcp.json`'s `server_url` if clients need to target the hosted endpoint.
