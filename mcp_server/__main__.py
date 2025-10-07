"""Entry point for running the MCP server with ``python -m mcp_server``."""
from __future__ import annotations

import os

import uvicorn


def main() -> None:
    """Launch the FastAPI app under Uvicorn."""

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("mcp_server.app:app", host=host, port=port, reload=False, factory=False)


if __name__ == "__main__":
    main()
