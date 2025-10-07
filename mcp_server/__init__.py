"""MCP-compatible HTTP server wrapping the scrape crawler."""

__all__ = ["create_app"]

from .app import create_app  # noqa: E402,F401
