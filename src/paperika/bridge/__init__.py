"""paperika institutional-download bridge (AGA-260).

A manual/on-demand FastAPI service that drives a codex-steered browser session over
a managed persistent Chrome+CDP to fetch one academic-paper PDF via this machine's
institutional (IP-based) access. Paperika is retained as the queue / dedupe /
identity-verification / retry-bookkeeping / notifications layer.

Public surface: ``create_app`` (FastAPI factory). The module-level ``app`` (built
lazily) is the uvicorn entry point: ``paperika.bridge.app:app``.
"""

from __future__ import annotations

from .app import create_app

__all__ = ["create_app"]
