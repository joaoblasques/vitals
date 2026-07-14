"""Shared pytest fixtures.

The unit tests exercise the de-identification *logic*, not the secrecy of the salt, so they run with
a deterministic dummy `VITALS_SALT` (ADR 0002). Set before any test imports/builds silver SQL, and
only if the environment hasn't already provided one (a real `.env` / CI secret still wins)."""
from __future__ import annotations

import os


def pytest_configure(config):  # noqa: ARG001 — pytest hook signature
    os.environ.setdefault("VITALS_SALT", "test-salt-not-a-secret")
