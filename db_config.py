"""Helpers to load database credentials without hard-coding secrets."""

from __future__ import annotations

import os
from typing import Any, Dict, Mapping, Optional


def load_db_config(overrides: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """
    Build a psycopg2 connection dict using env vars and optional overrides.

    Looks for standard Postgres env vars (PGHOST, PGPORT, PGDATABASE, PGUSER,
    PGPASSWORD). Any values provided via overrides take precedence. Raises a
    RuntimeError when required fields are missing to avoid silently connecting
    with partial credentials.
    """

    config: Dict[str, Any] = {
        "host": os.environ.get("PGHOST"),
        "port": os.environ.get("PGPORT"),
        "dbname": os.environ.get("PGDATABASE"),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
    }

    if overrides:
        for key, value in overrides.items():
            if value not in (None, ""):
                config[key] = value

    port_value = config.get("port")
    if port_value in (None, ""):
        config["port"] = 5432
    else:
        config["port"] = int(port_value)

    missing = [field for field in ("host", "dbname", "user", "password") if not config.get(field)]
    if missing:
        raise RuntimeError(
            "Missing database settings. Set PGHOST, PGDATABASE, PGUSER, PGPASSWORD "
            "or pass overrides. Missing: " + ", ".join(missing)
        )

    return config

