"""Project-local .env discovery.

Resolution order (first match wins):
  1. $ENV_FILE  — explicit absolute or relative path. Honors ~ expansion.
  2. .env       next to the entrypoint script.
  3. .env.production  next to the entrypoint script.

If nothing is found, the call is a no-op — the shell wrapper may have already
exported the variables (e.g. via `set -a; source /path/to/env; set +a`), in
which case the Python side has nothing to do.

Contract: entrypoint scripts call `load_project_env(__file__)` BEFORE any
import from `shared.*` (so subsequent module-level os.environ lookups see
the loaded values).

Zero dependencies on other `shared.*` modules — safe to import from anywhere.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def load_project_env(script_file: Optional[str] = None) -> Optional[str]:
    """Load a project env file. Returns the path that was loaded, or None.

    `script_file` should usually be `__file__` from the calling entrypoint.
    Pass `None` to skip script-dir search and rely solely on $ENV_FILE.
    """
    explicit = os.environ.get("ENV_FILE", "").strip()
    if explicit:
        p = Path(explicit).expanduser()
        if p.is_file():
            load_dotenv(p, override=False)
            return str(p)

    if script_file is not None:
        here = Path(script_file).resolve().parent
        for name in (".env", ".env.production"):
            p = here / name
            if p.is_file():
                load_dotenv(p, override=False)
                return str(p)

    return None
