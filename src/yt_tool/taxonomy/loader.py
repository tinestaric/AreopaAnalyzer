from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def load_taxonomy(path: Path | None) -> Dict[str, Any] | None:
    if not path:
        return None
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

