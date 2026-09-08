from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def load_taxonomy(path: Path | None) -> Dict[str, Any] | None:
    if not path:
        return None
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def subcategory_descriptions(entry: Any) -> Dict[str, str]:
    """Normalize a taxonomy category entry into {subcategory_name: description}.

    Supports both the legacy list-of-names form and the newer
    {"description": ..., "subcategories": {name: description}} form.
    """
    if isinstance(entry, list):
        return {name: "" for name in entry}
    if isinstance(entry, dict):
        subs = entry.get("subcategories", {})
        if isinstance(subs, list):
            return {name: "" for name in subs}
        if isinstance(subs, dict):
            return {name: (desc or "") for name, desc in subs.items()}
    return {}


def category_description(entry: Any) -> str:
    if isinstance(entry, dict):
        return entry.get("description", "") or ""
    return ""


def flatten_names(taxonomy: Dict[str, Any]) -> Dict[str, List[str]]:
    """Category name -> list of its subcategory names, regardless of schema shape."""
    return {category: list(subcategory_descriptions(entry).keys()) for category, entry in taxonomy.items()}


def format_taxonomy_for_prompt(taxonomy: Dict[str, Any]) -> str:
    """Render the taxonomy as a human-readable, description-annotated list for an LLM prompt."""
    lines: List[str] = []
    for category, entry in taxonomy.items():
        cat_desc = category_description(entry)
        lines.append(f"- {category}" + (f": {cat_desc}" if cat_desc else ""))
        for sub, sub_desc in subcategory_descriptions(entry).items():
            lines.append(f"    - {sub}" + (f": {sub_desc}" if sub_desc else ""))
    return "\n".join(lines)
