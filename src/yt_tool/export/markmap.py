from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional


def generate_markmap(
    videos: List[Dict],
    taxonomy: Optional[Dict] = None,
    root_title: str = "YouTube Channel",
    root_url: str | None = None,
) -> str:
    lines: List[str] = [
        "---",
        "markmap:",
        "  initialExpandLevel: 2",
        "  colorFreezeLevel: 3",
        "  duration: 1000",
        "  spacingVertical: 10",
        "---\n",
    ]

    if root_url:
        lines.append(f"# [{root_title}]({root_url})\n")
    else:
        lines.append(f"# {root_title}\n")

    if taxonomy and isinstance(taxonomy, dict):
        for category, subcats in taxonomy.items():
            lines.append(f"## {category}")
            cat_videos = [v for v in videos if category in v.get("categories", [])]
            for i, v in enumerate(cat_videos, 1):
                title = (v.get("title") or "").replace("[", "").replace("]", "")
                url = v.get("url")
                lines.append(f"### {i}. [{title}]({url})")
            if isinstance(subcats, list):
                for sub in subcats:
                    lines.append(f"### {sub}")
                    sub_videos = [v for v in videos if sub in v.get("categories", [])]
                    for j, v in enumerate(sub_videos, 1):
                        title = (v.get("title") or "").replace("[", "").replace("]", "")
                        url = v.get("url")
                        lines.append(f"#### {j}. [{title}]({url})")
    else:
        # Simple by-category tree from observed categories
        observed = sorted({c for v in videos for c in v.get("categories", [])})
        for category in observed:
            lines.append(f"## {category}")
            for i, v in enumerate([v for v in videos if category in v.get("categories", [])], 1):
                title = (v.get("title") or "").replace("[", "").replace("]", "")
                url = v.get("url")
                lines.append(f"### {i}. [{title}]({url})")

    return "\n".join(lines)


def save_markmap(content: str, output_path: Path) -> None:
    output_path.write_text(content, encoding="utf-8")

