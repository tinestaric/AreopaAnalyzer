from __future__ import annotations

import logging
from typing import Dict, List, Optional

from .client import YouTubeClient
from ..tagging.azure_openai import AzureOpenAITaggingProvider


logger = logging.getLogger(__name__)


def _normalize_title(title: str) -> str:
    words = title.split()
    if words and words[0].isdigit():
        title = " ".join(words[1:])
        if title.lstrip().startswith("-"):
            title = title.lstrip()[1:].lstrip()
    return title


def build_entries_from_details(details: Dict, tagging_results: Dict[str, Dict]) -> List[Dict]:
    entries: List[Dict] = []
    for video in details.get("items", []):
        vid = video["id"]
        snippet = video["snippet"]
        stats = video["statistics"]
        title = _normalize_title(snippet.get("title", ""))
        res = tagging_results.get(vid, {"speakers": [], "categories": []})
        entries.append(
            {
                "title": title,
                "views": int(stats.get("viewCount", 0)),
                "url": f"https://youtube.com/watch?v={vid}",
                "description": snippet.get("description", ""),
                "published_at": snippet.get("publishedAt", ""),
                "speakers": res["speakers"],
                "categories": res["categories"],
            }
        )
    return entries


def build_simple_entries_from_details(details: Dict) -> List[Dict]:
    entries: List[Dict] = []
    for video in details.get("items", []):
        vid = video["id"]
        snippet = video["snippet"]
        stats = video["statistics"]
        title = _normalize_title(snippet.get("title", ""))
        entries.append(
            {
                "title": title,
                "views": int(stats.get("viewCount", 0)),
                "url": f"https://youtube.com/watch?v={vid}",
                "description": snippet.get("description", ""),
                "published_at": snippet.get("publishedAt", ""),
                "speakers": [],
                "categories": [],
            }
        )
    return entries


def analyze_channel(
    yt: YouTubeClient,
    channel_id: str,
    processed_ids: List[str],
    tagger: AzureOpenAITaggingProvider,
    limit: Optional[int] = None,
) -> List[Dict]:
    all_entries: List[Dict] = []
    next_token: Optional[str] = None
    processed_set = set(processed_ids or [])

    while True:
        video_ids, next_token = yt.fetch_video_ids(channel_id, limit=limit, page_token=next_token)
        if not video_ids:
            break

        details = yt.get_video_details(video_ids)
        new_videos, existing_videos = [], []
        for item in details.get("items", []):
            (existing_videos if item["id"] in processed_set else new_videos).append(item)

        # Tag only new videos
        tagging_results: Dict[str, Dict] = {}
        if new_videos:
            batch: List[Dict[str, str]] = [
                {"video_id": v["id"], "title": v["snippet"]["title"], "description": v["snippet"].get("description", "")}
                for v in new_videos
            ]
            results = tagger.analyze_content_batch(batch)
            for r in results:
                tagging_results[r["video_id"]] = {"speakers": r["speakers"], "categories": r["categories"]}

        all_entries.extend(build_entries_from_details({"items": new_videos}, tagging_results))
        all_entries.extend(build_simple_entries_from_details({"items": existing_videos}))

        if not next_token or (limit and len(all_entries) >= limit):
            break

    # Sort by publish date (ISO-8601) descending; falls back to empty string if missing
    all_entries = sorted(all_entries, key=lambda x: x.get("published_at", ""), reverse=True)
    if limit:
        all_entries = all_entries[:limit]
    return all_entries

