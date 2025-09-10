from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional

from ..tagging.azure_openai import AzureOpenAITaggingProvider
from ..taxonomy.loader import load_taxonomy
from ..taxonomy.match import filter_categories
from ..storage.repo import (
    compute_session_speaker_stats,
    load_sessions,
    load_sessions_sync_metadata,
    merge_session_data,
    save_session_speaker_stats,
    save_sessions,
    save_sessions_sync_metadata,
)
from .client import DirectionsSessionsClient, SessionEntry


def analyze_sessions(
    session_list_url: str,
    data_dir: Path,
    batch_size: int,
    taxonomy_path: Optional[Path] = None,
    restrict_to_taxonomy: bool = False,
    skip_ai: bool = False,
    request_timeout: int = 60,
    respect_robots: bool = True,
    delay_min: float = 0.3,
    delay_max: float = 0.8,
    limit: Optional[int] = None,
) -> List[Dict]:
    existing = load_sessions(data_dir)
    sync = load_sessions_sync_metadata(data_dir)

    client = DirectionsSessionsClient(
        session_list_url=session_list_url,
        user_agent=None,
        request_timeout=request_timeout,
        respect_robots=respect_robots,
    )
    collected: List[SessionEntry] = client.collect((delay_min, delay_max), max_items=limit)

    # Build tagger
    tagger: Optional[AzureOpenAITaggingProvider] = None
    if not skip_ai:
        tagger = AzureOpenAITaggingProvider(
            batch_size=batch_size,
            taxonomy_path=taxonomy_path,
            restrict_to_taxonomy=restrict_to_taxonomy,
        )

    # Determine which sessions are new for AI enrichment
    processed_ids = set(sync.get("processed_session_ids", []))
    new_entries = [s for s in collected if (s.session_id or s.detail_url) not in processed_ids]

    # Prepare descriptions for AI
    def to_desc(s: SessionEntry) -> Dict[str, str]:
        return {
            "video_id": s.session_id or s.detail_url,  # reuse key field name to fit existing tagger
            "title": s.title,
            "description": s.description or " ".join(s.categories),
        }

    enriched_map: Dict[str, Dict[str, List[str]]] = {}
    if tagger and new_entries:
        for i in range(0, len(new_entries), batch_size):
            batch = new_entries[i : i + batch_size]
            ai_results = tagger.analyze_content_batch([to_desc(s) for s in batch])
            for res in ai_results:
                key = res["video_id"]
                enriched_map[str(key)] = {
                    "speakers": [str(x) for x in (res.get("speakers") or [])],
                    "categories": [str(x) for x in (res.get("categories") or [])],
                }

    # Merge AI results into collected sessions
    merged_pre = []
    for s in collected:
        key = s.session_id or s.detail_url
        ai = enriched_map.get(str(key), None)
        speakers = s.speakers
        categories = s.categories
        if ai:
            if ai.get("speakers"):
                speakers = sorted({*speakers, *ai["speakers"]})
            if ai.get("categories"):
                categories = sorted({*categories, *ai["categories"]})
        
        # Convert SessionEntry to dict with structured fields
        session_dict = {
            "session_id": s.session_id,
            "title": s.title,
            "detail_url": s.detail_url,
            "url": s.detail_url,  # Add url field for consistency with videos.json
            "speakers": speakers,
            "categories": categories,
            "description": s.description,
            "source": s.source,
            # Add structured fields
            "session_type": s.session_type,
            "level": s.level,
            "roles": s.roles,
            "product": s.product,
            "areas": s.areas,
            "submitter": s.submitter,
        }
        merged_pre.append(session_dict)

    # Optional taxonomy restriction after enrichment
    if restrict_to_taxonomy and taxonomy_path:
        tax = load_taxonomy(taxonomy_path)
        for e in merged_pre:
            e["categories"] = filter_categories(e.get("categories", []), tax)

    # Merge with existing, preferring existing human edits
    merged = merge_session_data(merged_pre, existing)
    save_sessions(data_dir, merged)

    sync["processed_session_ids"] = [e.get("session_id") or e.get("detail_url") for e in merged]
    save_sessions_sync_metadata(data_dir, sync)

    stats = compute_session_speaker_stats(merged)
    save_session_speaker_stats(data_dir, stats)

    return merged


