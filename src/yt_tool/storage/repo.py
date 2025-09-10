from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


def ensure_data_dir(data_dir: Path) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def _read_json(path: Path, default: Any) -> Any:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def _write_json(path: Path, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_videos(data_dir: Path) -> List[Dict]:
    return _read_json(data_dir / "videos.json", default=[])


def save_videos(data_dir: Path, videos: List[Dict]) -> None:
    _write_json(data_dir / "videos.json", videos)


def load_sync_metadata(data_dir: Path) -> Dict:
    return _read_json(
        data_dir / "sync_metadata.json",
        default={"last_sync": None, "processed_video_ids": []},
    )


def save_sync_metadata(data_dir: Path, metadata: Dict) -> None:
    _write_json(data_dir / "sync_metadata.json", metadata)


def merge_video_data(new_videos: List[Dict], existing_videos: List[Dict]) -> List[Dict]:
    url_to_existing: Dict[str, Dict] = {v["url"]: v for v in existing_videos}
    for video in new_videos:
        if video["url"] in url_to_existing:
            existing = url_to_existing[video["url"]]
            video["speakers"] = existing.get("speakers", [])
            video["categories"] = existing.get("categories", [])
    return new_videos


def compute_speaker_stats(videos: List[Dict]) -> Dict:
    speaker_stats: Dict[str, Dict[str, Any]] = {}
    for video in videos:
        for speaker in video.get("speakers", []):
            if speaker not in speaker_stats:
                speaker_stats[speaker] = {"name": speaker, "appearances": 0, "videos": []}
            speaker_stats[speaker]["appearances"] += 1
            speaker_stats[speaker]["videos"].append(
                {"title": video["title"], "url": video["url"], "views": video.get("views", 0)}
            )

    speakers_list = list(speaker_stats.values())
    speakers_list.sort(key=lambda x: x["appearances"], reverse=True)
    return {
        "speakers": speakers_list,
        "total_speakers": len(speakers_list),
    }


def save_speaker_stats(data_dir: Path, stats: Dict) -> None:
    _write_json(data_dir / "speaker_stats.json", stats)


# Sessions storage helpers

def load_sessions(data_dir: Path) -> List[Dict]:
    return _read_json(data_dir / "sessions.json", default=[])


def save_sessions(data_dir: Path, sessions: List[Dict]) -> None:
    _write_json(data_dir / "sessions.json", sessions)


def load_sessions_sync_metadata(data_dir: Path) -> Dict:
    return _read_json(
        data_dir / "sync_metadata_sessions.json",
        default={"last_sync": None, "processed_session_ids": []},
    )


def save_sessions_sync_metadata(data_dir: Path, metadata: Dict) -> None:
    _write_json(data_dir / "sync_metadata_sessions.json", metadata)


def merge_session_data(new_sessions: List[Dict], existing_sessions: List[Dict]) -> List[Dict]:
    # Normalize existing sessions to have url field if missing
    normalized_existing = []
    for s in existing_sessions:
        if 'url' not in s and 'detail_url' in s:
            s = dict(s)  # make a copy
            s['url'] = s['detail_url']
        normalized_existing.append(s)
    
    id_to_existing: Dict[str, Dict] = {s.get("session_id") or s.get("url"): s for s in normalized_existing}
    merged: List[Dict] = []
    for session in new_sessions:
        key = session.get("session_id") or session.get("detail_url") or session.get("url")
        if key in id_to_existing:
            existing = id_to_existing[key]
            # preserve human-edited fields
            session["speakers"] = existing.get("speakers", session.get("speakers", []))
            session["categories"] = existing.get("categories", session.get("categories", []))
        merged.append(session)
    return merged


def compute_session_speaker_stats(sessions: List[Dict]) -> Dict:
    speaker_stats: Dict[str, Dict[str, Any]] = {}
    for session in sessions:
        for speaker in session.get("speakers", []):
            if speaker not in speaker_stats:
                speaker_stats[speaker] = {"name": speaker, "appearances": 0, "sessions": []}
            speaker_stats[speaker]["appearances"] += 1
            speaker_stats[speaker]["sessions"].append(
                {"title": session.get("title"), "url": session.get("detail_url") or session.get("url")}
            )

    speakers_list = list(speaker_stats.values())
    speakers_list.sort(key=lambda x: x["appearances"], reverse=True)
    return {
        "speakers": speakers_list,
        "total_speakers": len(speakers_list),
    }


def save_session_speaker_stats(data_dir: Path, stats: Dict) -> None:
    _write_json(data_dir / "speaker_stats_sessions.json", stats)
