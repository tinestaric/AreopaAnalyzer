from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Optional

from .config import load_config
from .export.markmap import generate_markmap, save_markmap
from .storage.repo import (
    compute_speaker_stats,
    ensure_data_dir,
    load_sync_metadata,
    load_videos,
    merge_video_data,
    save_speaker_stats,
    save_sync_metadata,
    save_videos,
)
from .taxonomy.loader import load_taxonomy
from .taxonomy.match import filter_categories
from .youtube.analyzer import analyze_channel
from .youtube.client import YouTubeClient
from .tagging.azure_openai import AzureOpenAITaggingProvider
from .sessions.analyzer import analyze_sessions
from .storage.repo import (
    load_sessions,
    save_sessions,
    compute_session_speaker_stats,
)


def cmd_analyze(args: argparse.Namespace) -> None:
    cfg = load_config()
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))

    taxonomy_path = Path(args.taxonomy) if args.taxonomy else None
    taxonomy = load_taxonomy(taxonomy_path)

    if getattr(args, "session_list_url", None):
        entries = analyze_sessions(
            session_list_url=args.session_list_url,
            data_dir=data_dir,
            batch_size=args.batch_size or cfg.batch_size,
            taxonomy_path=taxonomy_path,
            restrict_to_taxonomy=bool(args.restrict_to_taxonomy),
            skip_ai=bool(getattr(args, "skip_ai", False)),
            request_timeout=int(getattr(args, "request_timeout", 60)),
            respect_robots=not bool(getattr(args, "no_robots", False)),
            delay_min=float(getattr(args, "delay_min", 0.3)),
            delay_max=float(getattr(args, "delay_max", 0.8)),
            limit=args.limit,
        )
        print(f"Processed {len(entries)} sessions.")
        if getattr(args, "export_markmap", False):
            content = generate_markmap(
                entries,
                taxonomy=taxonomy,
                root_title=args.root_title or "Conference Sessions",
                root_url=args.root_url or args.session_list_url,
                exclude_session_types=[] if getattr(args, 'include_all_session_types', False) else None,
            )
            out_path = Path(args.output) if args.output else (data_dir / "sessions_markmap.md")
            save_markmap(content, out_path)
            print(f"Saved markmap to {out_path}")
        return

    # YouTube path (default)
    sync = load_sync_metadata(data_dir)
    existing = load_videos(data_dir)

    tagger = AzureOpenAITaggingProvider(
        batch_size=args.batch_size or cfg.batch_size,
        taxonomy_path=taxonomy_path,
        restrict_to_taxonomy=bool(args.restrict_to_taxonomy),
    )
    yt = YouTubeClient(cfg.youtube_api_key)

    entries = analyze_channel(
        yt=yt,
        channel_id=args.channel_id,
        processed_ids=sync.get("processed_video_ids", []),
        tagger=tagger,
        limit=args.limit,
    )

    # optionally filter categories if taxonomy provided and restriction requested
    if args.restrict_to_taxonomy and taxonomy:
        for e in entries:
            e["categories"] = filter_categories(e.get("categories", []), taxonomy)

    merged = merge_video_data(entries, existing)
    save_videos(data_dir, merged)

    sync["processed_video_ids"] = [e["url"].split("v=")[-1] for e in merged]
    save_sync_metadata(data_dir, sync)

    stats = compute_speaker_stats(merged)
    save_speaker_stats(data_dir, stats)

    print(f"Processed {len(merged)} videos. Speakers: {stats['total_speakers']}")

    # Optionally export markmap in the same run
    if getattr(args, "export_markmap", False):
        content = generate_markmap(
            merged,
            taxonomy=taxonomy,
            root_title=args.root_title or "YouTube Channel",
            root_url=args.root_url,
            exclude_session_types=[] if getattr(args, 'include_all_session_types', False) else None,
        )
        out_path = Path(args.output) if args.output else (data_dir / "videos_markmap.md")
        save_markmap(content, out_path)
        print(f"Saved markmap to {out_path}")


def cmd_export_markmap(args: argparse.Namespace) -> None:
    cfg = load_config()
    
    # Data directory contains the source data files
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))
    
    # Output directory for the markmap file (defaults to data_dir if not specified)
    output_dir = Path(args.output_dir) if getattr(args, 'output_dir', None) else data_dir
    
    # Check if --sessions flag is used or if sessions.json exists and is newer/larger than videos.json
    use_sessions = getattr(args, 'sessions', False)
    if not use_sessions:
        sessions_file = data_dir / "sessions.json"
        videos_file = data_dir / "videos.json"
        if sessions_file.exists() and (not videos_file.exists() or sessions_file.stat().st_size > videos_file.stat().st_size):
            print("Auto-detecting sessions.json as data source (larger/newer than videos.json)")
            use_sessions = True
    
    if use_sessions:
        data = load_sessions(data_dir)
        default_title = "Conference Sessions"
        default_output = output_dir / "sessions_markmap.md"
    else:
        data = load_videos(data_dir)
        default_title = "YouTube Channel"
        default_output = output_dir / "videos_markmap.md"
    
    taxonomy = load_taxonomy(Path(args.taxonomy)) if args.taxonomy else None
    
    # Derive filename from taxonomy if not explicitly provided
    if args.output:
        out = Path(args.output)
    elif args.taxonomy and taxonomy:
        taxonomy_path = Path(args.taxonomy)
        # Extract name from taxonomy file (remove extension and path)
        taxonomy_name = taxonomy_path.stem
        if taxonomy_name != "taxonomy":  # Don't use generic "taxonomy" name
            # Use taxonomy name for filename
            if use_sessions:
                filename = f"{taxonomy_name}_markmap.md"
            else:
                filename = f"{taxonomy_name}_markmap.md"
            out = output_dir / filename
        else:
            out = default_output
    else:
        out = default_output
    
    content = generate_markmap(
        data,
        taxonomy=taxonomy,
        root_title=args.root_title or default_title,
        root_url=args.root_url,
        exclude_session_types=[] if getattr(args, 'include_all_session_types', False) else None,
    )
    
    # Create output directory if it doesn't exist
    out.parent.mkdir(parents=True, exist_ok=True)
    
    save_markmap(content, out)
    print(f"Saved markmap to {out} (source: {'sessions.json' if use_sessions else 'videos.json'} from {data_dir})")


def cmd_stats(args: argparse.Namespace) -> None:
    cfg = load_config()
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))
    
    # Use sessions if --sessions flag is provided, otherwise use videos
    use_sessions = getattr(args, 'sessions', False)
    
    if use_sessions:
        sessions = load_sessions(data_dir)
        _print_session_stats(sessions)
    else:
        videos = load_videos(data_dir)
        _print_video_stats(videos)


def _print_video_stats(videos: List[Dict]) -> None:
    """Print statistics for YouTube videos data."""
    from .storage.repo import compute_speaker_stats
    
    stats = compute_speaker_stats(videos)
    print(f"Total videos: {len(videos)}")
    print(f"Total speakers: {stats['total_speakers']}")
    print(f"\nTop 10 speakers by video count:")
    for s in stats["speakers"][:10]:
        print(f"  {s['name']}: {s['appearances']} videos")


def _print_session_stats(sessions: List[Dict]) -> None:
    """Print comprehensive statistics for conference sessions data."""
    from collections import Counter
    from .storage.repo import compute_session_speaker_stats
    
    print(f"Total sessions: {len(sessions)}")
    
    # Speaker stats
    speaker_stats = compute_session_speaker_stats(sessions)
    print(f"Total speakers: {speaker_stats['total_speakers']}")
    print(f"\nTop 10 speakers by session count:")
    for s in speaker_stats["speakers"][:10]:
        print(f"  {s['name']}: {s['appearances']} sessions")
    
    # Session type distribution
    session_types = Counter(s.get("session_type") for s in sessions if s.get("session_type"))
    if session_types:
        print(f"\nSession types:")
        for stype, count in session_types.most_common():
            print(f"  {stype}: {count} sessions")
    
    # Level distribution
    levels = Counter(s.get("level") for s in sessions if s.get("level"))
    if levels:
        print(f"\nSession levels:")
        for level, count in levels.most_common():
            print(f"  {level}: {count} sessions")
    
    # Role distribution (flattened from roles arrays)
    all_roles = []
    for s in sessions:
        if s.get("roles"):
            all_roles.extend(s["roles"])
    role_counts = Counter(all_roles)
    if role_counts:
        print(f"\nTarget roles:")
        for role, count in role_counts.most_common():
            print(f"  {role}: {count} sessions")
    
    # Product distribution
    products = Counter(s.get("product") for s in sessions if s.get("product"))
    if products:
        print(f"\nProducts:")
        for product, count in products.most_common():
            print(f"  {product}: {count} sessions")
    
    # Areas distribution (flattened from areas arrays)
    all_areas = []
    for s in sessions:
        if s.get("areas"):
            all_areas.extend(s["areas"])
    area_counts = Counter(all_areas)
    if area_counts:
        print(f"\nTechnical areas:")
        for area, count in area_counts.most_common():
            print(f"  {area}: {count} sessions")
    
    # Submitter distribution
    submitters = Counter(s.get("submitter") for s in sessions if s.get("submitter"))
    if submitters:
        print(f"\nSubmitters:")
        for submitter, count in submitters.most_common():
            print(f"  {submitter}: {count} sessions")
    
    # Multi-speaker sessions
    multi_speaker = [s for s in sessions if len(s.get("speakers", [])) > 1]
    print(f"\nMulti-speaker sessions: {len(multi_speaker)} ({len(multi_speaker)/len(sessions)*100:.1f}%)")
    
    # Category diversity (sessions with most categories)
    sessions_with_categories = [s for s in sessions if s.get("categories")]
    if sessions_with_categories:
        avg_categories = sum(len(s["categories"]) for s in sessions_with_categories) / len(sessions_with_categories)
        max_categories = max(len(s["categories"]) for s in sessions_with_categories)
        print(f"\nCategory diversity:")
        print(f"  Average categories per session: {avg_categories:.1f}")
        print(f"  Maximum categories in a session: {max_categories}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="yt-tool")
    sub = p.add_subparsers(dest="command", required=True)

    a = sub.add_parser("analyze", help="Fetch items, tag new ones, update data")
    src = a.add_mutually_exclusive_group(required=True)
    src.add_argument("--channel-id", help="YouTube channel id")
    src.add_argument("--session-list-url", help="Conference sessions list URL")
    a.add_argument("--data-dir")
    a.add_argument("--taxonomy")
    a.add_argument("--restrict-to-taxonomy", action="store_true")
    a.add_argument("--limit", type=int)
    a.add_argument("--batch-size", type=int)
    a.add_argument("--skip-ai", action="store_true", help="Skip AI tagging (sessions)")
    a.add_argument("--request-timeout", type=int, default=60, help="HTTP request timeout seconds (sessions)")
    a.add_argument("--no-robots", action="store_true", help="Ignore robots.txt (sessions)")
    a.add_argument("--delay-min", type=float, default=0.3, help="Min polite delay between requests (sessions)")
    a.add_argument("--delay-max", type=float, default=0.8, help="Max polite delay between requests (sessions)")
    a.add_argument("--export-markmap", action="store_true", help="Also export markmap after analyze")
    a.add_argument("--root-title", help="Markmap root title when exporting")
    a.add_argument("--root-url", help="Markmap root URL when exporting")
    a.add_argument("--output", help="Markmap output path when exporting")
    a.add_argument("--include-all-session-types", action="store_true", help="Include keynote, sponsor, and ISV sessions in markmap")
    a.set_defaults(func=cmd_analyze)

    e = sub.add_parser("export", help="Export assets")
    esub = e.add_subparsers(dest="export_command", required=True)
    mm = esub.add_parser("markmap", help="Export markmap markdown")
    mm.add_argument("--data-dir", help="Source directory containing sessions.json/videos.json")
    mm.add_argument("--output-dir", help="Output directory for markmap file (defaults to --data-dir)")
    mm.add_argument("--taxonomy")
    mm.add_argument("--root-title")
    mm.add_argument("--root-url")
    mm.add_argument("--output")
    mm.add_argument("--sessions", action="store_true", help="Use sessions.json instead of videos.json")
    mm.add_argument("--include-all-session-types", action="store_true", help="Include keynote, sponsor, and ISV sessions in markmap")
    mm.set_defaults(func=cmd_export_markmap)

    s = sub.add_parser("stats", help="Show simple statistics")
    s.add_argument("--data-dir")
    s.add_argument("--sessions", action="store_true", help="Use sessions.json instead of videos.json")
    s.set_defaults(func=cmd_stats)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

