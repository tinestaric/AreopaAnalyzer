from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

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
        )
        out_path = Path(args.output) if args.output else (data_dir / "videos_markmap.md")
        save_markmap(content, out_path)
        print(f"Saved markmap to {out_path}")


def cmd_export_markmap(args: argparse.Namespace) -> None:
    cfg = load_config()
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))
    videos = load_videos(data_dir)
    taxonomy = load_taxonomy(Path(args.taxonomy)) if args.taxonomy else None
    content = generate_markmap(
        videos,
        taxonomy=taxonomy,
        root_title=args.root_title or "YouTube Channel",
        root_url=args.root_url,
    )
    out = Path(args.output or (data_dir / "videos_markmap.md"))
    save_markmap(content, out)
    print(f"Saved markmap to {out}")


def cmd_stats(args: argparse.Namespace) -> None:
    cfg = load_config()
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))
    videos = load_videos(data_dir)
    stats = compute_speaker_stats(videos)
    print(f"Total videos: {len(videos)}")
    print(f"Total speakers: {stats['total_speakers']}")
    for s in stats["speakers"][:10]:
        print(f"{s['name']}: {s['appearances']} videos")


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
    a.set_defaults(func=cmd_analyze)

    e = sub.add_parser("export", help="Export assets")
    esub = e.add_subparsers(dest="export_command", required=True)
    mm = esub.add_parser("markmap", help="Export markmap markdown")
    mm.add_argument("--data-dir")
    mm.add_argument("--taxonomy")
    mm.add_argument("--root-title")
    mm.add_argument("--root-url")
    mm.add_argument("--output")
    mm.set_defaults(func=cmd_export_markmap)

    s = sub.add_parser("stats", help="Show simple statistics")
    s.add_argument("--data-dir")
    s.set_defaults(func=cmd_stats)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

