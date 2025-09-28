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


def cmd_analyze(args: argparse.Namespace) -> None:
    cfg = load_config()
    data_dir = ensure_data_dir(Path(args.data_dir or cfg.data_dir))

    sync = load_sync_metadata(data_dir)
    existing = load_videos(data_dir)

    taxonomy_path = Path(args.taxonomy) if args.taxonomy else None
    taxonomy = load_taxonomy(taxonomy_path)

    tagger = AzureOpenAITaggingProvider(
        batch_size=args.batch_size or cfg.batch_size,
        taxonomy_path=taxonomy_path,
        restrict_to_taxonomy=bool(args.restrict_to_taxonomy),
    )
    yt = YouTubeClient(cfg.youtube_api_key)

    print(f"Starting analysis for channel: {args.channel_id}")
    processed_before = len(sync.get("processed_video_ids", []))
    print(f"Previously processed videos: {processed_before}")
    
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

    # Show details of newly processed videos
    new_videos = []
    existing_video_urls = {v["url"] for v in existing}
    for entry in entries:
        if entry["url"] not in existing_video_urls:
            new_videos.append(entry)
    
    if new_videos:
        print(f"\n=== NEWLY PROCESSED VIDEOS ({len(new_videos)}) ===")
        for video in new_videos:
            speakers_str = ", ".join(video.get("speakers", [])) or "No speakers identified"
            categories_str = ", ".join(video.get("categories", [])) or "No categories assigned"
            print(f"\n{video['title']}")
            print(f"   Speakers: {speakers_str}")
            print(f"   Categories: {categories_str}")
            print(f"   Views: {video.get('views', 0):,}")
            print(f"   URL: {video['url']}")
    else:
        print("\n=== NO NEW VIDEOS TO PROCESS ===")

    merged = merge_video_data(entries, existing)
    save_videos(data_dir, merged)

    sync["processed_video_ids"] = [e["url"].split("v=")[-1] for e in merged]
    save_sync_metadata(data_dir, sync)

    stats = compute_speaker_stats(merged)
    save_speaker_stats(data_dir, stats)

    print(f"\n=== SUMMARY ===")
    print(f"Total videos in database: {len(merged)}")
    print(f"Videos processed this run: {len(new_videos)}")
    print(f"Total unique speakers: {stats['total_speakers']}")
    print(f"Previously processed: {processed_before} → Now processed: {len(merged)}")

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

    a = sub.add_parser("analyze", help="Fetch videos, tag new ones, update data")
    a.add_argument("--channel-id", required=True)
    a.add_argument("--data-dir")
    a.add_argument("--taxonomy")
    a.add_argument("--restrict-to-taxonomy", action="store_true")
    a.add_argument("--limit", type=int)
    a.add_argument("--batch-size", type=int)
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

