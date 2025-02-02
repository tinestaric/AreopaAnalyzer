import os
import sys
import logging
import json
import datetime
import time
from pathlib import Path

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from src.youtube.analyzer import YouTubeAnalyzer
from src.utils.config import API_KEY
from src.utils.markmap_generator import MarkmapGenerator

def setup_logging():
    """Configure logging for both console and file output"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Output to console
            logging.FileHandler('youtube_analysis.log')  # Output to file
        ]
    )

def save_json_data(data: dict, filename: str, data_dir: Path):
    """Save data to a JSON file with proper encoding"""
    with open(data_dir / filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def prepare_video_data(videos: list) -> list:
    """Prepare video data for JSON storage"""
    return [{
        'title': video['title'],
        'views': video['views'],
        'url': video['url'],
        'speakers': video['speakers'],
        'categories': video.get('categories', [])
    } for video in videos]

def prepare_speaker_stats(videos: list) -> dict:
    """Prepare speaker statistics"""
    speaker_stats = {}
    for video in videos:
        for speaker in video['speakers']:
            if speaker not in speaker_stats:
                speaker_stats[speaker] = {
                    'name': speaker,
                    'appearances': 0,
                    'videos': []
                }
            speaker_stats[speaker]['appearances'] += 1
            speaker_stats[speaker]['videos'].append({
                'title': video['title'],
                'url': video['url'],
                'views': video['views']
            })
    
    speakers_list = list(speaker_stats.values())
    speakers_list.sort(key=lambda x: x['appearances'], reverse=True)
    
    return {
        'speakers': speakers_list,
        'total_speakers': len(speakers_list),
        'last_updated': datetime.datetime.now().isoformat()
    }

def display_results(videos: list, speaker_stats: dict):
    """Display analysis results to console"""
    print(f"\nTotal videos processed: {len(videos)}")
    print("\nTop 10 Most Viewed Videos:")
    for i, video in enumerate(videos[:10], 1):
        print(f"\n{i}. {video['title']}")
        print(f"Views: {video['views']:,}")
        print(f"URL: {video['url']}")
        if video['speakers']:
            print(f"Speakers: {', '.join(video['speakers'])}")
        if video.get('categories'):
            print(f"Categories: {', '.join(video['categories'])}")

    print("\nTop 10 Speakers by Appearances:")
    for speaker in speaker_stats['speakers'][:10]:
        print(f"{speaker['name']}: {speaker['appearances']} videos")

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting YouTube analysis...")
    start_time = time.time()
    
    analyzer = YouTubeAnalyzer(API_KEY, batch_size=5)
    channel_id = "UCWL0RbbT6ILzzCd6Ix7t0AQ"
    
    if channel_id:
        logger.info(f"Analyzing channel: {channel_id}")
        videos = analyzer.analyze_channel_videos(channel_id)
        
        # Create data directory if it doesn't exist
        data_dir = Path(project_root) / 'data'
        data_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare and save all videos
        video_data = prepare_video_data(videos)
        speaker_stats = prepare_speaker_stats(videos)
        
        # Generate and save markmap data
        markmap_generator = MarkmapGenerator()
        markmap_content = markmap_generator.generate_markmap(video_data)
        markmap_generator.save_markmap(markmap_content, 'videos_markmap.md', data_dir)
        
        save_json_data(video_data, 'videos.json', data_dir)
        save_json_data(speaker_stats, 'speaker_stats.json', data_dir)
        
        # Display results (keep top 10 for display purposes only)
        display_results(videos, speaker_stats)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Analysis completed in {elapsed_time:.2f} seconds")
        logger.info(f"Total videos processed: {len(videos)}")
        logger.info(f"Results saved to videos.json, speaker_stats.json, and videos_markmap.md")

if __name__ == "__main__":
    main() 