import os
import json
from pathlib import Path
import logging
from typing import List, Dict

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(project_root)

from src.utils.markmap_generator import MarkmapGenerator

def setup_logging():
    """Configure logging for both console and file output"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Output to console
            logging.FileHandler('markmap_generation.log')  # Output to file
        ]
    )

def load_videos(data_dir: Path) -> List[dict]:
    """Load videos from the JSON file"""
    with open(data_dir / 'videos.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting markmap generation...")
    
    # Setup paths
    data_dir = Path(project_root) / 'data'
    category_mapping_path = data_dir / 'category_mapping.json'
    
    try:
        # Load videos
        logger.info("Loading videos from videos.json...")
        videos = load_videos(data_dir)
        logger.info(f"Loaded {len(videos)} videos")
        
        # Generate markmap
        logger.info("Generating markmap...")
        markmap_generator = MarkmapGenerator(category_mapping_path)
        markmap_content = markmap_generator.generate_markmap(videos)
        
        # Save markmap
        output_file = 'videos_markmap.md'
        markmap_generator.save_markmap(markmap_content, output_file, data_dir)
        logger.info(f"Markmap saved to {output_file}")
        
    except FileNotFoundError as e:
        logger.error(f"Required file not found: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 