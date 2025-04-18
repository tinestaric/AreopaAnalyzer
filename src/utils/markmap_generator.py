from pathlib import Path
from typing import List, Dict
import json

class MarkmapGenerator:
    def __init__(self, category_mapping_path: Path):
        # Load categories and their relationships from JSON file
        with open(category_mapping_path, 'r') as f:
            self.hierarchy = json.load(f)

    def _add_videos_to_category(self, videos: List[dict], category: str, level: int = 1, parent_category: str = None, number: str = "") -> List[str]:
        """Helper function to add videos under their categories"""
        lines = []
        # Add the category itself
        category_prefix = f"{number}. " if number else ""
        lines.append("#" * level + f" {category_prefix}{category}")
        
        # Add videos for this category
        if (category in ["General", "Other"]) and parent_category:
            # For "General" or "Other" under parent categories, only include videos that have parent tag but no other subcategory tags
            subcategories = set(self.hierarchy[parent_category]) - {"General", "Other"}
            category_videos = [
                v for v in videos 
                if parent_category in v.get('categories', []) and 
                not any(subcat in v.get('categories', []) for subcat in subcategories)
            ]
        elif category in self.hierarchy and isinstance(self.hierarchy[category], list) and any(sub in ["General", "Other"] for sub in self.hierarchy[category]):
            # For main categories that have a General or Other subcategory, don't show videos here
            category_videos = []
        else:
            category_videos = [v for v in videos if category in v.get('categories', [])]
            
        for i, video in enumerate(category_videos, 1):
            title = video['title'].replace('[', '').replace(']', '')  # Clean title
            url = video['url']
            video_number = f"{number}.{i}" if number else f"{i}"
            lines.append("#" * (level + 1) + f" {video_number}. [{title}]({url})")
        
        return lines

    def generate_markmap(self, videos: List[dict]) -> str:
        """Generate markmap-friendly markdown content"""
        # Start with markmap options
        content = [
            "---",
            "markmap:",
            "  initialExpandLevel: 2",
            "  colorFreezeLevel: 3",
            "  duration: 1000",
            "  spacingVertical: 10",
            "---\n"
        ]
        
        # Add the root topic with link to areopa.academy
        content.append("# [Areopa Webinars](https://areopa.academy/)\n")
        
        # Generate content for each top-level category
        for i, (category, subcategories) in enumerate(self.hierarchy.items(), 1):
            content.extend(self._add_videos_to_category(videos, category, level=2, number=str(i)))
            
            # Add subcategories if they exist and are in list format
            if isinstance(subcategories, list):
                for j, subcategory in enumerate(subcategories, 1):
                    subcategory_number = f"{i}.{j}"
                    content.extend(self._add_videos_to_category(
                        videos, 
                        subcategory, 
                        level=3, 
                        parent_category=category,
                        number=subcategory_number
                    ))
        
        return "\n".join(content)

    def save_markmap(self, content: str, filename: str, data_dir: Path):
        """Save markmap content to a markdown file"""
        with open(data_dir / filename, 'w', encoding='utf-8') as f:
            f.write(content) 