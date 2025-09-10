from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

def generate_markmap(
    videos: List[Dict],
    taxonomy: Dict,
    root_title: str = "YouTube Channel",
    root_url: str | None = None,
    exclude_session_types: Optional[List[str]] = None,
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

    # Filter session types based on taxonomy metadata or parameter
    metadata = taxonomy.get("_metadata", {})
    
    # Check for include_session_types first (whitelist), then exclude_session_types (blacklist)
    if "include_session_types" in metadata:
        include_session_types = metadata["include_session_types"]
        filtered_videos = []
        for video in videos:
            session_type = video.get("session_type")
            if session_type in include_session_types:
                filtered_videos.append(video)
        print(f"Included {len(filtered_videos)} sessions with types: {include_session_types}")
    else:
        # Use exclude list (backwards compatibility)
        if "exclude_session_types" in metadata:
            exclude_session_types = metadata["exclude_session_types"]
        elif exclude_session_types is None:
            exclude_session_types = ["Keynote", "Sponsor (45 min)", "ISV Theatre (15 min)"]
        
        filtered_videos = []
        for video in videos:
            session_type = video.get("session_type")
            if session_type not in exclude_session_types:
                filtered_videos.append(video)
        
        print(f"Filtered out {len(videos) - len(filtered_videos)} sessions with excluded types: {exclude_session_types}")

    # Extract structure from metadata (required)
    if "_metadata" not in taxonomy or "structure" not in taxonomy["_metadata"]:
        raise ValueError("Taxonomy must contain _metadata.structure field")
    
    structure = taxonomy["_metadata"]["structure"]
    # Remove metadata from taxonomy data for processing
    taxonomy_data = {k: v for k, v in taxonomy.items() if k != "_metadata"}
    
    _generate_taxonomy_tree(lines, taxonomy_data, filtered_videos, level=2, path=[], structure=structure)

    return "\n".join(lines)


def _generate_taxonomy_tree(lines: List[str], node: Dict, videos: List[Dict], level: int = 2, path: List[str] = None, structure: str = None) -> None:
    """Recursively generate taxonomy tree structure for markmap."""
    if level > 6:  # Prevent infinite recursion, markdown only supports h1-h6
        return
    
    if path is None:
        path = []
    
    prefix = "#" * level
    
    for key, value in node.items():
        current_path = path + [key]
        
        # Check if this branch has any videos before adding it
        if not _has_videos_in_subtree_with_path(key, value, videos, current_path, structure):
            continue
            
        lines.append(f"{prefix} {key}")
        
        # Recursively handle nested structure
        if isinstance(value, dict):
            _generate_taxonomy_tree(lines, value, videos, level + 1, current_path, structure)
        elif isinstance(value, list):
            # Handle leaf nodes (list of final categories) - videos go here
            for item in value:
                item_path = current_path + [item]
                item_videos = _get_videos_for_category(videos, item, item_path, structure)
                if item_videos:  # Only add if there are videos
                    lines.append(f"{prefix}# {item}")
                    for j, v in enumerate(item_videos, 1):
                        title = (v.get("title") or "").replace("[", "").replace("]", "")
                        url = v.get("url")
                        lines.append(f"{prefix}## {j}. [{title}]({url})")
        else:
            # Handle case where value is neither dict nor list (leaf node with direct videos)
            matching_videos = _get_videos_for_category(videos, key, current_path, structure)
            if matching_videos:
                for i, v in enumerate(matching_videos, 1):
                    title = (v.get("title") or "").replace("[", "").replace("]", "")
                    url = v.get("url")
                    lines.append(f"{prefix}# {i}. [{title}]({url})")


def _get_videos_for_category(videos: List[Dict], target_category: str, taxonomy_path: List[str], structure: str) -> List[Dict]:
    """Get videos that match the full taxonomy path using structured fields."""
    
    matching_videos = []
    
    for video in videos:
        # Check if target category exists in this video
        if not _video_has_category(video, target_category):
            continue
            
        # Match against taxonomy path based on structure
        if _video_matches_taxonomy_path(video, taxonomy_path, structure):
            matching_videos.append(video)
    
    return matching_videos


def _video_has_category(video: Dict, target_category: str) -> bool:
    """Check if video has the target category in any of its structured fields."""
    return (
        target_category == video.get("level") or
        target_category == video.get("product") or
        target_category in video.get("roles", []) or
        target_category in video.get("areas", []) or
        target_category == video.get("submitter") or
        target_category == video.get("session_type")
    )


def _video_matches_taxonomy_path(video: Dict, taxonomy_path: List[str], structure: str) -> bool:
    """Check if video matches all components in the taxonomy path."""
    # Map taxonomy path to expected components
    component_map = _map_taxonomy_path(taxonomy_path, structure)
    
    # Get video's structured field values directly
    video_level = video.get("level")
    video_product = video.get("product")
    video_roles = video.get("roles", [])
    video_areas = video.get("areas", [])
    video_submitter = video.get("submitter")
    
    # Check each expected component
    for component, expected_value in component_map.items():
        if expected_value is None:
            continue
            
        if component == "level" and video_level != expected_value:
            return False
        elif component == "product" and video_product != expected_value:
            return False
        elif component == "role" and not _matches_role(expected_value, video_roles):
            return False
        elif component == "area" and not _matches_area(video_product, expected_value, video_areas):
            return False
        elif component == "submitter" and video_submitter != expected_value:
            return False
    
    return True


def _parse_structure(structure: str) -> Dict[str, int]:
    """Parse structure string into component position mapping."""
    # Parse structure like "level-product-role-area" into position mapping
    components = structure.split("-")
    position_map = {}
    for index, component in enumerate(components):
        position_map[component] = index
    
    return position_map


def _map_taxonomy_path(taxonomy_path: List[str], structure: str) -> Dict[str, str]:
    """Map taxonomy path to component values based on structure."""
    position_map = _parse_structure(structure)
    
    # Build mapping based on parsed structure
    component_map = {}
    for component, position in position_map.items():
        if position < len(taxonomy_path):
            component_map[component] = taxonomy_path[position]
        else:
            component_map[component] = None
    
    return component_map


def _matches_area(product: str, expected_area: str, video_areas: List[str]) -> bool:
    """Check if video matches the area requirement."""
    # For "All" product, we no longer have area levels, so area matching is always True
    if product == "All":
        return True
    else:
        # Check if expected area is in the list of video areas
        return expected_area in video_areas or (expected_area == "General" and not video_areas)


def _matches_role(expected_role: str, video_roles: List[str]) -> bool:
    """Check if video matches the role requirement."""
    if expected_role == "All":
        # Match if video has "All" role OR if we're matching against any role
        return True  # "All" role should match any video
    else:
        return expected_role in video_roles


def _has_videos_in_subtree_with_path(key: str, value, videos: List[Dict], current_path: List[str], structure: str) -> bool:
    """Check if this node or any of its descendants has videos using the new category matching logic."""
    
    # For leaf nodes (lists), check if any item has videos
    if isinstance(value, list):
        for item in value:
            item_path = current_path + [item]
            if _get_videos_for_category(videos, item, item_path, structure):
                return True
        return False
    
    # For intermediate nodes (dicts), check descendants
    elif isinstance(value, dict):
        for sub_key, sub_value in value.items():
            sub_path = current_path + [sub_key]
            if _has_videos_in_subtree_with_path(sub_key, sub_value, videos, sub_path, structure):
                return True
        return False
    
    # For direct leaf nodes, check if this key has videos
    else:
        return bool(_get_videos_for_category(videos, key, current_path, structure))


def save_markmap(content: str, output_path: Path) -> None:
    output_path.write_text(content, encoding="utf-8")

