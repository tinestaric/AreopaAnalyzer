from typing import Dict, List, Optional

from .loader import flatten_names


def filter_categories(categories: List[str], taxonomy: Optional[Dict]) -> List[str]:
    if not taxonomy:
        return categories
    flat = flatten_names(taxonomy)
    allowed = set([*flat.keys(), *sum(flat.values(), [])])
    return [c for c in categories if c in allowed]
