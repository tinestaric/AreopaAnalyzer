from typing import Dict, List, Optional


def filter_categories(categories: List[str], taxonomy: Optional[Dict]) -> List[str]:
    if not taxonomy:
        return categories
    allowed = set([*taxonomy.keys(), *sum([v for v in taxonomy.values() if isinstance(v, list)], [])])
    return [c for c in categories if c in allowed]

