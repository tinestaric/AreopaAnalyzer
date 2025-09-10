from typing import Dict, List, Protocol


class TaggingProvider(Protocol):
    batch_size: int

    def analyze_content_batch(self, descriptions: List[Dict[str, str]]) -> List[Dict[str, object]]:
        ...

