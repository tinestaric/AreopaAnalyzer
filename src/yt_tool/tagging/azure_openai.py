from __future__ import annotations

import json
import os
from pathlib import Path
from time import sleep
from typing import Dict, List, Set

from openai import AzureOpenAI


class AzureOpenAITaggingProvider:
    def __init__(
        self,
        batch_size: int = 5,
        taxonomy_path: Path | None = None,
        restrict_to_taxonomy: bool = False,
    ) -> None:
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-05-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.batch_size = batch_size
        self.request_delay = 2
        self.max_retries = 3
        self.retry_delay = 30
        self.restrict_to_taxonomy = restrict_to_taxonomy

        self.known_categories: Set[str] = set()
        if taxonomy_path and taxonomy_path.exists():
            self.known_categories = self._load_categories(taxonomy_path)

    def _load_categories(self, path: Path) -> Set[str]:
        mapping = json.loads(Path(path).read_text(encoding="utf-8"))
        categories: Set[str] = set()
        for category, subcategories in mapping.items():
            categories.add(category)
            if isinstance(subcategories, list):
                categories.update(sub for sub in subcategories if sub not in ["General", "Other"])
        return categories

    def _format_descriptions(self, descriptions: List[Dict[str, str]]) -> str:
        return "\n\n".join(
            f"{i+1}. Title: {desc['title']}\nDescription: {desc['description']}" for i, desc in enumerate(descriptions)
        )

    def _create_prompt(self) -> str:
        taxonomy_hint = (
            f"ONLY use categories from this fixed list: {sorted(list(self.known_categories))}"
            if self.known_categories
            else "Suggest concise, relevant categories"
        )
        return f"""
For each numbered YouTube video below:
1. Identify the main speakers or participants (exclude moderators/hosts)
2. Assign 1-2 relevant categories. {taxonomy_hint}.

Return JSON with numbered keys matching the videos, e.g.:
{{
  "1": {{ "speakers": ["name"], "categories": ["category"] }},
  "2": {{ "speakers": ["name1","name2"], "categories": ["category"] }}
}}

Videos:
{{descriptions}}
"""

    def analyze_content_batch(self, descriptions: List[Dict[str, str]]) -> List[Dict[str, object]]:
        formatted = self._format_descriptions(descriptions)
        for attempt in range(self.max_retries):
            try:
                sleep(self.request_delay)
                response = self.client.chat.completions.create(
                    model=self.deployment_name,
                    messages=[{"role": "user", "content": self._create_prompt().format(descriptions=formatted)}],
                    temperature=0.3,
                    max_tokens=1000,
                    response_format={"type": "json_object"},
                )
                parsed = json.loads(response.choices[0].message.content)

                numbered: List[Dict[str, object]] = []
                for i in range(1, len(descriptions) + 1):
                    item = parsed.get(str(i), {"speakers": [], "categories": []})
                    cats = item.get("categories", []) or []
                    if self.restrict_to_taxonomy and self.known_categories:
                        cats = [c for c in cats if c in self.known_categories]
                    item["categories"] = cats
                    numbered.append(item)

                return [
                    {
                        "video_id": descriptions[i]["video_id"],
                        "speakers": numbered[i].get("speakers", []),
                        "categories": numbered[i].get("categories", []),
                    }
                    for i in range(len(descriptions))
                ]
            except Exception as e:
                if "429" in str(e) and attempt < self.max_retries - 1:
                    sleep(self.retry_delay * (attempt + 1))
                    continue
                return [
                    {"video_id": d["video_id"], "speakers": [], "categories": []} for d in descriptions
                ]

