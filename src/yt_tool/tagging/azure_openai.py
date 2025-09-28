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
        self.batch_size = min(batch_size, 5)  # Enforce max batch size of 5
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
        return """For each numbered YouTube video below:
        1. Identify the main speakers or participants (excluding moderators, hosts, or interviewers)
        2. Assign 1 relevant categories ONLY from this fixed list:
        {known_categories}

        DO NOT create new categories - only use categories from the list above.
        If no category fits, use "Other".

        Return the results as a JSON object with numbered keys matching the videos:
        {{
            "1": {{
                "speakers": ["name1", "name2"],
                "categories": ["category1", "category2"]
            }},
            "2": {{
                "speakers": ["name3"],
                "categories": ["category3"]
            }}
        }}

        Example:
        Title 1: "Interview about Quantum Physics"
        Description 1: "In this episode, host John Smith interviews Dr. Jane Doe about quantum physics"
        
        Title 2: "Climate Change Discussion"
        Description 2: "Moderator Alice Brown leads a discussion with experts Bob Wilson and Carol Davis about climate change"

        Result:
        {{
            "1": {{
                "speakers": ["Jane Doe"],
                "categories": ["Science"]
            }},
            "2": {{
                "speakers": ["Bob Wilson", "Carol Davis"],
                "categories": ["Environment"]
            }}
        }}

        Here are the videos to analyze:
        {descriptions}"""

    def analyze_content_batch(self, descriptions: List[Dict[str, str]]) -> List[Dict[str, object]]:
        print(f"  Analyzing batch of {len(descriptions)} videos with Azure OpenAI...")
        formatted = self._format_descriptions(descriptions)
        for attempt in range(self.max_retries):
            try:
                sleep(self.request_delay)
                prompt = self._create_prompt().format(
                    descriptions=formatted,
                    known_categories=list(self.known_categories) if self.known_categories else "No categories yet - suggest appropriate ones"
                )
                response = self.client.chat.completions.create(
                    model=self.deployment_name,
                    messages=[{"role": "user", "content": prompt}],
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
                print(f"    Error analyzing batch (attempt {attempt + 1}/{self.max_retries}): {e}")
                if "429" in str(e) and attempt < self.max_retries - 1:
                    sleep(self.retry_delay * (attempt + 1))
                    continue
                return [
                    {"video_id": d["video_id"], "speakers": [], "categories": []} for d in descriptions
                ]

