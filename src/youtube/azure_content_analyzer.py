from openai import AzureOpenAI
from typing import List, Dict, Set
import os
import logging
import json
from pathlib import Path
from time import sleep
import time

class AzureContentAnalyzer:
    def __init__(self, batch_size: int = 5):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-05-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        self.request_delay = 2  # Delay between Azure API calls in seconds
        self.max_retries = 3
        self.retry_delay = 30  # Delay in seconds when hitting rate limit
        
        # Load or initialize category mapping
        self.categories_file = Path("data/category_mapping.json")
        self.known_categories = self._load_category_mapping()

    def _load_category_mapping(self) -> Set[str]:
        """Load existing category mapping and flatten it into a set of categories"""
        if self.categories_file.exists():
            with open(self.categories_file, 'r') as f:
                mapping = json.load(f)
                categories = set()
                
                # Process each main category and its subcategories
                for category, subcategories in mapping.items():
                    categories.add(category)  # Add main category
                    
                    # Add subcategories if they exist
                    if isinstance(subcategories, list):
                        # Skip 'General' and 'Other' subcategories as we'll use the parent category instead
                        categories.update(subcat for subcat in subcategories 
                                       if subcat not in ["General", "Other"])
                
                return categories
        return set()

    def analyze_content_batch(self, descriptions: List[Dict[str, str]]) -> List[Dict[str, any]]:
        """Extract speakers and categories from multiple video descriptions using Azure OpenAI."""
        batch_num = getattr(self, '_batch_counter', 0) + 1
        setattr(self, '_batch_counter', batch_num)
        
        if not self.known_categories:
            self.logger.error("No categories available in mapping file. Cannot process videos.")
            return [{'video_id': desc['video_id'], 'speakers': [], 'categories': []} for desc in descriptions]
        
        self.logger.info(f"Processing batch {batch_num}: {len(descriptions)} descriptions...")
        
        formatted_descriptions = self._format_descriptions(descriptions)

        for attempt in range(self.max_retries):
            try:
                response = self._get_ai_response(formatted_descriptions)
                results = json.loads(response.choices[0].message.content)
                
                # Convert numbered results to list and validate categories
                numbered_results = []
                for i in range(1, len(descriptions) + 1):
                    result = results.get(str(i), {'speakers': [], 'categories': []})
                    # Filter out any categories not in the known set
                    result['categories'] = [cat for cat in result['categories'] if cat in self.known_categories]
                    numbered_results.append(result)
                
                final_results = self._create_final_results(descriptions, numbered_results)
                
                self.logger.info(f"Successfully processed batch {batch_num}")
                
                return final_results
                
            except Exception as e:
                if "429" in str(e) and attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    self.logger.warning(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt + 1}/{self.max_retries}")
                    sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Error in batch {batch_num}: {e}")
                    return [{'video_id': desc['video_id'], 'speakers': [], 'categories': []} for desc in descriptions]

    def _create_analysis_prompt(self) -> str:
        """Create the prompt for content analysis"""
        categories_list = sorted(list(self.known_categories))
        if not categories_list:
            self.logger.warning("No categories found in mapping file!")
            return ""
        
        return """For each numbered YouTube video below:
        1. Identify the main speakers or participants (excluding moderators, hosts, or interviewers)
        2. Assign 1-2 relevant categories ONLY from this fixed list:
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

    def _format_descriptions(self, descriptions: List[Dict[str, str]]) -> str:
        """Format descriptions for the prompt"""
        return "\n\n".join(
            f"{i+1}. Title: {desc['title']}\nDescription: {desc['description']}" 
            for i, desc in enumerate(descriptions)
        )

    def _get_ai_response(self, formatted_descriptions: str):
        """Get response from Azure OpenAI with rate limiting"""
        sleep(self.request_delay)  # Add delay before each Azure API call
        
        prompt = self._create_analysis_prompt().format(
            descriptions=formatted_descriptions,
            known_categories=list(self.known_categories) if self.known_categories else "No categories yet - suggest appropriate ones"
        )
        
        return self.client.chat.completions.create(
            model=self.deployment_name,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000,
            response_format={"type": "json_object"}
        )

    def _create_final_results(self, descriptions: List[Dict[str, str]], results: List[Dict]) -> List[Dict]:
        """Create final results with video IDs"""
        return [
            {
                'video_id': descriptions[i]['video_id'],
                'speakers': result['speakers'],
                'categories': result['categories']
            }
            for i, result in enumerate(results)
        ] 