from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Dict
import logging
from ..utils.config import API_KEY
from .azure_content_analyzer import AzureContentAnalyzer
from time import sleep

class YouTubeAnalyzer:
    def __init__(self, api_key: str, batch_size: int = 5):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.content_analyzer = AzureContentAnalyzer(batch_size=batch_size)
        self.logger = logging.getLogger(__name__)
        self.youtube_delay = 1  # Delay between YouTube API calls in seconds

    def analyze_channel_videos(self, channel_id: str, limit: int = None) -> List[Dict]:
        """
        Analyze videos from a channel, processing their descriptions and returning detailed information.
        Args:
            channel_id: The YouTube channel ID to analyze
            limit: Maximum number of videos to return after sorting by views
        """
        try:
            videos = []
            next_page_token = None
            page_count = 0
            total_videos_found = None
            
            # Get initial page to determine total video count
            initial_request = self.youtube.search().list(
                channelId=channel_id,
                type='video',
                part='id',
                maxResults=1
            )
            initial_response = initial_request.execute()
            total_videos_found = initial_response['pageInfo']['totalResults']
            self.logger.info(f"Found {total_videos_found} total videos in channel")
            
            while True:
                page_count += 1
                self.logger.info(f"Fetching page {page_count} of videos... ({len(videos)}/{total_videos_found} processed)")
                
                # Get video IDs for current page
                video_ids, next_page_token = self._fetch_video_ids(channel_id, limit, len(videos), next_page_token)
                if not video_ids:
                    break
                
                # Get detailed video information
                video_details = self._get_video_details(video_ids)
                
                # Process video descriptions in batches
                results_by_video = self._process_video_descriptions(video_details['items'])
                
                # Add processed videos to results
                videos.extend(self._create_video_entries(video_details['items'], results_by_video))
                
                self.logger.info(f"Progress: {len(videos)}/{total_videos_found} videos processed")
                
                if not next_page_token or (limit and len(videos) >= limit):
                    break
            
            videos = sorted(videos, key=lambda x: x['views'], reverse=True)
            if limit:
                videos = videos[:limit]
                
            self.logger.info(f"Finished processing all videos. Total videos processed: {len(videos)}")
            return videos
            
        except HttpError as e:
            self.logger.error(f"Error fetching videos: {e}")
            return []

    def _fetch_video_ids(self, channel_id: str, limit: int, current_count: int, page_token: str = None) -> tuple:
        sleep(self.youtube_delay)  # Add delay before each YouTube API call
        request_size = min(50, limit - current_count) if limit else 50
        if request_size <= 0:
            return [], None
            
        request = self.youtube.search().list(
            channelId=channel_id,
            type='video',
            part='id',
            maxResults=request_size,
            pageToken=page_token
        )
        response = request.execute()
        
        video_ids = [item['id']['videoId'] for item in response['items']]
        return video_ids, response.get('nextPageToken')

    def _get_video_details(self, video_ids: List[str]) -> Dict:
        sleep(self.youtube_delay)  # Add delay before each YouTube API call
        stats_request = self.youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids)
        )
        return stats_request.execute()

    def _process_video_descriptions(self, videos: List[Dict]) -> Dict:
        description_batch = []
        current_batch = []
        
        for video in videos:
            current_batch.append({
                'video_id': video['id'],
                'description': video['snippet']['description'],
                'title': video['snippet']['title']  # Add title to the batch
            })
            
            if len(current_batch) >= self.content_analyzer.batch_size:
                description_batch.append(current_batch)
                current_batch = []
        
        if current_batch:
            description_batch.append(current_batch)
        
        results_by_video = {}
        for batch in description_batch:
            self.logger.info(f"Processing batch of {len(batch)} videos...")
            results = self.content_analyzer.analyze_content_batch(batch)
            for result in results:
                results_by_video[result['video_id']] = {
                    'speakers': result['speakers'],
                    'categories': result['categories']
                }
                
        return results_by_video

    def _create_video_entries(self, videos: List[Dict], results_by_video: Dict) -> List[Dict]:
        entries = []
        for video in videos:
            title = video['snippet']['title']
            # Remove date prefix if it matches the pattern (8 digits followed by dash)
            if len(title) > 8 and title[:8].isdigit() and title[8:].startswith(' - '):
                title = title[11:].strip()
            
            video_results = results_by_video.get(video['id'], {'speakers': [], 'categories': []})
            
            entries.append({
                'title': title,
                'views': int(video['statistics']['viewCount']),
                'url': f"https://youtube.com/watch?v={video['id']}",
                'description': video['snippet']['description'],
                'speakers': video_results['speakers'],
                'categories': video_results['categories']
            })
        return entries