from __future__ import annotations

from time import sleep
from typing import Dict, List, Optional, Tuple

from googleapiclient.discovery import build


class YouTubeClient:
    def __init__(self, api_key: str, delay_seconds: float = 1.0) -> None:
        self.youtube = build("youtube", "v3", developerKey=api_key)
        self.delay_seconds = delay_seconds

    def fetch_video_ids(
        self, channel_id: str, limit: Optional[int] = None, page_token: Optional[str] = None
    ) -> Tuple[List[str], Optional[str]]:
        sleep(self.delay_seconds)
        request_size = min(50, (limit or 50))
        request = self.youtube.search().list(
            channelId=channel_id,
            type="video",
            part="id",
            maxResults=request_size,
            pageToken=page_token,
        )
        response = request.execute()
        video_ids = [item["id"]["videoId"] for item in response.get("items", [])]
        return video_ids, response.get("nextPageToken")

    def get_video_details(self, video_ids: List[str]) -> Dict:
        sleep(self.delay_seconds)
        request = self.youtube.videos().list(part="snippet,statistics", id=",".join(video_ids))
        return request.execute()

