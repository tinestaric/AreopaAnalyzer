from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; directions-sessions-collector/1.0)"


@dataclass
class SessionEntry:
    session_id: str
    title: str
    detail_url: str
    speakers: List[str]
    categories: List[str]
    description: str
    source: str = "directions-emea-2025"


class DirectionsSessionsClient:
    def __init__(
        self,
        session_list_url: str,
        user_agent: str | None = None,
        request_timeout: int = 60,
        respect_robots: bool = True,
    ) -> None:
        self.session_list_url = session_list_url
        self.session = self._make_session(user_agent or DEFAULT_USER_AGENT)
        self.request_timeout = request_timeout
        self.respect_robots = respect_robots
        self._robots_allowed_cache: Optional[bool] = None

    def _make_session(self, user_agent: str) -> requests.Session:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retries))
        s.headers.update({"User-Agent": user_agent})
        return s

    def _soup(self, url: str) -> BeautifulSoup:
        r = self.session.get(url, timeout=self.request_timeout)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")

    def _robots_allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        if self._robots_allowed_cache is not None:
            return self._robots_allowed_cache
        try:
            from urllib import robotparser

            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            rp = robotparser.RobotFileParser()
            r = self.session.get(robots_url, timeout=self.request_timeout)
            if r.status_code >= 400:
                self._robots_allowed_cache = True
                return True
            rp.parse(r.text.splitlines())
            allowed = rp.can_fetch(self.session.headers.get("User-Agent", DEFAULT_USER_AGENT), url)
            self._robots_allowed_cache = allowed
            return allowed
        except Exception:
            self._robots_allowed_cache = True
            return True

    def _clean(self, text: str) -> str:
        return re.sub(r"\s+", " ", (text or "")).strip()

    def collect(
        self,
        polite_delay_range: tuple[float, float] = (0.3, 0.8),
        max_items: Optional[int] = None,
    ) -> List[SessionEntry]:
        if not self._robots_allowed(self.session_list_url):
            raise RuntimeError("robots.txt disallows fetching the session list URL")
        list_soup = self._soup(self.session_list_url)

        # Identify and parse session cards from the list page only (detail pages are generic)
        results: List[SessionEntry] = []
        seen_ids: set[str] = set()
        for a in list_soup.select('a[href*="session="]'):
            href = urljoin(self.session_list_url, a.get("href") or "")
            if "session=" not in href:
                continue
            sid = parse_qs(urlparse(href).query).get("session", [""])[0]
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)

            card = a.find_parent(["article", "li", "div"]) or a.parent
            title = self._clean(a.get_text(" ", strip=True) or "")
            if not title and card:
                head = card.select_one("h2,h3,h4")
                title = self._clean(head.get_text(" ", strip=True) if head else "")

            # Extract inline tags if present
            tags_text = ""
            if card:
                label = None
                for node in card.find_all(text=True):
                    val = self._clean(str(node))
                    if val.lower().startswith("tags"):
                        label = node
                        break
                if label:
                    sibling_text = self._clean(getattr(label.parent, "get_text", lambda *_: "")(" ", strip=True))
                    tags_text = self._clean(sibling_text.replace("Tags", "", 1).strip(" :"))

            categories = [self._clean(x) for x in re.split(r",|/|\|", tags_text) if self._clean(x)] if tags_text else []

            # Try to capture speakers inline if present
            speakers: List[str] = []
            if card:
                for cand in card.select(".speaker,.speaker-name,.name,strong,b"):
                    name = self._clean(cand.get_text(" ", strip=True) or "")
                    if name and len(name) > 2 and len(name) <= 100 and not re.search(r"^(Tags|Session|Type|Audience)$", name, re.I):
                        speakers.append(name)
            speakers = sorted({n for n in speakers if n})

            results.append(
                SessionEntry(
                    session_id=sid,
                    title=title,
                    detail_url=href,
                    speakers=speakers,
                    categories=categories,
                    description="",
                )
            )

            if max_items is not None and len(results) >= max_items:
                break

        return results


