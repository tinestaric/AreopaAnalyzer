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
    description: str
    source: str = "directions-emea-2025"
    # Structured category fields
    session_type: Optional[str] = None
    level: Optional[str] = None
    roles: List[str] = None
    product: Optional[str] = None
    areas: List[str] = None  # Multiple areas/domains
    submitter: Optional[str] = None  # "Microsoft" or "Directions"
    # Legacy field for backward compatibility
    categories: List[str] = None
    
    def __post_init__(self):
        if self.roles is None:
            self.roles = []
        if self.areas is None:
            self.areas = []
        if self.categories is None:
            self.categories = []


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
        session_links = list_soup.select('a[href*="session="]')
        
        print(f"Found {len(session_links)} session links on the list page")
        
        for i, a in enumerate(session_links, 1):
            href = urljoin(self.session_list_url, a.get("href") or "")
            if "session=" not in href:
                continue
            sid = parse_qs(urlparse(href).query).get("session", [""])[0]
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            
            print(f"Processing session {len(results)+1}: {sid} - ", end="", flush=True)

            # Find the specific session container. Look for the closest parent div that contains
            # this session's title but doesn't contain other session titles.
            card = None
            current = a
            for _ in range(6):
                parent_div = current.find_parent("div") if current else None
                if not parent_div:
                    break
                
                # Check if this div contains speakers/tags for this session
                if parent_div.select_one(".speaker-name, a.session-tags"):
                    # Ensure it doesn't contain multiple session titles
                    session_links = parent_div.select('a[href*="session="]')
                    if len(session_links) == 1 and session_links[0] == a:
                        card = parent_div
                        break
                current = parent_div
            
            # Fallback: try to find the session block by class structure
            if card is None:
                # Look for the div with classes that typically wrap a single session
                session_name_div = a.find_parent("div", class_="session-name")
                if session_name_div:
                    card = session_name_div.find_parent("div", class_=lambda x: x and "title" in x)
                if card is None:
                    card = a.find_parent(["article", "li", "div"]) or a.parent
            title = self._clean(a.get_text(" ", strip=True) or "")
            if not title and card:
                head = card.select_one("h2,h3,h4")
                title = self._clean(head.get_text(" ", strip=True) if head else "")
            
            print(title, flush=True)

            # Extract tags/categories from explicit anchors on the list page
            raw_categories: List[str] = []
            if card:
                for tag_anchor in card.select("a.session-tags"):
                    label_text = self._clean(tag_anchor.get_text(" ", strip=True) or "")
                    if label_text and label_text not in raw_categories:
                        raw_categories.append(label_text)
            
            # Categorize tags into structured fields
            categorized = self._categorize_tags(raw_categories)

            # Extract speakers from the list page (avoid visiting detail pages for this)
            speakers: List[str] = []
            if card:
                # Prefer the explicit speaker-name structure
                speaker_nodes = card.select(".speaker-name a span")
                if not speaker_nodes:
                    # Fallback to anchors under speaker-name if spans are absent
                    speaker_nodes = card.select(".speaker-name a")
                for node in speaker_nodes:
                    name = self._clean(node.get_text(" ", strip=True) or "")
                    if name and name not in speakers:
                        speakers.append(name)

            # Fetch description from the detail page only
            description = ""
            try:
                # Respect a small random delay to be polite when fetching details
                try:
                    low, high = polite_delay_range
                    time.sleep(random.uniform(float(low), float(high)))
                except Exception:
                    pass

                detail_soup = self._soup(href)
                # Primary: description sits in the grid column with pt-5 right after the title column
                desc_texts: List[str] = []
                title_h1 = detail_soup.select_one("h1")
                parent_col = title_h1.find_parent(
                    "div",
                    class_=lambda c: c and ("g-col-lg-6" in c and "g-col-12" in c and "mb-3" in c),
                ) if title_h1 else None
                sibling_col = parent_col.find_next_sibling(
                    "div",
                    class_=lambda c: c and ("g-col-lg-6" in c and "g-col-12" in c and "mb-3" in c and "pt-5" in c),
                ) if parent_col else None
                if sibling_col:
                    ps = sibling_col.select("p")
                    if ps:
                        desc_texts = [self._clean(p.get_text(" ", strip=True) or "") for p in ps]
                    else:
                        desc_texts = [self._clean(sibling_col.get_text(" ", strip=True) or "")]
                # Fallbacks
                if not desc_texts:
                    block = detail_soup.select_one(".g-col-lg-6.g-col-12.mb-3.pt-5")
                    if block:
                        ps = block.select("p")
                        if ps:
                            desc_texts = [self._clean(p.get_text(" ", strip=True) or "") for p in ps]
                        else:
                            desc_texts = [self._clean(block.get_text(" ", strip=True) or "")]
                if not desc_texts:
                    # Legacy selectors
                    desc_node = (
                        detail_soup.select_one(".session-description")
                        or detail_soup.select_one(".description")
                        or detail_soup.select_one(".abstract")
                        or detail_soup.select_one("#description")
                    )
                    if desc_node:
                        desc_texts = [self._clean(desc_node.get_text(" ", strip=True) or "")]
                description = " ".join([t for t in desc_texts if t])
            except Exception:
                description = ""

            results.append(
                SessionEntry(
                    session_id=sid,
                    title=title,
                    detail_url=href,
                    speakers=speakers,
                    description=description,
                    session_type=categorized["session_type"],
                    level=categorized["level"],
                    roles=categorized["roles"],
                    product=categorized["product"],
                    areas=categorized["areas"],
                    submitter=categorized["submitter"],
                    categories=categorized["categories"],
                )
            )

            if max_items is not None and len(results) >= max_items:
                print(f"Reached limit of {max_items} sessions")
                break

        print(f"Completed processing {len(results)} sessions")
        return results

    def _categorize_tags(self, raw_categories: List[str]) -> Dict[str, any]:
        """
        Categorize raw session tags into structured fields.
        """
        # Define category sets
        SESSION_TYPES = {"Session (45 min)", "Deep dive session (105 min)", "Workshop (105 min)", 
                        "Keynote", "Sponsor (45 min)", "ISV Theatre (15 min)", "Roundtable (45 min)"}
        LEVELS = {"100 Beginner", "200 Intermediate", "300 Advanced", "400 Expert"}
        ROLE_CATEGORIES = {"Consultant", "Developer", "Project Manager", "Sales & Marketing", 
                          "Leadership", "HR"}
        PRODUCT_CATEGORIES = {"Business Central", "Power Platform", "Other"}
        SUBMITTER_CATEGORIES = {"Microsoft", "Directions"}
        
        # Initialize result structure
        result = {
            "session_type": None,
            "level": None,
            "roles": [],
            "product": None,
            "areas": [],
            "submitter": None,
            "categories": raw_categories.copy()  # Keep original for backward compatibility
        }
        
        # Categorize each tag
        remaining_categories = []
        
        for i, category in enumerate(raw_categories):
            if category in SESSION_TYPES:
                result["session_type"] = category
            elif category in LEVELS:
                result["level"] = category
            elif category in ROLE_CATEGORIES:
                result["roles"].append(category)
            elif category in PRODUCT_CATEGORIES:
                result["product"] = category
            elif category in SUBMITTER_CATEGORIES:
                result["submitter"] = category
            elif category == "All":
                # Disambiguate "All" based on position (Product "All" is second-to-last)
                if i == len(raw_categories) - 2:
                    result["product"] = "All"
                else:
                    result["roles"].append("All")
            else:
                # This is likely an area/domain category
                remaining_categories.append(category)
        
        # Handle areas - collect all domain/area categories
        if remaining_categories:
            # Filter out obvious non-area categories
            area_candidates = [cat for cat in remaining_categories 
                             if cat not in ["EMEA", "2025"]]
            result["areas"] = area_candidates  # Take all as areas
        
        return result


