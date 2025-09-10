from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from playwright.sync_api import sync_playwright


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
        verbose: bool = False,
    ) -> List[SessionEntry]:
        if not self._robots_allowed(self.session_list_url):
            raise RuntimeError("robots.txt disallows fetching the session list URL")

        results: List[SessionEntry] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.session.headers.get("User-Agent", DEFAULT_USER_AGENT))
            page = context.new_page()
            page.set_default_navigation_timeout(self.request_timeout * 1000)
            if verbose:
                print(f"[sessions] Loading list: {self.session_list_url}")
            page.goto(self.session_list_url, wait_until="domcontentloaded")

            # Try to accept cookie dialogs if present
            for selector in [
                'button:has-text("Accept All")',
                'button:has-text("Accept selected")',
                'button:has-text("Accept")',
                'button:has-text("OK")',
            ]:
                try:
                    loc = page.locator(selector).first
                    if loc.is_visible():
                        loc.click()
                        break
                except Exception:
                    pass

            # Attempt to load all content (if lazy loaded)
            try:
                prev_height = 0
                for _ in range(20):
                    page.wait_for_load_state("networkidle", timeout=self.request_timeout * 1000)
                    height = page.evaluate("document.body.scrollHeight")
                    if height == prev_height:
                        break
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    prev_height = height
                    time.sleep(0.2)
            except Exception:
                pass

            js = r"""
(() => {
  const anchors = Array.from(document.querySelectorAll('a[href*="session="]'));
  const seen = new Set();
  const items = [];
  for (const a of anchors) {
    try {
      const url = a.href;
      const sid = new URL(url, location.href).searchParams.get('session');
      if (!sid || seen.has(sid)) continue;
      seen.add(sid);
      const title = (a.textContent || '').replace(/\s+/g, ' ').trim();

      let speakers = [];
      let categories = [];
      // Prefer structured extraction inside the session container block
      const cont = a.closest('.title') || a.parentElement?.closest('.title') || null;
      if (cont) {
        speakers = Array.from(cont.querySelectorAll('.speaker-name a span, .speaker-name a, .speaker-name .text-start'))
          .map(n => (n.textContent || '').replace(/\s+/g, ' ').trim())
          .filter(Boolean);
        categories = Array.from(cont.querySelectorAll('a.session-tags'))
          .map(n => (n.textContent || '').replace(/\s+/g, ' ').trim())
          .filter(Boolean);
      }
      const scanFrom = (node) => {
        let cur = node ? node.nextSibling : null;
        let hops = 0;
        while (cur && hops < 40 && (speakers.length === 0 || categories.length === 0)) {
          let raw = '';
          if (cur.nodeType === Node.ELEMENT_NODE) raw = (cur.textContent || '').replace(/\s+/g, ' ').trim();
          else if (cur.nodeType === Node.TEXT_NODE) raw = String(cur.textContent || '').replace(/\s+/g, ' ').trim();
          if (raw) {
            const sp = raw.match(/\b(Speakers?)\b\s*[:\-]?\s*(.*)$/i);
            if (sp && speakers.length === 0) {
              let rest = sp[2] || '';
              // stop at Tags if both present on one line
              const cut = rest.match(/(.*?)(?:\bTags\b.*)?$/i);
              if (cut && cut[1]) rest = cut[1];
              const el = (cur.nodeType === Node.ELEMENT_NODE) ? cur : null;
              let vals = [];
              if (el) {
                vals = Array.from(el.querySelectorAll('a,span,strong,b'))
                  .map(n => (n.textContent || '').replace(/\s+/g, ' ').trim())
                  .filter(t => t && !/^Speakers?$/i.test(t));
              }
              if (vals.length === 0 && rest) vals = rest.split(/\s*[•|,\/]\s*/).map(s => s.trim()).filter(Boolean);
              speakers.push(...vals);
            }
            const tg = raw.match(/\b(Tags)\b\s*[:\-]?\s*(.*)$/i);
            if (tg && categories.length === 0) {
              const rest = tg[2] || '';
              const el = (cur.nodeType === Node.ELEMENT_NODE) ? cur : null;
              let vals2 = [];
              if (el) {
                vals2 = Array.from(el.querySelectorAll('a,span,div'))
                  .map(n => (n.textContent || '').replace(/\s+/g, ' ').trim())
                  .filter(t => t && !/^Tags$/i.test(t));
              }
              if (vals2.length === 0 && rest) vals2 = rest.split(/\s*[•|,\/]\s*/).map(s => s.trim()).filter(Boolean);
              categories.push(...vals2);
            }
          }
          cur = cur.nextSibling;
          hops++;
        }
      };
      if (speakers.length === 0 || categories.length === 0) scanFrom(a);
      if (speakers.length === 0 || categories.length === 0) scanFrom(a.parentElement);
      if (speakers.length === 0 || categories.length === 0) scanFrom(a.parentElement ? a.parentElement.parentElement : null);
      speakers = Array.from(new Set(speakers.filter(Boolean)));
      categories = Array.from(new Set(categories.filter(Boolean)));
      items.push({ session_id: sid, title, detail_url: url, speakers, categories });
    } catch (e) { /* ignore */ }
  }
  return items;
})();
"""
            data = page.evaluate(js)
            if verbose:
                print(f"[sessions] Found {len(data) if isinstance(data, list) else 0} session links on list page")

            if isinstance(data, list):
                for item in data[: (max_items or len(data))]:
                    # Polite delay between processing each session (affects detail fetches too)
                    time.sleep(random.uniform(*polite_delay_range))
                    session_id = str(item.get("session_id", ""))
                    title = self._clean(item.get("title", ""))
                    detail_url = str(item.get("detail_url", ""))
                    speakers = [self._clean(s) for s in item.get("speakers", [])]
                    categories = [self._clean(c) for c in item.get("categories", [])]
                    description = ""

                    # Only fetch detail to enrich description (not speakers/tags)
                    if not description:
                        try:
                            if verbose:
                                print(f"[sessions] Enriching from detail: {detail_url}")
                            p2 = context.new_page()
                            p2.set_default_navigation_timeout(self.request_timeout * 1000)
                            p2.goto(detail_url, wait_until="domcontentloaded")
                            # Extract title and description from the content column next to the H1 block; ignore generic schedule link
                            detail_js = r"""
((expectedTitle) => {
  const out = { title: '', description: '', speakers: [], tags: [] };
  const pageH1 = (document.querySelector('h1')?.textContent || '').replace(/\s+/g, ' ').trim();
  const norm = s => (s || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const expected = norm(expectedTitle || '');
  const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4'))
    .map(h => ({ el: h, text: (h.textContent || '').replace(/\s+/g, ' ').trim() }))
    .filter(h => h.text && h.text !== pageH1);
  // Prefer a heading that includes the expected title tokens
  let best = null;
  if (expected) {
    const tokens = expected.split(' ').filter(t => t.length > 2).slice(0, 6);
    let bestScore = 0;
    for (const h of headings) {
      const ht = norm(h.text);
      let score = 0;
      for (const t of tokens) { if (ht.includes(t)) score++; }
      if (score > bestScore) { bestScore = score; best = h; }
    }
  }
  if (!best) {
    best = headings.find(h => h.el.tagName.toLowerCase() !== 'h1') || headings[0] || null;
  }
  if (best) {
    out.title = best.text;
    // The description is typically in the next grid column: .g-col-lg-6.g-col-12.mb-3.pt-5
    const contentCol = document.querySelector('.g-col-lg-6.g-col-12.mb-3.pt-5');
    const chunks = [];
    if (contentCol) {
      const ps = Array.from(contentCol.querySelectorAll('p'));
      for (const p of ps) {
        const txt = (p.textContent || '').replace(/\s+/g, ' ').trim();
        if (txt && txt.length > 60) chunks.push(txt);
      }
    }
    if (chunks.length === 0) {
      // Fallback to scan siblings after best heading, skipping nav
      let el = best.el.nextElementSibling;
      while (el) {
        const tag = (el.tagName || '').toLowerCase();
        const txt = (el.textContent || '').replace(/\s+/g, ' ').trim();
        if (/^h[1-6]$/.test(tag) && /speakers?|tags/i.test(txt)) break;
        if (/back to event schedule/i.test(txt)) { el = el.nextElementSibling; continue; }
        if ((tag === 'p' || tag === 'div') && txt && txt.length > 60) chunks.push(txt);
        el = el.nextElementSibling;
      }
    }
    out.description = chunks.join('\n\n');
  }
  const head = Array.from(document.querySelectorAll('h2,h3,h4')).find(h => /speakers?/i.test(h.textContent || ''));
  if (head) {
    let el = head.nextElementSibling;
    while (el) {
      if (el.matches('h2,h3,h4,h5,h6')) break;
      const cand = el.querySelector('h3,h4,strong,b,.speaker-name,.name');
      const name = (cand?.textContent || '').replace(/\s+/g, ' ').trim();
      if (name) out.speakers.push(name);
      const txt = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!name && txt && !/Image|LinkedIn/i.test(txt)) {
        out.speakers.push(txt.split('\n')[0].trim());
      }
      el = el.nextElementSibling;
    }
  }
  // Optionally pick up tags on detail page if present
  const maybeTags = Array.from(document.querySelectorAll('*'))
    .find(n => /^tags\b/i.test((n.textContent || '').trim()));
  if (maybeTags) {
    const t = (maybeTags.textContent || '').replace(/\s+/g, ' ').trim();
    const m = t.match(/tags\s*[:\-]?\s*(.*)$/i);
    if (m) {
      out.tags = m[1].split(/\s*[•|,\/]\s*/).map(s => s.trim()).filter(Boolean);
    }
  }
  out.speakers = Array.from(new Set(out.speakers.filter(Boolean)));
  out.tags = Array.from(new Set(out.tags.filter(Boolean)));
  return out;
})
"""
                            detail = p2.evaluate(detail_js, title)
                            p2.close()
                            if isinstance(detail, dict):
                                # Keep speakers/categories from list; only take description from detail
                                if not title:
                                    title = self._clean(str(detail.get("title", ""))) or title
                                description = self._clean(str(detail.get("description", "")))
                        except Exception:
                            pass

                    results.append(
                        SessionEntry(
                            session_id=session_id,
                            title=title,
                            detail_url=detail_url,
                            speakers=speakers,
                            categories=categories,
                            description=description,
                        )
                    )

            if verbose:
                print(f"[sessions] Collected {len(results)} sessions")
            browser.close()

        return results


