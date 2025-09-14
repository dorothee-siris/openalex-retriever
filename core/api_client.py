# core/api_client.py
"""
API client utilities for OpenAlex.
- Global token-bucket rate limiter (REQUESTS_PER_SECOND)
- Shared requests.Session with connection pooling
- Simple 429 backoff
- Single polite-pool email (no rotation)
"""

from typing import Optional, Dict
import requests
import threading
import time

# ---- Config knobs (tune here) ----
MAILTO = "theodore.hervieux@sirisacademic.com"
REQUESTS_PER_SECOND = 8          # global polite rate
MAX_WORKERS = 10                 # threads for slices/doc_types per entity
PARALLEL_ENTITIES = 5            # run up to N institutions/authors at once


# ---- helpers ----

def search_author_by_name(session: requests.Session, first_name: str, last_name: str):
    url = "https://api.openalex.org/authors"
    params = {"search": f"{first_name} {last_name}", "per_page": 50}
    r = rate_limited_get(session, url, params=params, timeout=30)
    if not r or r.status_code != 200:
        return []
    try:
        return (r.json() or {}).get("results", []) or []
    except Exception:
        return []
    
from typing import Tuple, List, Dict, Any, Optional


def fetch_works_cursor_page(
    session: requests.Session, url: str, base_params: Dict[str, Any], cursor: Optional[str]
) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
    # mailto is injected inside rate_limited_get
    params = dict(base_params)
    params.setdefault("per_page", 200)
    params["cursor"] = cursor if cursor is not None else "*"

    r = rate_limited_get(session, url, params=params, timeout=30)
    if not r or r.status_code != 200:
        return [], None, False
    try:
        data = r.json() or {}
        results = data.get("results", []) or []
        next_cursor = (data.get("meta") or {}).get("next_cursor")
        return results, next_cursor, True
    except Exception:
        return [], None, False


# ---- Token-bucket limiter ----
class RateLimiter:
    def __init__(self, rps: float):
        self.rps = float(rps)
        self.tokens = self.rps
        self.last = time.time()
        self.lock = threading.Lock()

    def wait(self):
        """Block until a token is available."""
        while True:
            with self.lock:
                now = time.time()
                dt = now - self.last
                self.tokens = min(self.rps, self.tokens + dt * self.rps)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_s = (1.0 - self.tokens) / self.rps
            time.sleep(wait_s)

_limiter = RateLimiter(REQUESTS_PER_SECOND)

def get_session() -> requests.Session:
    """Shared session with connection pooling and retries."""
    s = requests.Session()
    s.headers.update({"User-Agent": "SIRIS OpenAlex Retriever/1.0"})
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=3)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def rate_limited_get(session: requests.Session, url: str, params: Optional[Dict] = None, timeout=30) -> Optional[requests.Response]:
    """Global-throttled GET with small retry/backoff on 429/network errors."""
    if params is None:
        params = {}
    params["mailto"] = MAILTO

    backoff = 2.0
    for _ in range(5):  # up to 5 tries
        _limiter.wait()
        try:
            r = session.get(url, params=params, timeout=timeout)
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue

        if r.status_code == 429:
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue
        return r
    return None