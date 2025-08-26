# core/api_client.py
"""OpenAlex API client functions (polite, thread-safe rate limiting; no ORCID lookups)."""

from typing import Dict, List, Optional, Any, Tuple
import threading
import requests
import time
import random

# -------------------- Constants --------------------

MAILTO = "theodore.hervieux@sirisacademic.com"

# Respectful pacing (global, across all threads)
PUBLICATIONS_DELAY = 0.10   # works endpoints
AUTHORS_DELAY = 0.20        # authors search
RETRY_AFTER_429 = 2.0

# Parallelism (used elsewhere; keep as-is)
MAX_WORKERS = 3             # for works fetching by doc type
MAX_AUTHOR_WORKERS = 10     # suggested upper bound for author prefetch (optional)

# -------------------- Global rate limit state --------------------

_last_request_time: float = 0.0
_rate_lock = threading.Lock()

# -------------------- Session --------------------

def get_session() -> requests.Session:
    """Create a requests session with connection pooling and a proper UA."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": f"SIRIS Academic Research Tool/1.0 (mailto:{MAILTO})"
    })
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=3,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

# -------------------- Core HTTP with polite throttling --------------------

def rate_limited_get(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    max_retries: int = 3,
    delay: float = PUBLICATIONS_DELAY,
) -> Optional[requests.Response]:
    """Rate-limited GET with exponential backoff. Global lock enforces min delay across threads."""
    global _last_request_time

    retries = 0
    backoff = RETRY_AFTER_429

    while retries <= max_retries:
        # throttle globally
        with _rate_lock:
            now = time.time()
            elapsed = now - _last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            _last_request_time = time.time()

        try:
            resp = session.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException:
            retries += 1
            if retries <= max_retries:
                time.sleep(backoff)
                backoff *= 2
            continue

        # OK
        if resp.status_code == 200:
            return resp

        # Too many requests -> backoff and retry
        if resp.status_code == 429:
            retries += 1
            if retries > max_retries:
                return resp
            # jittered backoff
            wait = backoff * (0.5 + 0.5 * random.random())
            time.sleep(wait)
            backoff *= 2
            continue

        # Other non-200 -> return as-is (let caller decide)
        return resp

    return None

# -------------------- Authors search (name only) --------------------

def search_author_by_name(session: requests.Session, first_name: str, last_name: str) -> List[Dict[str, Any]]:
    """Search authors by name. Returns raw /authors 'results' list (not /people)."""
    url = "https://api.openalex.org/authors"
    params = {
        "search": f"{first_name} {last_name}",
        "per_page": 50,          # pull a bigger page; caller can cap to 20
        "mailto": MAILTO,
    }
    resp = rate_limited_get(session, url, params=params, delay=AUTHORS_DELAY)
    if resp and resp.status_code == 200:
        try:
            data = resp.json()
            return data.get("results", []) or []
        except Exception:
            return []
    return []

# -------------------- Works paging helper --------------------

def fetch_works_page(session: requests.Session, url: str, params: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int, bool]:
    """
    Fetch one page from /works. Returns (results, total_count, success_flag).
    Caller is responsible for setting 'per_page' and 'page' in params.
    """
    resp = rate_limited_get(session, url, params=params, delay=PUBLICATIONS_DELAY)
    if not resp or resp.status_code != 200:
        return [], 0, False
    try:
        data = resp.json()
        results = data.get("results", []) or []
        total = int(data.get("meta", {}).get("count", 0) or 0)
        return results, total, True
    except Exception:
        return [], 0, False
