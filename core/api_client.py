"""OpenAlex API client functions"""
import requests
import numpy as np
import time
import threading
from typing import Dict, List, Optional, Any

# Constants
MAILTO = "theodore.hervieux@sirisacademic.com"
PUBLICATIONS_DELAY = 0.1
AUTHORS_DELAY = 0.2
RETRY_AFTER_429 = 2
MAX_WORKERS = 3

# Global rate limiting
last_request_time = 0
rate_lock = threading.Lock()

def get_session() -> requests.Session:
    """Create and cache a requests session with connection pooling"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': f'SIRIS Academic Research Tool/1.0 (mailto:{MAILTO})'
    })
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def rate_limited_get(session: requests.Session, url: str, params: Optional[Dict] = None, 
                    max_retries: int = 3, delay: float = PUBLICATIONS_DELAY) -> Optional[requests.Response]:
    """Rate-limited API request with exponential backoff"""
    global last_request_time
    retry_count = 0
    backoff_time = RETRY_AFTER_429
    
    while retry_count <= max_retries:
        # Apply rate limiting
        with rate_lock:
            now = time.time()
            elapsed = now - last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            last_request_time = time.time()
        
        try:
            response = session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                retry_count += 1
                if retry_count > max_retries:
                    return response
                wait_time = backoff_time * (0.5 + 0.5 * np.random.random())
                time.sleep(wait_time)
                backoff_time *= 2
            else:
                return response
        except requests.exceptions.RequestException:
            retry_count += 1
            if retry_count <= max_retries:
                time.sleep(backoff_time)
    
    return None

def search_author_by_name(session: requests.Session, first_name: str, last_name: str) -> List[Dict]:
    """Search for an author by name"""
    url = "https://api.openalex.org/authors"
    params = {
        "search": f"{first_name} {last_name}",
        "mailto": MAILTO
    }
    
    response = rate_limited_get(session, url, params=params, delay=AUTHORS_DELAY)
    if response and response.status_code == 200:
        data = response.json()
        return data.get("results", [])
    return []

def search_author_by_orcid(session: requests.Session, orcid: str) -> List[Dict]:
    """Search for an author by ORCID ID"""
    url = "https://api.openalex.org/authors"
    params = {
        "filter": f"orcid:{orcid}",
        "mailto": MAILTO
    }
    
    response = rate_limited_get(session, url, params=params, delay=AUTHORS_DELAY)
    if response and response.status_code == 200:
        data = response.json()
        return data.get("results", [])
    return []

def get_author_details(session: requests.Session, author_id: str) -> Optional[Dict]:
    """Fetch detailed author information using the author ID"""
    if author_id.startswith("A"):
        author_id_clean = author_id
    else:
        author_id_clean = author_id.split("/")[-1]
    
    url = f"https://api.openalex.org/people/{author_id_clean}"
    params = {"mailto": MAILTO}
    
    response = rate_limited_get(session, url, params=params, delay=AUTHORS_DELAY)
    if response and response.status_code == 200:
        return response.json()
    return None

def fetch_works_page(session: requests.Session, url: str, params: Dict) -> tuple:
    """Fetch a single page of works from the API"""
    response = rate_limited_get(session, url, params=params)
    
    if not response or response.status_code != 200:
        return [], 0, False
    
    try:
        data = response.json()
        results = data.get("results", [])
        total_count = data.get("meta", {}).get("count", 0)
        return results, total_count, True
    except Exception:
        return [], 0, False