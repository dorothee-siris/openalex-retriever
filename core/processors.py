# core/processors.py
"""
Fetch layer (per-entity) for OpenAlex works.

This version:
- Uses OpenAlex **cursor pagination** (no 10k cap)
- Removes year-slice "chunking": ONE full-range query per doc type
- Keeps per-entity parallelism across doc types via ThreadPoolExecutor
- Exposes simple dedup + text cleaning utilities used by the UI
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional
import gc
import re

from .api_client import (
    MAILTO,
    MAX_WORKERS,
    fetch_works_cursor_page,  # cursor page helper; forces per_page, injects mailto via rate_limited_get
)

# ------------------------- Small utilities -------------------------

def _build_filter(
    entity_type: str,
    entity_id: str,
    start_year: int,
    end_year: int,
    language_filter: str,
) -> str:
    """
    Build an OpenAlex `filter` string for an entity + date range (+ optional language).
    """
    # Entity scope
    if entity_type == "author":
        ent = f"authorships.author.id:{entity_id}"
    else:
        # default to institution
        ent = f"institutions.id:{entity_id}"

    # Date range (publication date recommended for OpenAlex filters)
    yr = f"from_publication_date:{start_year}-01-01,to_publication_date:{end_year}-12-31"

    # Language
    lang = "language:en" if language_filter == "english_only" else None

    parts = [ent, yr, lang]
    return ",".join([p for p in parts if p])


def process_publications_batch(
    results: List[Dict[str, Any]],
    entity_id: str,
    entity_name: str,
    entity_type: str,
    selected_metadata: Iterable[str],
) -> List[Dict[str, Any]]:
    """
    Map a cursor page of OpenAlex works to the row structure you need.
    Here we attach a few provenance fields and forward the JSON.
    If you previously had a custom flattener/mapper, you can slot it here.
    """
    out: List[Dict[str, Any]] = []
    for w in results:
        # Shallow copy is safer if callers mutate
        row = dict(w)
        row["__entity_id"] = entity_id
        row["__entity_name"] = entity_name
        row["__entity_type"] = entity_type
        out.append(row)
    return out


def deduplicate_publications_optimized(publications: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Simple, fast de-dup on the OpenAlex `id` field.
    Keeps the first occurrence; extend if you have richer merge rules.
    """
    seen = set()
    unique: List[Dict[str, Any]] = []
    for p in publications:
        pid = p.get("id")
        if pid and pid not in seen:
            seen.add(pid)
            unique.append(p)
    return unique


_ws_re = re.compile(r"\s+")
_ctrl_re = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

def clean_text_field(s: str) -> str:
    """
    Basic text cleaner to keep CSV/Parquet happy.
    - strip control chars
    - collapse whitespace
    - strip leading/trailing whitespace
    """
    if not isinstance(s, str):
        return s
    s = _ctrl_re.sub("", s)
    s = _ws_re.sub(" ", s)
    return s.strip()

# ------------------------- Core fetchers -------------------------

def fetch_single_doc_type(
    session: Any,
    entity_id: str,
    entity_name: str,
    entity_type: str,
    base_filter_str: str,
    doc_type: Optional[str],
    selected_metadata: Iterable[str],
    per_page: int = 200,
) -> List[Dict[str, Any]]:
    """
    Cursor through **all** results for ONE doc_type (or all types if doc_type=None).
    No year chunking. No page caps. Stops only when next_cursor is None.
    """
    url = "https://api.openalex.org/works"
    flt = f"{base_filter_str},type:{doc_type}" if doc_type else base_filter_str
    params = {"filter": flt, "mailto": MAILTO}

    publications: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        results, next_cursor, ok = fetch_works_cursor_page(
            session, url, params, cursor, per_page=per_page
        )
        if not ok:
            # network/HTTP error already retried by api_client; stop this stream
            break

        if results:
            publications.extend(
                process_publications_batch(
                    results, entity_id, entity_name, entity_type, selected_metadata
                )
            )
            # Be nice to the GC for very large streams
            if len(publications) % 5000 == 0:
                gc.collect()

        if not next_cursor:
            break
        cursor = next_cursor

    return publications


def fetch_publications_parallel(
    session: Any,
    entity_id: str,
    entity_name: str,
    entity_type: str,
    start_year: int,
    end_year: int,
    doc_types: List[str],
    selected_metadata: Iterable[str],
    language_filter: str,
    *,
    max_workers: Optional[int] = None,
    per_page: int = 200,
) -> List[Dict[str, Any]]:
    """
    Per-entity orchestrator (no year chunking).
    - If `doc_types` is empty -> single full-range cursor stream (all types)
    - Else -> fan out across doc types with up to `max_workers` (default MAX_WORKERS)
    """
    base_filter = _build_filter(entity_type, entity_id, start_year, end_year, language_filter)
    types: List[Optional[str]] = list(doc_types) if doc_types else [None]

    # Single stream fast-path
    if len(types) == 1:
        return fetch_single_doc_type(
            session,
            entity_id,
            entity_name,
            entity_type,
            base_filter,
            types[0],
            selected_metadata,
            per_page=per_page,
        )

    # Parallel across doc types
    pubs: List[Dict[str, Any]] = []
    mw = max_workers or MAX_WORKERS
    with ThreadPoolExecutor(max_workers=min(mw, len(types))) as ex:
        futures = [
            ex.submit(
                fetch_single_doc_type,
                session,
                entity_id,
                entity_name,
                entity_type,
                base_filter,
                t,
                selected_metadata,
                per_page,
            )
            for t in types
        ]
        for f in as_completed(futures):
            try:
                pubs.extend(f.result() or [])
            except Exception:
                # Swallow a single doc-type failure; continue others
                pass
    return pubs
