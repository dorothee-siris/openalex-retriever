# core/processors.py
"""Data processing functions"""

from typing import Dict, List, Any, Optional
import pandas as pd
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed

from .api_client import MAX_WORKERS
from .formatters import (
    OPENALEX_PATTERN,
    DOI_PATTERN,
    clean_text_field,
    format_abstract_optimized,
    format_authors_simple,
    format_institutions,
    format_raw_affiliation_strings,
    format_counts_by_year,
    format_topic_and_score,
    format_concepts,
    format_sdgs,
    format_grants,
    extract_author_position,
)

# -------------------- helpers --------------------

def get_value_from_nested_dict(data: Dict, key_path: str) -> Any:
    """Extract value from nested dictionary"""
    if not data or not key_path:
        return None
    keys = key_path.split(".")
    value = data
    for key in keys:
        if value is None:
            return None
        try:
            value = value.get(key)
        except AttributeError:
            return None
    return value


# -------------------- batch processing --------------------

def process_publications_batch(
    results: List[Dict],
    entity_id: str,
    entity_name: str,
    entity_type: str,
    selected_metadata: List[str],
) -> List[Dict]:
    """
    Process publications and stamp them with the triggering entity:
      - for institutions: institutions_extracted = entity_name
      - for authors: authors_extracted = <Name, Surname from input>, position_extracted = First/Middle/Last
    """
    publications: List[Dict] = []

    for pub in results:
        pub_data: Dict[str, Any] = {}

        if entity_type == "institution":
            pub_data["institutions_extracted"] = entity_name
            pub_data["authors_extracted"] = ""
            pub_data["position_extracted"] = ""
        else:  # author
            pub_data["institutions_extracted"] = ""
            # entity_name is now "Name, Surname" coming from UI (file label)
            pub_data["authors_extracted"] = entity_name
            pub_data["position_extracted"] = extract_author_position(pub, entity_id)

        for field in selected_metadata:
            if field == "id":
                value = OPENALEX_PATTERN.sub("", pub.get("id", ""))
            elif field == "doi":
                doi = pub.get("doi", "")
                value = DOI_PATTERN.sub("", doi) if doi else ""
            elif field == "display_name":
                value = clean_text_field(pub.get("display_name", ""))
            elif field == "abstract_inverted_index":
                value = format_abstract_optimized(pub.get("abstract_inverted_index", {}))
            elif field == "authorships":
                value = format_authors_simple(pub.get("authorships", []))
            elif field == "institutions":
                value = format_institutions(pub.get("authorships", []))
            elif field == "raw_affiliation_strings":
                value = format_raw_affiliation_strings(pub.get("authorships", []))
            elif field == "primary_topic_and_score":
                topic_name = get_value_from_nested_dict(pub, "primary_topic.display_name")
                topic_score = get_value_from_nested_dict(pub, "primary_topic.score")
                value = f"{topic_name} ; {topic_score:.4f}" if topic_name and topic_score is not None else ""
            elif field.startswith("primary_location.source"):
                value = get_value_from_nested_dict(pub, field)
                if field == "primary_location.source.issn":
                    issns = value or []
                    value = ",".join(issns)
                else:
                    value = clean_text_field(value) if isinstance(value, str) else value
            elif field == "corresponding_author_ids":
                ids = pub.get("corresponding_author_ids", [])
                value = " | ".join(OPENALEX_PATTERN.sub("", i) for i in ids)
            elif field == "corresponding_institution_ids":
                ids = pub.get("corresponding_institution_ids", [])
                value = " | ".join(OPENALEX_PATTERN.sub("", i) for i in ids)
            elif field == "counts_by_year":
                value = format_counts_by_year(pub.get("counts_by_year", []))
            elif field == "topics":
                value = format_topic_and_score(pub.get("topics", []))
            elif field == "concepts":
                value = format_concepts(pub.get("concepts", []))
            elif field == "sustainable_development_goals":
                value = format_sdgs(pub.get("sustainable_development_goals", []))
            elif field == "grants":
                value = format_grants(pub.get("grants", []))
            elif field == "datasets":
                value = ", ".join(pub.get("datasets", []))
            else:
                value = get_value_from_nested_dict(pub, field)
                if isinstance(value, str):
                    value = clean_text_field(value)
                elif value is not None and not isinstance(value, str):
                    value = str(value)

            pub_data[field] = value if value is not None else ""

        publications.append(pub_data)

    return publications


# -------------------- deduplication --------------------

def deduplicate_publications_optimized(all_publications: List[Dict]) -> List[Dict]:
    """
    Merge duplicates by work id.
    - Combine institutions (set)
    - Combine matched authors from the user list (map: 'Name, Surname' -> position)
    - Output aligned pipes: 'Authors Extracted' and 'Author Position'
    """
    seen: Dict[str, Dict] = {}

    for pub in all_publications:
        pub_id = pub.get("id", "")
        if not pub_id:
            continue

        entry = seen.get(pub_id)
        if not entry:
            entry = {
                "data": pub,
                "institutions": set(),
                "author_positions": {},  # { "Name, Surname": "First/Middle/Last/Not found" }
            }
            seen[pub_id] = entry

        inst = (pub.get("institutions_extracted") or "").strip()
        if inst:
            entry["institutions"].add(inst)

        author_label = (pub.get("authors_extracted") or "").strip()    # "Name, Surname"
        position = (pub.get("position_extracted") or "").strip()
        if author_label:
            # don't overwrite an existing position unless the new one is non-empty
            if author_label not in entry["author_positions"] or not entry["author_positions"][author_label]:
                entry["author_positions"][author_label] = position

    # Build final rows
    result: List[Dict] = []
    for item in seen.values():
        row = item["data"].copy()

        inst_list = sorted(filter(None, item["institutions"]))
        # sort authors alphabetically for a stable order
        author_labels = sorted(item["author_positions"].keys())

        row["institutions_extracted"] = " | ".join(inst_list)
        row["authors_extracted"] = " | ".join(author_labels)
        row["position_extracted"] = " | ".join(item["author_positions"].get(a, "") for a in author_labels)

        result.append(row)

    return result


# -------------------- fetching --------------------

def fetch_single_doc_type(
    session,
    url: str,
    base_filter_str: str,
    doc_type: Optional[str],
    entity_id: str,
    entity_name: str,
    entity_type: str,
    selected_metadata: List[str],
    page_callback=None,
    request_callback=None,
) -> List[Dict]:
    """
    Fetch publications for a single document type.
    - page_callback(int_added) is called after each page to increment the UI counter
    - request_callback(ok: bool) is called after each HTTP attempt
    """
    from .api_client import MAILTO, fetch_works_page

    filter_str = f"{base_filter_str},type:{doc_type}" if doc_type else base_filter_str
    params = {
        "filter": filter_str,
        "per_page": 50,
        "mailto": MAILTO,
    }

    publications: List[Dict] = []

    # First page
    results, total_count, success = fetch_works_page(session, url, params)
    if request_callback:
        request_callback(bool(success))
    if not success:
        return publications

    if page_callback:
        page_callback(len(results))

    publications.extend(
        process_publications_batch(results, entity_id, entity_name, entity_type, selected_metadata)
    )

    per_page = 50
    total_pages = min((total_count + per_page - 1) // per_page, 200)

    # Remaining pages
    for page in range(2, total_pages + 1):
        params["page"] = page
        results, _, success = fetch_works_page(session, url, params)

        if request_callback:
            request_callback(bool(success))

        if success:
            if page_callback:
                page_callback(len(results))
            publications.extend(
                process_publications_batch(results, entity_id, entity_name, entity_type, selected_metadata)
            )

        if len(publications) % 5000 == 0:
            gc.collect()

    return publications


def fetch_publications_parallel(
    session,
    entity_id: str,
    entity_name: str,
    entity_type: str,
    start_year: int,
    end_year: int,
    doc_types: List[str],
    selected_metadata: List[str],
    language_filter: str,
    page_callback=None,
    request_callback=None,
) -> List[Dict]:
    """
    Fetch publications for an entity (institution or author).
    If callbacks are provided, we run sequentially (so the UI can update on each page).
    Otherwise we keep the original parallel-per-doc-type behavior.
    """
    if entity_id.startswith("https://openalex.org/"):
        entity_id = entity_id.split("/")[-1]

    entity_id = entity_id.lower()
    url = "https://api.openalex.org/works"

    # Build filter based on entity type
    if entity_type == "institution":
        entity_filter = f"authorships.institutions.id:{entity_id}"
    else:  # author
        entity_filter = f"authorships.author.id:{entity_id}"

    base_filter_parts = [
        entity_filter,
        f"publication_year:{start_year}-{end_year}",
    ]
    if language_filter == "english_only":
        base_filter_parts.append("language:en")

    base_filter_str = ",".join(base_filter_parts)
    all_publications: List[Dict] = []

    # All-works mode
    if not doc_types:
        publications = fetch_single_doc_type(
            session,
            url,
            base_filter_str,
            None,
            entity_id,
            entity_name,
            entity_type,
            selected_metadata,
            page_callback=page_callback,
            request_callback=request_callback,
        )
        all_publications.extend(publications)

    else:
        # If no live UI callbacks, we can safely parallelize per doc type
        if page_callback is None and request_callback is None:
            with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(doc_types))) as executor:
                futures = []
                for doc_type in doc_types:
                    futures.append(
                        executor.submit(
                            fetch_single_doc_type,
                            session,
                            url,
                            base_filter_str,
                            doc_type,
                            entity_id,
                            entity_name,
                            entity_type,
                            selected_metadata,
                            None,
                            None,
                        )
                    )
                for f in as_completed(futures):
                    try:
                        all_publications.extend(f.result())
                    except Exception as e:
                        # swallow; counting of failed HTTP is done inside fetch_works_page path
                        pass
        else:
            # Sequential when we want live page/call updates
            for doc_type in doc_types:
                publications = fetch_single_doc_type(
                    session,
                    url,
                    base_filter_str,
                    doc_type,
                    entity_id,
                    entity_name,
                    entity_type,
                    selected_metadata,
                    page_callback=page_callback,
                    request_callback=request_callback,
                )
                all_publications.extend(publications)

    # Remove duplicates within entity (same id appearing across doc types)
    unique_publications: Dict[str, Dict] = {}
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id and pub_id not in unique_publications:
            unique_publications[pub_id] = pub

    return list(unique_publications.values())
