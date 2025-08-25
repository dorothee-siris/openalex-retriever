"""Data processing functions"""
import pandas as pd
import gc
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import io

from .api_client import rate_limited_get, get_session, MAX_WORKERS
from .formatters import *

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

def process_publications_batch(results: List[Dict], entity_id: str, entity_name: str, 
                              entity_type: str, selected_metadata: List[str]) -> List[Dict]:
    """Process publications in batches for better memory management"""
    publications = []
    
    for pub in results:
        pub_data = {}
        
        # Set entity extraction field based on type
        if entity_type == "institution":
            pub_data["institutions_extracted"] = entity_name
            pub_data["authors_extracted"] = ""
            pub_data["position_extracted"] = ""
        else:  # author
            pub_data["institutions_extracted"] = ""
            pub_data["authors_extracted"] = entity_name
            pub_data["position_extracted"] = extract_author_position(pub, entity_id)
        
        # Process metadata fields
        for field in selected_metadata:
            if field == "id":
                value = OPENALEX_PATTERN.sub('', pub.get("id", ""))
            elif field == "doi":
                doi = pub.get("doi", "")
                value = DOI_PATTERN.sub('', doi) if doi else ""
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
                value = " | ".join(OPENALEX_PATTERN.sub('', id) for id in ids)
            elif field == "corresponding_institution_ids":
                ids = pub.get("corresponding_institution_ids", [])
                value = " | ".join(OPENALEX_PATTERN.sub('', id) for id in ids)
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

def deduplicate_publications_optimized(all_publications: List[Dict]) -> List[Dict]:
    """Optimized deduplication using sets and single pass"""
    seen = {}
    
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id:
            if pub_id not in seen:
                seen[pub_id] = {
                    'data': pub,
                    'institutions': {pub.get("institutions_extracted", "")} if pub.get("institutions_extracted") else set(),
                    'authors': {pub.get("authors_extracted", "")} if pub.get("authors_extracted") else set()
                }
            else:
                # Merge entity information
                inst = pub.get("institutions_extracted", "")
                if inst:
                    seen[pub_id]['institutions'].add(inst)
                auth = pub.get("authors_extracted", "")
                if auth:
                    seen[pub_id]['authors'].add(auth)
    
    # Build final list with merged entities
    result = []
    for item in seen.values():
        pub_data = item['data'].copy()
        pub_data['institutions_extracted'] = ' | '.join(sorted(filter(None, item['institutions'])))
        pub_data['authors_extracted'] = ' | '.join(sorted(filter(None, item['authors'])))
        result.append(pub_data)
    
    return result

def fetch_single_doc_type(session, url: str, base_filter_str: str, doc_type: str, 
                         entity_id: str, entity_name: str, entity_type: str, 
                         selected_metadata: List[str], progress_callback=None) -> List[Dict]:
    """Fetch publications for a single document type"""
    from .api_client import MAILTO, fetch_works_page
    
    filter_str = f"{base_filter_str},type:{doc_type}" if doc_type else base_filter_str
    params = {
        "filter": filter_str,
        "per_page": 50,
        "mailto": MAILTO
    }
    
    publications = []
    
    # First request to get total count
    results, total_count, success = fetch_works_page(session, url, params)
    if not success:
        return publications
    
    # Process first page
    publications.extend(process_publications_batch(
        results, entity_id, entity_name, entity_type, selected_metadata
    ))
    
    # Calculate pages
    per_page = 50
    total_pages = min((total_count + per_page - 1) // per_page, 200)
    
    # Get remaining pages
    for page in range(2, total_pages + 1):
        params["page"] = page
        results, _, success = fetch_works_page(session, url, params)
        
        if success:
            publications.extend(process_publications_batch(
                results, entity_id, entity_name, entity_type, selected_metadata
            ))
        
        # Memory management
        if len(publications) % 5000 == 0:
            gc.collect()
    
    return publications

def fetch_publications_parallel(session, entity_id: str, entity_name: str, entity_type: str,
                              start_year: int, end_year: int, doc_types: List[str], 
                              selected_metadata: List[str], language_filter: str) -> List[Dict]:
    """Fetch publications using parallel requests"""
    from .api_client import MAILTO
    
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
        f"publication_year:{start_year}-{end_year}"
    ]
    
    if language_filter == "english_only":
        base_filter_parts.append("language:en")
    
    base_filter_str = ",".join(base_filter_parts)
    
    all_publications = []
    
    if not doc_types:  # All Works mode
        publications = fetch_single_doc_type(
            session, url, base_filter_str, None, entity_id, entity_name, entity_type, selected_metadata
        )
        all_publications.extend(publications)
    else:
        # Parallel fetch by document type
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(doc_types))) as executor:
            futures = []
            for doc_type in doc_types:
                future = executor.submit(
                    fetch_single_doc_type, session, url, base_filter_str, 
                    doc_type, entity_id, entity_name, entity_type, selected_metadata
                )
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    publications = future.result()
                    all_publications.extend(publications)
                except Exception as e:
                    print(f"Error in parallel fetch: {e}")
    
    # Remove duplicates within entity
    unique_publications = {}
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id and pub_id not in unique_publications:
            unique_publications[pub_id] = pub
    
    return list(unique_publications.values())