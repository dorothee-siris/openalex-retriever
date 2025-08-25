import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import unicodedata
from datetime import datetime, timedelta
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

# Constants
MAILTO = "theodore.hervieux@sirisacademic.com"
PUBLICATIONS_DELAY = 0.1  # 100ms between requests
RETRY_AFTER_429 = 2
CURRENT_YEAR = datetime.now().year
BATCH_SIZE = 1000  # For processing large datasets
MAX_WORKERS = 3  # For parallel API requests

# Pre-compiled regex patterns for better performance
OPENALEX_PATTERN = re.compile(r'https://openalex.org/')
DOI_PATTERN = re.compile(r'https://doi.org/')

# Document types from OpenAlex
DOCUMENT_TYPES = [
    "article", "book-chapter", "dataset", "preprint", "dissertation", "book", 
    "review", "paratext", "libguides", "letter", "other", "reference-entry", 
    "report", "editorial", "peer-review", "erratum", "standard", "grant",
    "supplementary-materials", "retraction"
]

# Metadata fields with updated naming
METADATA_FIELDS = {
    # Core Publication Information
    "id": "OpenAlex ID",
    "doi": "DOI",
    "display_name": "Title",
    "publication_year": "Publication Year",
    "publication_date": "Publication Date",
    "language": "Language",
    "type": "Publication Type",
    "abstract_inverted_index": "Abstract",
    "has_fulltext": "Full Text Available",
    "is_retracted": "Is Retracted",
    
    # Access Information
    "open_access.is_oa": "Is OA",
    "open_access.oa_status": "OA Status",
    "apc_paid.value_usd": "Paid APC in USD",
    
    # Publication Source
    "primary_location.source.display_name": "Source",
    "primary_location.source.type": "Source Type",
    "primary_location.source.issn": "ISSN",
    "primary_location.source.host_organization_name": "Publisher",
    "primary_location.pdf_url": "PDF",
    "primary_location.license": "License",
    
    # Authorship Information
    "authorships": "Authors",
    "institutions": "Institutions",
    "raw_affiliation_strings": "Raw Affiliation Strings",
    "countries_distinct_count": "Number of Countries",
    "institutions_distinct_count": "Number of Institutions",
    "corresponding_author_ids": "Corresponding Author IDs",
    "corresponding_institution_ids": "Corresponding Institution IDs",
    
    # Impact Metrics
    "fwci": "Field-Weighted Citation Impact",
    "cited_by_count": "Citation Count",
    "citation_normalized_percentile.value": "Citation Percentile",
    "citation_normalized_percentile.is_in_top_1_percent": "In Top 1% Cited",
    "citation_normalized_percentile.is_in_top_10_percent": "In Top 10% Cited",
    "counts_by_year": "Citations per Year",
    
    # Classification Information
    "primary_topic_and_score": "Primary Topic and Score",
    "primary_topic.subfield.display_name": "Primary Subfield",
    "primary_topic.field.display_name": "Primary Field", 
    "primary_topic.domain.display_name": "Primary Domain",
    "topics": "All Topics",
    "concepts": "Concepts",
    
    # Additional Information
    "sustainable_development_goals": "SDG",
    "grants": "Funding Grants",
    "datasets": "Related Datasets"
}

# Initialize session state
if 'selected_institutions' not in st.session_state:
    st.session_state.selected_institutions = []
if 'last_request_time' not in st.session_state:
    st.session_state.last_request_time = 0
if 'progress_data' not in st.session_state:
    st.session_state.progress_data = {}

@st.cache_resource
def get_session():
    """Create and cache a requests session with connection pooling"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': f'SIRIS Academic Research Tool/1.0 (mailto:{MAILTO})'
    })
    # Add connection pooling for better performance
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

@st.cache_data
def load_institutions():
    """Load institutions from parquet file"""
    try:
        df = pd.read_parquet('institutions_master.parquet')
        return df
    except Exception as e:
        st.error(f"Error loading institutions file: {e}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def search_institutions_cached(df, query):
    """Cached search for institutions"""
    if not query or len(query) < 2:
        return pd.DataFrame()
    
    query_lower = query.lower()
    
    # Vectorized search for better performance
    mask = (
        df['display_name'].str.lower().str.contains(query_lower, na=False, regex=False) |
        df['display_name_alternatives'].str.lower().str.contains(query_lower, na=False, regex=False) |
        df['display_name_acronyms'].str.lower().str.contains(query_lower, na=False, regex=False) |
        df['city'].str.lower().str.contains(query_lower, na=False, regex=False)
    )
    
    results = df[mask]
    
    # Prepare display dataframe
    display_df = pd.DataFrame({
        'Select': False,
        'Name': results['display_name'].values,
        'Acronym': results['display_name_acronyms'].values,
        'Type': results['type'].values,
        'Country': results['country_code'].values,
        'City': results['city'].values,
        'ROR': results['ror_id'].apply(lambda x: f"https://ror.org/{x}" if x else ""),
        'Total Works': results['total_works_count'].values,
        'Avg. Works/Year': results['avg_works_per_year_2021_2023'].values,
        'openalex_id': results['openalex_id'].values
    })
    
    return display_df

def rate_limited_get(session, url, params=None, max_retries=3):
    """Rate-limited API request with exponential backoff"""
    retry_count = 0
    backoff_time = RETRY_AFTER_429
    
    while retry_count <= max_retries:
        # Apply rate limiting
        now = time.time()
        elapsed = now - st.session_state.last_request_time
        if elapsed < PUBLICATIONS_DELAY:
            time.sleep(PUBLICATIONS_DELAY - elapsed)
        st.session_state.last_request_time = time.time()
        
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

def get_value_from_nested_dict(data, key_path):
    """Extract value from nested dictionary - optimized version"""
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

def format_abstract_optimized(inverted_index):
    """Optimized abstract reconstruction using numpy for large abstracts"""
    if not inverted_index:
        return ""
    
    try:
        positions = []
        words = []
        
        for word, pos_list in inverted_index.items():
            positions.extend(pos_list)
            words.extend([word] * len(pos_list))
        
        if not positions:
            return ""
        
        # Use numpy for large abstracts, regular sorting for small ones
        if len(positions) > 1000:
            sort_idx = np.argsort(positions)
            abstract_text = " ".join(np.array(words)[sort_idx])
        else:
            abstract_text = " ".join(word for _, word in sorted(zip(positions, words)))
        
        # Remove line breaks and normalize whitespace
        abstract_text = abstract_text.replace('\n', ' ').replace('\r', ' ')
        abstract_text = ' '.join(abstract_text.split())  # Normalize multiple spaces
        
        return abstract_text
    except Exception:
        return "[Abstract processing error]"

def clean_text_field(text):
    """Clean text fields by removing line breaks and normalizing whitespace"""
    if not text or not isinstance(text, str):
        return text
    
    # Remove line breaks and carriage returns
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    # Normalize multiple spaces to single space
    text = ' '.join(text.split())
    return text

def format_authors_simple(authorships):
    """Format authors information - optimized with list comprehension"""
    if not authorships:
        return ""
    
    authors = [
        f"{authorship.get('author', {}).get('display_name', 'Unknown').strip()} (corresponding)"
        if authorship.get("is_corresponding", False)
        else authorship.get('author', {}).get('display_name', 'Unknown').strip()
        for authorship in authorships
    ]
    
    return " | ".join(authors)

def format_institutions(authorships):
    """Format institutions with deduplication - optimized version"""
    if not authorships:
        return ""
    
    seen = set()
    institutions = []
    
    for authorship in authorships:
        for inst in authorship.get("institutions", []):
            inst_id = inst.get("id", "")
            if inst_id and inst_id not in seen:
                seen.add(inst_id)
                inst_name = inst.get("display_name", "Unknown")
                inst_type = inst.get("type", "Unknown")
                inst_country = inst.get("country_code", "Unknown")
                inst_id_clean = OPENALEX_PATTERN.sub('', inst_id)
                institutions.append(f"{inst_name} ; {inst_type} ; {inst_country} ({inst_id_clean})")
    
    return " | ".join(institutions)

def format_raw_affiliation_strings(authorships):
    """Format raw affiliation strings - optimized with set and line break removal"""
    if not authorships:
        return ""
    
    affiliations = set()
    for authorship in authorships:
        for affiliation in authorship.get("raw_affiliation_strings", []):
            if affiliation and affiliation.strip():
                # Remove line breaks and normalize whitespace
                clean_affiliation = affiliation.strip().replace('\n', ' ').replace('\r', ' ')
                clean_affiliation = ' '.join(clean_affiliation.split())  # Normalize multiple spaces
                clean_affiliation = unicodedata.normalize('NFC', clean_affiliation)
                affiliations.add(clean_affiliation)
    
    return " | ".join(sorted(affiliations))

def format_counts_by_year(counts):
    """Format counts by year - optimized"""
    if not counts:
        return ""
    
    return " | ".join(
        f"{c.get('cited_by_count', 0)} ({c.get('year', 'Unknown')})"
        for c in sorted(counts, key=lambda x: x.get("year", 0), reverse=True)
    )

def format_topic_and_score(topics):
    """Format topics with scores"""
    if not topics:
        return ""
    
    return " | ".join(
        f"{t.get('display_name', 'Unknown')} ; {t.get('score', 0):.4f}"
        for t in topics
    )

def format_concepts(concepts):
    """Format concepts - optimized version"""
    if not concepts:
        return ""
    
    concepts_by_level = {}
    for concept in concepts:
        level = concept.get("level", 0)
        if level not in concepts_by_level:
            concepts_by_level[level] = []
        concepts_by_level[level].append(
            f"{concept.get('display_name', 'Unknown')} ; {concept.get('score', 0):.4f} (level {level})"
        )
    
    return " | ".join(
        item for level in sorted(concepts_by_level.keys())
        for item in concepts_by_level[level]
    )

def format_sdgs(sdgs):
    """Format SDGs"""
    if not sdgs:
        return ""
    
    return " | ".join(
        f"{sdg.get('display_name', 'Unknown')} ; {sdg.get('score', 0):.2f}"
        for sdg in sdgs
    )

def format_grants(grants):
    """Format grants"""
    if not grants:
        return ""
    
    formatted = []
    for grant in grants:
        funder = grant.get("funder_display_name", "Unknown")
        award_id = grant.get("award_id", "")
        formatted.append(f"{funder} ({award_id})" if award_id else funder)
    
    return ", ".join(formatted)

def process_publications_batch(results, institution_id, institution_name, selected_metadata):
    """Process publications in batches for better memory management"""
    publications = []
    
    for pub in results:
        pub_data = {"institutions_extracted": institution_name}
        
        for field in selected_metadata:
            if field == "id":
                value = OPENALEX_PATTERN.sub('', pub.get("id", ""))
            elif field == "doi":
                doi = pub.get("doi", "")
                value = DOI_PATTERN.sub('', doi) if doi else ""
            elif field == "display_name":
                # Clean title to remove line breaks
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
                # Clean source-related fields
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
                # Clean any string value to remove line breaks
                if isinstance(value, str):
                    value = clean_text_field(value)
                elif value is not None and not isinstance(value, str):
                    value = str(value)
            
            pub_data[field] = value if value is not None else ""
        
        publications.append(pub_data)
    
    return publications

def fetch_single_doc_type(session, url, base_filter_str, doc_type, institution_id, institution_name, selected_metadata):
    """Fetch publications for a single document type"""
    filter_str = f"{base_filter_str},type:{doc_type}" if doc_type else base_filter_str
    params = {
        "filter": filter_str,
        "per_page": 50,
        "mailto": MAILTO
    }
    
    publications = []
    response = rate_limited_get(session, url, params=params)
    
    if not response:
        st.session_state.progress_data['failed_requests'] += 1
        return publications
    
    if response.status_code != 200:
        st.session_state.progress_data['failed_requests'] += 1
        return publications
    
    st.session_state.progress_data['successful_requests'] += 1
    
    try:
        data = response.json()
        total_results = data.get("meta", {}).get("count", 0)
        per_page = data.get("meta", {}).get("per_page", 50)
        total_pages = (total_results + per_page - 1) // per_page
        
        # Process first page
        publications.extend(process_publications_batch(
            data.get("results", []), institution_id, institution_name, selected_metadata
        ))
        
        # Get remaining pages
        for page in range(2, min(total_pages + 1, 200)):  # Limit to 200 pages per doc type
            params["page"] = page
            response = rate_limited_get(session, url, params=params)
            
            if response and response.status_code == 200:
                st.session_state.progress_data['successful_requests'] += 1
                data = response.json()
                publications.extend(process_publications_batch(
                    data.get("results", []), institution_id, institution_name, selected_metadata
                ))
            else:
                st.session_state.progress_data['failed_requests'] += 1
                
            # Memory management for large datasets
            if len(publications) % 5000 == 0:
                gc.collect()
    except Exception as e:
        st.error(f"Error processing publications: {e}")
        st.session_state.progress_data['failed_requests'] += 1
    
    return publications

def fetch_publications_parallel(session, institution_id, institution_name, start_year, end_year, 
                              doc_types, selected_metadata, language_filter):
    """Fetch publications using parallel requests for different document types"""
    if institution_id.startswith("https://openalex.org/"):
        institution_id = institution_id.split("/")[-1]
    
    institution_id = institution_id.lower()
    url = "https://api.openalex.org/works"
    
    base_filter_parts = [
        f"authorships.institutions.id:{institution_id}",
        f"publication_year:{start_year}-{end_year}"
    ]
    
    if language_filter == "english_only":
        base_filter_parts.append("language:en")
    
    base_filter_str = ",".join(base_filter_parts)
    
    all_publications = []
    
    if not doc_types:  # All Works mode - single request
        publications = fetch_single_doc_type(
            session, url, base_filter_str, None, institution_id, institution_name, selected_metadata
        )
        all_publications.extend(publications)
    else:
        # Use ThreadPoolExecutor for parallel requests
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(doc_types))) as executor:
            futures = []
            for doc_type in doc_types:
                future = executor.submit(
                    fetch_single_doc_type, session, url, base_filter_str, 
                    doc_type, institution_id, institution_name, selected_metadata
                )
                futures.append(future)
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    publications = future.result()
                    all_publications.extend(publications)
                except Exception as e:
                    st.error(f"Error in parallel fetch: {e}")
    
    # Remove duplicates within institution
    unique_publications = {}
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id and pub_id not in unique_publications:
            unique_publications[pub_id] = pub
    
    return list(unique_publications.values())

def deduplicate_publications_optimized(all_publications):
    """Optimized deduplication using sets and single pass"""
    seen = {}
    
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id:
            if pub_id not in seen:
                seen[pub_id] = {
                    'data': pub,
                    'institutions': {pub.get("institutions_extracted", "")}
                }
            else:
                inst = pub.get("institutions_extracted", "")
                if inst:
                    seen[pub_id]['institutions'].add(inst)
    
    # Build final list with merged institutions
    result = []
    for item in seen.values():
        pub_data = item['data'].copy()
        pub_data['institutions_extracted'] = ' | '.join(sorted(item['institutions']))
        result.append(pub_data)
    
    return result

def write_csv_streaming(df, buffer, chunk_size=10000):
    """Write CSV in chunks for better memory management with proper escaping"""
    # Clean all string columns to remove line breaks
    for col in df.columns:
        if df[col].dtype == 'object':  # String columns
            df[col] = df[col].apply(lambda x: clean_text_field(x) if isinstance(x, str) else x)
    
    # Write with proper quoting to handle special characters
    df.to_csv(buffer, index=False, encoding='utf-8-sig', 
              quoting=1,  # Quote all non-numeric fields
              lineterminator='\n',  # Ensure consistent line endings
              escapechar='\\')  # Escape special characters

def update_doc_types_callback(select_all):
    """Callback for document type selection"""
    st.session_state.doc_types_state = {
        doc_type: select_all for doc_type in DOCUMENT_TYPES
    }

def update_metadata_callback(select_all):
    """Callback for metadata selection"""
    st.session_state.metadata_state = {
        field: (select_all or field == "id") for field in METADATA_FIELDS.keys()
    }

def main():
    st.set_page_config(
        page_title="OpenAlex Institution Publications Retriever",
        page_icon="📚",
        layout="wide"
    )
    
    st.title("📚 OpenAlex Institution Publications Retriever")
    st.markdown(f"*SIRIS Academic Research Tool - Contact: {MAILTO}*")
    
    # Load institutions data
    institutions_df = load_institutions()
    
    if institutions_df is None:
        st.error("Could not load institutions data. Please ensure 'institutions_master.parquet' is in the app directory.")
        return
    
    # Phase 1: Institution Selection
    st.header("1️⃣ Select Institutions")
    
    # Search input
    search_query = st.text_input("Search institutions by name, acronym, alternative names, or city:", 
                                placeholder="Type at least 2 characters to search...")
    
    # Full width layout for results
    if search_query:
        results_df = search_institutions_cached(institutions_df, search_query)
        
        if not results_df.empty:
            st.info(f"📊 Found {len(results_df)} matching institutions. Select up to {10 - len(st.session_state.selected_institutions)} more institutions.")
            
            # Use st.data_editor for interactive selection
            edited_df = st.data_editor(
                results_df.drop(columns=['openalex_id']),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select",
                        help="Check to add institution to selection",
                        default=False,
                        width="small"
                    ),
                    "Name": st.column_config.TextColumn(
                        "Institution Name",
                        width="large"
                    ),
                    "ROR": st.column_config.LinkColumn(
                        "ROR",
                        width="medium"
                    ),
                    "Total Works": st.column_config.NumberColumn(
                        "Total Works",
                        format="%d",
                        width="small"
                    ),
                    "Avg. Works/Year": st.column_config.NumberColumn(
                        "Avg. Works/Year",
                        format="%.1f",
                        width="small"
                    )
                },
                disabled=["Name", "Acronym", "Type", "Country", "City", "ROR", "Total Works", "Avg. Works/Year"],
                key="institution_selector"
            )
            
            # Add selected institutions button
            col1, col2 = st.columns([1, 5])
            with col1:
                if st.button("➕ Add Selected", type="primary"):
                    selected_rows = edited_df[edited_df['Select'] == True]
                    
                    if not selected_rows.empty:
                        added_count = 0
                        for idx, row in selected_rows.iterrows():
                            inst_data = {
                                'openalex_id': results_df.loc[idx, 'openalex_id'],
                                'display_name': row['Name']
                            }
                            
                            if len(st.session_state.selected_institutions) < 10:
                                if not any(i['openalex_id'] == inst_data['openalex_id'] 
                                         for i in st.session_state.selected_institutions):
                                    st.session_state.selected_institutions.append(inst_data)
                                    added_count += 1
                        
                        if added_count > 0:
                            st.success(f"Added {added_count} institution(s)")
                            st.rerun()
                        else:
                            st.warning("All selected institutions are already in your list or limit reached")
                    else:
                        st.warning("Please select at least one institution")
        else:
            st.info("No institutions found matching your search")
    
    # Display selected institutions
    st.divider()
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"📋 Selected Institutions ({len(st.session_state.selected_institutions)}/10)")
    with col3:
        if st.session_state.selected_institutions and st.button("🗑️ Clear All"):
            st.session_state.selected_institutions = []
            st.rerun()
    
    if st.session_state.selected_institutions:
        # Calculate total estimated publications per year
        total_avg_works = sum(
            institutions_df[institutions_df['openalex_id'] == inst['openalex_id']]['avg_works_per_year_2021_2023'].iloc[0]
            for inst in st.session_state.selected_institutions
            if not institutions_df[institutions_df['openalex_id'] == inst['openalex_id']].empty
        )
        
        # Display estimation
        inst_text = "institution produces" if len(st.session_state.selected_institutions) == 1 else "institutions produce"
        st.markdown(
            f"<p style='color: red;'>{'This' if len(st.session_state.selected_institutions) == 1 else 'These'} "
            f"{inst_text} an estimation of <b>{total_avg_works:.0f}</b> publications per year (all document types).</p>",
            unsafe_allow_html=True
        )
        
        st.session_state.total_avg_works_per_year = total_avg_works
        
        # Display selected institutions
        for i, inst in enumerate(st.session_state.selected_institutions):
            col1, col2 = st.columns([11, 1])
            with col1:
                st.write(f"**{i+1}.** {inst['display_name']}")
            with col2:
                if st.button("❌", key=f"remove_{i}", help=f"Remove {inst['display_name']}"):
                    st.session_state.selected_institutions.pop(i)
                    st.rerun()
    else:
        st.info("No institutions selected yet. Search and select institutions above.")
        st.session_state.total_avg_works_per_year = 0
    
    if not st.session_state.selected_institutions:
        st.warning("Please select at least one institution to continue")
        return
    
    st.divider()
    
    # Phase 2: Configure Retrieval Parameters
    st.header("2️⃣ Configure Retrieval Parameters")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_year = st.number_input("Start Year", min_value=1970, max_value=CURRENT_YEAR, value=CURRENT_YEAR-5)
    with col2:
        end_year = st.number_input("End Year", min_value=1970, max_value=CURRENT_YEAR, value=CURRENT_YEAR)
    with col3:
        language_filter = st.radio("Language Filter", ["All Languages", "English Only"])
    with col4:
        output_format = st.radio("Output Format", ["CSV", "Parquet"], 
                                help="Parquet format provides better compression for large files")
    
    # Document Types
    st.subheader("Document Types")
    
    if 'doc_types_state' not in st.session_state:
        st.session_state.doc_types_state = {doc_type: True for doc_type in DOCUMENT_TYPES}
        st.session_state.all_works = True
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        all_works = st.checkbox("All Works (no document type filtering - faster)", value=st.session_state.all_works)
    with col2:
        st.button("Select All", key="select_all_docs", disabled=all_works,
                 on_click=update_doc_types_callback, args=(True,))
    with col3:
        st.button("Unselect All", key="unselect_all_docs", disabled=all_works,
                 on_click=update_doc_types_callback, args=(False,))
    
    if all_works != st.session_state.all_works:
        st.session_state.all_works = all_works
        if all_works:
            st.session_state.doc_types_state = {doc_type: True for doc_type in DOCUMENT_TYPES}
    
    doc_cols = st.columns(5)
    selected_doc_types = []
    
    for i, doc_type in enumerate(DOCUMENT_TYPES):
        with doc_cols[i % 5]:
            if all_works:
                st.checkbox(doc_type, value=True, disabled=True, key=f"doc_{doc_type}")
            else:
                checked = st.checkbox(doc_type, value=st.session_state.doc_types_state.get(doc_type, False), 
                                    key=f"doc_{doc_type}")
                st.session_state.doc_types_state[doc_type] = checked
                if checked:
                    selected_doc_types.append(doc_type)
    
    if all_works:
        selected_doc_types = []
    
    # Metadata Fields
    st.subheader("Metadata Fields")
    
    if 'metadata_state' not in st.session_state:
        st.session_state.metadata_state = {field: True for field in METADATA_FIELDS.keys()}
        st.session_state.all_metadata = True
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        all_metadata = st.checkbox("All Metadata Fields", value=st.session_state.all_metadata)
    with col2:
        st.button("Select All", key="select_all_meta", disabled=all_metadata,
                 on_click=update_metadata_callback, args=(True,))
    with col3:
        st.button("Unselect All", key="unselect_all_meta", disabled=all_metadata,
                 on_click=update_metadata_callback, args=(False,))
    
    if all_metadata != st.session_state.all_metadata:
        st.session_state.all_metadata = all_metadata
        if all_metadata:
            st.session_state.metadata_state = {field: True for field in METADATA_FIELDS.keys()}
    
    metadata_categories = {
        "Core Publication Information": ["id", "doi", "display_name", "publication_year", "publication_date", 
                                        "language", "type", "abstract_inverted_index", "has_fulltext", "is_retracted"],
        "Access Information": ["open_access.is_oa", "open_access.oa_status", "apc_paid.value_usd"],
        "Publication Source": ["primary_location.source.display_name", "primary_location.source.type", 
                              "primary_location.source.issn", "primary_location.source.host_organization_name",
                              "primary_location.pdf_url", "primary_location.license"],
        "Authorship Information": ["authorships", "institutions", "raw_affiliation_strings", "countries_distinct_count",
                                  "institutions_distinct_count", "corresponding_author_ids", "corresponding_institution_ids"],
        "Impact Metrics": ["fwci", "cited_by_count", "citation_normalized_percentile.value",
                          "citation_normalized_percentile.is_in_top_1_percent", 
                          "citation_normalized_percentile.is_in_top_10_percent", "counts_by_year"],
        "Classification Information": ["primary_topic_and_score", "primary_topic.subfield.display_name", 
                                      "primary_topic.field.display_name", "primary_topic.domain.display_name",
                                      "topics", "concepts"],
        "Additional Information": ["sustainable_development_goals", "grants", "datasets"]
    }
    
    selected_metadata = ["id"]
    
    for category, fields in metadata_categories.items():
        with st.expander(category, expanded=True):
            field_cols = st.columns(3)
            for i, field in enumerate(fields):
                with field_cols[i % 3]:
                    if field == "id":
                        st.checkbox(METADATA_FIELDS.get(field, field), value=True, disabled=True, key=f"meta_{field}")
                    elif all_metadata:
                        st.checkbox(METADATA_FIELDS.get(field, field), value=True, disabled=True, key=f"meta_{field}")
                        selected_metadata.append(field)
                    else:
                        if st.checkbox(METADATA_FIELDS.get(field, field), 
                                     value=st.session_state.metadata_state.get(field, False), 
                                     key=f"meta_{field}"):
                            selected_metadata.append(field)
                            st.session_state.metadata_state[field] = True
                        else:
                            st.session_state.metadata_state[field] = False
    
    st.divider()
    
    # Phase 3: Retrieve Publications
    st.header("3️⃣ Retrieve Publications")
    
    # Show warning if needed
    if hasattr(st.session_state, 'total_avg_works_per_year') and st.session_state.total_avg_works_per_year > 0:
        years_span = end_year - start_year + 1
        estimated_publications = st.session_state.total_avg_works_per_year * years_span
        
        if estimated_publications > 100000:
            st.markdown(
                f"<div style='background-color: #ffebee; padding: 10px; border-radius: 5px; border-left: 3px solid red;'>"
                f"<p style='color: red; margin: 0;'><b>⚠️ Warning!</b> The file might contain more than <b>{estimated_publications:,.0f}</b> publications "
                f"and exceed 200 MB. Consider:</p>"
                f"<ul style='color: red; margin: 5px 0;'>"
                f"<li>Filtering document types</li>"
                f"<li>Selecting less metadata</li>"
                f"<li>Removing abstracts (can save up to 40% of space)</li>"
                f"<li>Choosing Parquet format for better compression</li>"
                f"</ul></div>",
                unsafe_allow_html=True
            )
            st.markdown("")
    
    if st.button("🚀 Start Retrieval", type="primary"):
        # Start timing
        start_time = time.time()
        
        # Initialize progress tracking
        st.session_state.progress_data = {
            'current_institution': 0,
            'total_institutions': len(st.session_state.selected_institutions),
            'successful_requests': 0,
            'failed_requests': 0,
            'publications_fetched': 0
        }
        
        # Get cached session
        session = get_session()
        
        # Progress placeholders
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        metrics_placeholder = st.empty()
        timer_placeholder = st.empty()
        
        all_publications = []
        
        # Process each institution with progress updates
        for i, inst in enumerate(st.session_state.selected_institutions):
            st.session_state.progress_data['current_institution'] = i + 1
            
            # Update progress and timer
            progress = (i + 1) / len(st.session_state.selected_institutions)
            progress_bar.progress(progress)
            
            # Update timer - show elapsed time
            elapsed = time.time() - start_time
            timer_placeholder.info(f"⏱️ Time elapsed: {str(timedelta(seconds=int(elapsed)))}")
            
            status_placeholder.info(f"Fetching: {inst['display_name']}")
            
            # Fetch publications with parallel requests
            lang_filter = "english_only" if language_filter == "English Only" else "all_languages"
            
            institution_pubs = fetch_publications_parallel(
                session, 
                inst['openalex_id'], 
                inst['display_name'],
                start_year, 
                end_year, 
                selected_doc_types,
                selected_metadata,
                lang_filter
            )
            
            all_publications.extend(institution_pubs)
            
            # Update metrics
            metrics_placeholder.metric(
                label="Progress",
                value=f"{i+1}/{len(st.session_state.selected_institutions)} institutions",
                delta=f"{len(all_publications)} publications fetched (before deduplication)"
            )
            
            # Memory management
            if len(all_publications) > 10000 and i % 3 == 0:
                gc.collect()
        
        # Deduplication
        if all_publications:
            status_placeholder.info("Deduplicating publications and merging institution names...")
            merged_publications = deduplicate_publications_optimized(all_publications)
            
            # Clear memory
            del all_publications
            gc.collect()
            
            # Update final metrics
            metrics_placeholder.metric(
                label="Final Results",
                value=f"{len(merged_publications)} unique publications",
                delta=f"Processing complete"
            )
            
            # Create output dataframe
            df_output = pd.DataFrame(merged_publications)
            
            # Clean all text fields in the dataframe to remove line breaks
            for col in df_output.columns:
                if df_output[col].dtype == 'object':  # String columns
                    df_output[col] = df_output[col].apply(lambda x: clean_text_field(x) if isinstance(x, str) else x)
            
            # Reorder columns
            columns_order = ["id"] + [col for col in selected_metadata if col != "id"] + ["institutions_extracted"]
            df_output = df_output[[col for col in columns_order if col in df_output.columns]]
            
            # Rename columns
            column_mapping = {field: METADATA_FIELDS.get(field, field) for field in df_output.columns}
            column_mapping["institutions_extracted"] = "Institutions Extracted"
            df_output = df_output.rename(columns=column_mapping)
            
            # Generate filename
            num_institutions = len(st.session_state.selected_institutions)
            timestamp = datetime.now().strftime("%H%M")
            
            # Calculate total time
            total_time = time.time() - start_time
            timer_placeholder.success(f"✅ Total processing time: {str(timedelta(seconds=int(total_time)))}")
            
            # Get API call statistics
            total_api_calls = st.session_state.progress_data.get('successful_requests', 0) + st.session_state.progress_data.get('failed_requests', 0)
            successful_calls = st.session_state.progress_data.get('successful_requests', 0)
            failed_calls = st.session_state.progress_data.get('failed_requests', 0)
            
            if output_format == "CSV":
                filename = f"pubs_{num_institutions}_institutions_{timestamp}.csv"
                
                # Use streaming for large files
                csv_buffer = io.StringIO()
                if len(df_output) > 50000:
                    write_csv_streaming(df_output, csv_buffer)
                else:
                    df_output.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                
                csv_data = csv_buffer.getvalue().encode('utf-8-sig')
                
                # Success message with detailed statistics
                st.success(
                    f"✅ Retrieved {len(merged_publications)} unique publications from {num_institutions} institutions\n\n"
                    f"**Time elapsed:** {str(timedelta(seconds=int(total_time)))}\n\n"
                    f"**API calls:** {successful_calls} successful, {failed_calls} failed (Total: {total_api_calls}/100,000 daily limit)"
                )
                
                st.download_button(
                    label=f"📥 Download {filename}",
                    data=csv_data,
                    file_name=filename,
                    mime="text/csv",
                    type="primary"
                )
            else:  # Parquet format
                filename = f"pubs_{num_institutions}_institutions_{timestamp}.parquet"
                
                parquet_buffer = io.BytesIO()
                df_output.to_parquet(parquet_buffer, index=False, compression='snappy')
                parquet_data = parquet_buffer.getvalue()
                
                file_size_mb = len(parquet_data) / (1024 * 1024)
                
                # Success message with detailed statistics
                st.success(
                    f"✅ Retrieved {len(merged_publications)} unique publications from {num_institutions} institutions\n\n"
                    f"**Time elapsed:** {str(timedelta(seconds=int(total_time)))}\n\n"
                    f"**API calls:** {successful_calls} successful, {failed_calls} failed (Total: {total_api_calls}/100,000 daily limit)\n\n"
                    f"**File size:** {file_size_mb:.1f} MB (Parquet compression)"
                )
                
                st.download_button(
                    label=f"📥 Download {filename}",
                    data=parquet_data,
                    file_name=filename,
                    mime="application/octet-stream",
                    type="primary"
                )
            
            # Final cleanup
            del df_output
            gc.collect()
        else:
            st.warning("No publications found for the selected criteria")

if __name__ == "__main__":
    main()