import streamlit as st
import pandas as pd
import requests
import time
import unicodedata
from datetime import datetime
import io
import threading

# Constants
MAILTO = "theodore.hervieux@sirisacademic.com"
PUBLICATIONS_DELAY = 0.1  # 100ms between requests
RETRY_AFTER_429 = 2
CURRENT_YEAR = datetime.now().year

# Document types from OpenAlex
DOCUMENT_TYPES = [
    "article", "book-chapter", "dataset", "preprint", "dissertation", "book", 
    "review", "paratext", "libguides", "letter", "other", "reference-entry", 
    "report", "editorial", "peer-review", "erratum", "standard", "grant",
    "supplementary-materials", "retraction"
]

# Metadata fields
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
    "primary_topic.display_name": "Primary Topic",
    "primary_topic.score": "Primary Topic Score",
    "primary_topic.subfield.display_name": "Primary Subfield",
    "primary_topic.field.display_name": "Primary Field", 
    "primary_topic.domain.display_name": "Primary Domain",
    "topics": "Topics",
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

@st.cache_data
def load_institutions():
    """Load institutions from parquet file"""
    try:
        df = pd.read_parquet('institutions_master.parquet')
        return df
    except Exception as e:
        st.error(f"Error loading institutions file: {e}")
        return None

def search_institutions(df, query):
    """Search institutions by name, alternatives, acronyms, and city"""
    if not query or len(query) < 2:
        return pd.DataFrame()
    
    query_lower = query.lower()
    
    # Search in display_name, alternatives, acronyms, AND city
    mask = (
        df['display_name'].str.lower().str.contains(query_lower, na=False) |
        df['display_name_alternatives'].str.lower().str.contains(query_lower, na=False) |
        df['display_name_acronyms'].str.lower().str.contains(query_lower, na=False) |
        df['city'].str.lower().str.contains(query_lower, na=False)
    )
    
    results = df[mask].head(30)  # Increased to 30 results for better selection
    
    # Prepare display dataframe with Select column
    display_df = pd.DataFrame({
        'Select': False,  # Checkbox column
        'Name': results['display_name'],
        'Acronym': results['display_name_acronyms'],
        'Type': results['type'],
        'Country': results['country_code'],
        'City': results['city'],
        'ROR': results['ror_id'].apply(lambda x: f"https://ror.org/{x}" if x else ""),
        'Total Works': results['total_works_count'],
        'Avg. Works/Year': results['avg_works_per_year_2021_2023'],
        'openalex_id': results['openalex_id']  # Keep for selection
    })
    
    return display_df

def rate_limited_get(session, url, params=None, max_retries=3):
    """Rate-limited API request"""
    retry_count = 0
    backoff_time = RETRY_AFTER_429
    
    while retry_count <= max_retries:
        # Apply rate limiting
        now = time.time()
        elapsed = now - st.session_state.last_request_time
        if elapsed < PUBLICATIONS_DELAY:
            time.sleep(PUBLICATIONS_DELAY - elapsed)
        st.session_state.last_request_time = time.time()
        
        response = session.get(url, params=params)
        
        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            retry_count += 1
            if retry_count > max_retries:
                return response
            wait_time = backoff_time * (0.5 + 0.5)
            time.sleep(wait_time)
            backoff_time *= 2
        else:
            return response
    
    return response

def get_value_from_nested_dict(data, key_path):
    """Extract value from nested dictionary"""
    if not data or not key_path:
        return None
    
    keys = key_path.split(".")
    value = data
    
    try:
        for key in keys:
            if value is None:
                return None
            value = value.get(key)
        return value
    except (AttributeError, KeyError):
        return None

def format_abstract(inverted_index):
    """Reconstruct abstract from inverted index"""
    if not inverted_index:
        return ""
    
    word_positions = []
    try:
        for word, positions in inverted_index.items():
            for pos in positions:
                word_positions.append((pos, word))
        
        sorted_words = [word for _, word in sorted(word_positions)]
        return " ".join(sorted_words)
    except Exception:
        return "[Abstract processing error]"

def format_authors_simple(authorships):
    """Format authors information"""
    if not authorships:
        return ""
    
    authors = []
    for authorship in authorships:
        author_data = authorship.get("author", {})
        author_name = author_data.get("display_name", "Unknown")
        author_name = unicodedata.normalize('NFC', author_name.strip())
        
        if authorship.get("is_corresponding", False):
            author_name += " (corresponding)"
        
        authors.append(author_name)
    
    return " | ".join(authors)

def format_institutions(authorships):
    """Format institutions information"""
    if not authorships:
        return ""
    
    unique_institutions = {}
    institution_order = []
    
    for authorship in authorships:
        institutions = authorship.get("institutions", [])
        for inst in institutions:
            inst_id = inst.get("id", "")
            if inst_id and inst_id not in unique_institutions:
                inst_name = inst.get("display_name", "Unknown")
                inst_type = inst.get("type", "Unknown")
                inst_country = inst.get("country_code", "Unknown")
                inst_id_clean = inst_id.replace("https://openalex.org/", "")
                
                formatted_inst = f"{inst_name}, {inst_type}, {inst_country} ({inst_id_clean})"
                unique_institutions[inst_id] = formatted_inst
                institution_order.append(inst_id)
    
    return " | ".join([unique_institutions[inst_id] for inst_id in institution_order])

def format_raw_affiliation_strings(authorships):
    """Format raw affiliation strings"""
    if not authorships:
        return ""
    
    all_affiliations = []
    for authorship in authorships:
        raw_affiliations = authorship.get("raw_affiliation_strings", [])
        for affiliation in raw_affiliations:
            if affiliation and affiliation.strip():
                clean_affiliation = unicodedata.normalize('NFC', affiliation.strip())
                if clean_affiliation not in all_affiliations:
                    all_affiliations.append(clean_affiliation)
    
    return " | ".join(all_affiliations)

def format_counts_by_year(counts):
    """Format counts by year"""
    if not counts:
        return ""
    
    sorted_counts = sorted(counts, key=lambda x: x.get("year", 0), reverse=True)
    formatted_counts = []
    for count in sorted_counts:
        year = count.get("year", "Unknown")
        cited_by_count = count.get("cited_by_count", 0)
        formatted_counts.append(f"{cited_by_count} ({year})")
    
    return " | ".join(formatted_counts)

def format_topic_and_score(topics):
    """Format topics with scores"""
    if not topics:
        return ""
    
    formatted_topics = []
    for topic in topics:
        display_name = topic.get("display_name", "Unknown")
        score = topic.get("score", 0)
        formatted_topics.append(f"{display_name}, {score:.4f}")
    
    return " | ".join(formatted_topics)

def format_concepts(concepts):
    """Format concepts by level"""
    if not concepts:
        return ""
    
    concepts_by_level = {}
    for concept in concepts:
        level = concept.get("level", 0)
        if level not in concepts_by_level:
            concepts_by_level[level] = []
        
        display_name = concept.get("display_name", "Unknown")
        score = concept.get("score", 0)
        concepts_by_level[level].append(f"{display_name}, {score:.4f} (level {level})")
    
    formatted_concepts = []
    for level in sorted(concepts_by_level.keys()):
        formatted_concepts.extend(concepts_by_level[level])
    
    return " | ".join(formatted_concepts)

def format_sdgs(sdgs):
    """Format SDGs"""
    if not sdgs:
        return ""
    
    formatted_sdgs = []
    for sdg in sdgs:
        display_name = sdg.get("display_name", "Unknown")
        score = sdg.get("score", 0)
        formatted_sdgs.append(f"{display_name}, {score:.2f}")
    
    return " | ".join(formatted_sdgs)

def format_grants(grants):
    """Format grants"""
    if not grants:
        return ""
    
    formatted_grants = []
    for grant in grants:
        funder = grant.get("funder_display_name", "Unknown")
        award_id = grant.get("award_id", "")
        
        if award_id:
            formatted_grants.append(f"{funder} ({award_id})")
        else:
            formatted_grants.append(funder)
    
    return ", ".join(formatted_grants)

def process_publications(results, institution_id, institution_name, selected_metadata):
    """Process publication results"""
    publications = []
    
    for pub in results:
        pub_data = {"institutions_extracted": institution_name}
        
        for field in selected_metadata:
            if field == "id":
                value = pub.get("id", "").replace("https://openalex.org/", "")
            elif field == "doi":
                doi = pub.get("doi", "")
                value = doi.replace("https://doi.org/", "") if doi else ""
            elif field == "abstract_inverted_index":
                value = format_abstract(pub.get("abstract_inverted_index", {}))
            elif field == "authorships":
                value = format_authors_simple(pub.get("authorships", []))
            elif field == "institutions":
                value = format_institutions(pub.get("authorships", []))
            elif field == "raw_affiliation_strings":
                value = format_raw_affiliation_strings(pub.get("authorships", []))
            elif field.startswith("primary_location.source.issn"):
                issns = get_value_from_nested_dict(pub, "primary_location.source.issn") or []
                value = ",".join(issns)
            elif field == "corresponding_author_ids":
                ids = pub.get("corresponding_author_ids", [])
                value = " | ".join([id.replace("https://openalex.org/", "") for id in ids])
            elif field == "corresponding_institution_ids":
                ids = pub.get("corresponding_institution_ids", [])
                value = " | ".join([id.replace("https://openalex.org/", "") for id in ids])
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
                datasets = pub.get("datasets", [])
                value = ", ".join(datasets)
            else:
                value = get_value_from_nested_dict(pub, field)
                if value is not None and not isinstance(value, str):
                    value = str(value)
            
            pub_data[field] = value if value is not None else ""
        
        publications.append(pub_data)
    
    return publications

def fetch_with_pagination(session, url, params, institution_id, institution_name, selected_metadata, progress_placeholder):
    """Fetch paginated results"""
    publications = []
    
    try:
        response = rate_limited_get(session, url, params=params)
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        total_results = data.get("meta", {}).get("count", 0)
        per_page = data.get("meta", {}).get("per_page", 50)
        total_pages = (total_results // per_page) + (1 if total_results % per_page > 0 else 0)
        
        # Update progress
        st.session_state.progress_data['successful_requests'] += 1
        
        # Process first page
        publications.extend(process_publications(data.get("results", []), institution_id, institution_name, selected_metadata))
        
        # Get remaining pages
        for page in range(2, total_pages + 1):
            params["page"] = page
            response = rate_limited_get(session, url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                publications.extend(process_publications(data.get("results", []), institution_id, institution_name, selected_metadata))
                st.session_state.progress_data['successful_requests'] += 1
            else:
                break
                
    except Exception as e:
        st.error(f"Error fetching publications: {e}")
    
    return publications

def fetch_publications(session, institution_id, institution_name, start_year, end_year, doc_types, selected_metadata, language_filter, progress_placeholder):
    """Fetch publications for an institution"""
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
    
    if not doc_types:  # All Works mode
        params = {
            "filter": base_filter_str,
            "per_page": 50,
            "mailto": MAILTO
        }
        all_publications.extend(fetch_with_pagination(session, url, params, institution_id, institution_name, selected_metadata, progress_placeholder))
    else:
        for doc_type in doc_types:
            filter_str = f"{base_filter_str},type:{doc_type}"
            params = {
                "filter": filter_str,
                "per_page": 50,
                "mailto": MAILTO
            }
            type_publications = fetch_with_pagination(session, url, params, institution_id, institution_name, selected_metadata, progress_placeholder)
            all_publications.extend(type_publications)
    
    # Remove duplicates
    unique_publications = {}
    for pub in all_publications:
        pub_id = pub.get("id", "")
        if pub_id and pub_id not in unique_publications:
            unique_publications[pub_id] = pub
    
    return list(unique_publications.values())

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
        results_df = search_institutions(institutions_df, search_query)
        
        if not results_df.empty:
            st.info(f"📊 Found {len(results_df)} matching institutions. Select up to {10 - len(st.session_state.selected_institutions)} more institutions.")
            
            # Use st.data_editor for interactive selection with checkboxes
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
                    # Get selected rows
                    selected_rows = edited_df[edited_df['Select'] == True]
                    
                    if not selected_rows.empty:
                        added_count = 0
                        for idx, row in selected_rows.iterrows():
                            inst_data = {
                                'openalex_id': results_df.loc[idx, 'openalex_id'],
                                'display_name': row['Name']
                            }
                            
                            # Check if not already added and limit not reached
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
    
    # Display selected institutions in a more compact sidebar-like section
    st.divider()
    
    # Selected institutions display
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"📋 Selected Institutions ({len(st.session_state.selected_institutions)}/10)")
    with col3:
        if st.session_state.selected_institutions and st.button("🗑️ Clear All"):
            st.session_state.selected_institutions = []
            st.rerun()
    
    if st.session_state.selected_institutions:
        # Display selected institutions in a clean format
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
    
    if not st.session_state.selected_institutions:
        st.warning("Please select at least one institution to continue")
        return
    
    st.divider()
    
    # Phase 2: Configure Retrieval Parameters
    st.header("2️⃣ Configure Retrieval Parameters")
    
    # Timeframe
    col1, col2, col3 = st.columns(3)
    with col1:
        start_year = st.number_input("Start Year", min_value=1970, max_value=CURRENT_YEAR, value=CURRENT_YEAR-5)
    with col2:
        end_year = st.number_input("End Year", min_value=1970, max_value=CURRENT_YEAR, value=CURRENT_YEAR)
    with col3:
        language_filter = st.radio("Language Filter", ["All Languages", "English Only"])
    
    # Document Types
    st.subheader("Document Types")
    all_works = st.checkbox("All Works (no document type filtering - faster)")
    
    if not all_works:
        doc_cols = st.columns(5)
        selected_doc_types = []
        for i, doc_type in enumerate(DOCUMENT_TYPES):
            with doc_cols[i % 5]:
                if st.checkbox(doc_type, value=(doc_type == "article")):
                    selected_doc_types.append(doc_type)
    else:
        selected_doc_types = []
    
    # Metadata Fields
    st.subheader("Metadata Fields")
    
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
        "Classification Information": ["primary_topic.display_name", "primary_topic.score", 
                                      "primary_topic.subfield.display_name", "primary_topic.field.display_name",
                                      "primary_topic.domain.display_name", "topics", "concepts"],
        "Additional Information": ["sustainable_development_goals", "grants", "datasets"]
    }
    
    selected_metadata = ["id"]  # Always include ID
    
    for category, fields in metadata_categories.items():
        with st.expander(category):
            field_cols = st.columns(3)
            for i, field in enumerate(fields):
                with field_cols[i % 3]:
                    if field != "id":  # ID is always selected
                        if st.checkbox(METADATA_FIELDS.get(field, field), key=f"meta_{field}"):
                            selected_metadata.append(field)
    
    st.divider()
    
    # Phase 3: Retrieve Publications
    st.header("3️⃣ Retrieve Publications")
    
    if st.button("🚀 Start Retrieval", type="primary"):
        # Initialize progress tracking
        st.session_state.progress_data = {
            'current_institution': 0,
            'total_institutions': len(st.session_state.selected_institutions),
            'successful_requests': 0,
            'publications_fetched': 0
        }
        
        # Create session
        session = requests.Session()
        session.headers.update({
            'User-Agent': f'SIRIS Academic Research Tool/1.0 (mailto:{MAILTO})'
        })
        
        # Progress placeholders
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        metrics_placeholder = st.empty()
        
        all_publications = []
        
        # Process each institution
        for i, inst in enumerate(st.session_state.selected_institutions):
            st.session_state.progress_data['current_institution'] = i + 1
            
            # Update progress
            progress = (i + 1) / len(st.session_state.selected_institutions)
            progress_bar.progress(progress)
            
            status_placeholder.info(f"Fetching: {inst['display_name']}")
            
            # Fetch publications
            lang_filter = "english_only" if language_filter == "English Only" else "all_languages"
            
            institution_pubs = fetch_publications(
                session, 
                inst['openalex_id'], 
                inst['display_name'],
                start_year, 
                end_year, 
                selected_doc_types,
                selected_metadata,
                lang_filter,
                status_placeholder
            )
            
            all_publications.extend(institution_pubs)
            st.session_state.progress_data['publications_fetched'] = len(all_publications)
            
            # Update metrics
            metrics_placeholder.metric(
                label="Progress",
                value=f"{i+1}/{len(st.session_state.selected_institutions)} institutions",
                delta=f"{len(all_publications)} publications fetched"
            )
        
        # Merge duplicates
        if all_publications:
            pub_dict = {}
            for pub in all_publications:
                pub_id = pub.get("id", "")
                if pub_id:
                    if pub_id not in pub_dict:
                        pub_dict[pub_id] = pub
                    else:
                        existing_institutions = pub_dict[pub_id].get("institutions_extracted", "")
                        new_institution = pub.get("institutions_extracted", "")
                        if new_institution and new_institution not in existing_institutions:
                            pub_dict[pub_id]["institutions_extracted"] = f"{existing_institutions} | {new_institution}" if existing_institutions else new_institution
            
            merged_publications = list(pub_dict.values())
            
            # Create CSV
            df_output = pd.DataFrame(merged_publications)
            
            # Reorder columns to match original format
            columns_order = ["id"] + [col for col in selected_metadata if col != "id"] + ["institutions_extracted"]
            df_output = df_output[columns_order]
            
            # Rename columns to friendly names
            column_mapping = {field: METADATA_FIELDS.get(field, field) for field in df_output.columns}
            column_mapping["institutions_extracted"] = "Institutions Extracted"
            df_output = df_output.rename(columns=column_mapping)
            
            # Generate filename
            num_institutions = len(st.session_state.selected_institutions)
            timestamp = datetime.now().strftime("%H%M")
            filename = f"pubs_{num_institutions}_institutions_{timestamp}.csv"
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            df_output.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_data = csv_buffer.getvalue()
            
            # Success message and download button
            st.success(f"✅ Retrieved {len(merged_publications)} unique publications from {num_institutions} institutions")
            
            st.download_button(
                label=f"📥 Download {filename}",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                type="primary"
            )
        else:
            st.warning("No publications found for the selected criteria")

if __name__ == "__main__":
    main()