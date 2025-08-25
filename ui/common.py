"""Shared UI components for configuration and retrieval"""
import streamlit as st
import pandas as pd
import numpy as np
import time
import io
import gc
from datetime import datetime, timedelta
from typing import List, Dict

from core.api_client import get_session
from core.processors import (
    fetch_publications_parallel, deduplicate_publications_optimized,
    clean_text_field
)

# Constants
CURRENT_YEAR = datetime.now().year

DOCUMENT_TYPES = [
    "article", "book-chapter", "dataset", "preprint", "dissertation", "book", 
    "review", "paratext", "libguides", "letter", "other", "reference-entry", 
    "report", "editorial", "peer-review", "erratum", "standard", "grant",
    "supplementary-materials", "retraction"
]

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

def render_config_section():
    """Render the configuration section (Phase 2)"""
    st.header("2️⃣ Configure Retrieval Parameters")
    
    # Initialize session state for configuration
    if 'config' not in st.session_state:
        st.session_state.config = {
            'start_year': CURRENT_YEAR - 5,
            'end_year': CURRENT_YEAR,
            'language_filter': 'All Languages',
            'output_format': 'CSV',
            'doc_types': [],
            'metadata': ['id']
        }
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.session_state.config['start_year'] = st.number_input(
            "Start Year", 
            min_value=1970, 
            max_value=CURRENT_YEAR, 
            value=st.session_state.config['start_year']
        )
    with col2:
        st.session_state.config['end_year'] = st.number_input(
            "End Year", 
            min_value=1970, 
            max_value=CURRENT_YEAR, 
            value=st.session_state.config['end_year']
        )
    with col3:
        st.session_state.config['language_filter'] = st.radio(
            "Language Filter", 
            ["All Languages", "English Only"]
        )
    with col4:
        st.session_state.config['output_format'] = st.radio(
            "Output Format", 
            ["CSV", "Parquet"],
            help="Parquet format provides better compression for large files"
        )
    
    # Document Types
    st.subheader("Document Types")
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        all_works = st.checkbox("All Works (no document type filtering - faster)", value=True)
    
    if not all_works:
        with col2:
            if st.button("Select All", key="select_all_docs"):
                st.session_state.config['doc_types'] = DOCUMENT_TYPES.copy()
                st.rerun()
        with col3:
            if st.button("Unselect All", key="unselect_all_docs"):
                st.session_state.config['doc_types'] = []
                st.rerun()
        
        doc_cols = st.columns(5)
        selected_doc_types = []
        
        for i, doc_type in enumerate(DOCUMENT_TYPES):
            with doc_cols[i % 5]:
                if st.checkbox(doc_type, value=doc_type in st.session_state.config.get('doc_types', []), key=f"doc_{doc_type}"):
                    selected_doc_types.append(doc_type)
        
        st.session_state.config['doc_types'] = selected_doc_types
    else:
        st.session_state.config['doc_types'] = []
    
    # Metadata Fields
    st.subheader("Metadata Fields")
    
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        all_metadata = st.checkbox("All Metadata Fields", value=True)
    
    selected_metadata = ["id"]  # Always include ID
    
    if not all_metadata:
        with col2:
            if st.button("Select All", key="select_all_meta"):
                st.session_state.config['metadata'] = list(METADATA_FIELDS.keys())
                st.rerun()
        with col3:
            if st.button("Unselect All", key="unselect_all_meta"):
                st.session_state.config['metadata'] = ["id"]
                st.rerun()
        
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
        
        for category, fields in metadata_categories.items():
            with st.expander(category, expanded=True):
                field_cols = st.columns(3)
                for i, field in enumerate(fields):
                    with field_cols[i % 3]:
                        if field == "id":
                            st.checkbox(METADATA_FIELDS.get(field, field), value=True, disabled=True, key=f"meta_{field}")
                        else:
                            if st.checkbox(
                                METADATA_FIELDS.get(field, field), 
                                value=field in st.session_state.config.get('metadata', []),
                                key=f"meta_{field}"
                            ):
                                if field not in selected_metadata:
                                    selected_metadata.append(field)
        
        st.session_state.config['metadata'] = selected_metadata
    else:
        st.session_state.config['metadata'] = list(METADATA_FIELDS.keys())

def render_retrieval_section():
    """Render the retrieval section (Phase 3)"""
    st.header("3️⃣ Retrieve Publications")
    
    # Check if we have entities selected
    if not st.session_state.selected_entities:
        st.warning("Please select at least one institution or author to retrieve publications")
        return
    
    # Display warning for large retrievals
    if st.session_state.selection_mode == "institutions":
        total_avg_works = sum(
            e['metadata'].get('avg_works_per_year', 0) 
            for e in st.session_state.selected_entities
        )
        
        if total_avg_works > 0:
            years_span = st.session_state.config['end_year'] - st.session_state.config['start_year'] + 1
            estimated_publications = total_avg_works * years_span
            
            if estimated_publications > 100000:
                st.warning(
                    f"⚠️ The file might contain more than {estimated_publications:,.0f} publications. Consider:\n"
                    "- Filtering document types\n"
                    "- Selecting less metadata\n"
                    "- Removing abstracts (can save up to 40% of space)\n"
                    "- Choosing Parquet format for better compression"
                )
    
    if st.button("🚀 Start Retrieval", type="primary"):
        retrieve_publications()

def retrieve_publications():
    """Main retrieval function"""
    # Get configuration
    config = st.session_state.config
    entities = st.session_state.selected_entities
    
    # Validate configuration
    if config['start_year'] > config['end_year']:
        st.error("Start year cannot be after end year")
        return
    
    # Initialize session
    session = get_session()
    
    # Progress tracking
    start_time = time.time()
    progress_bar = st.progress(0)
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()
    timer_placeholder = st.empty()
    
    all_publications = []
    
    # Process each entity
    for i, entity in enumerate(entities):
        # Update progress
        progress = (i + 1) / len(entities)
        progress_bar.progress(progress)
        
        elapsed = time.time() - start_time
        timer_placeholder.info(f"⏱️ Time elapsed: {str(timedelta(seconds=int(elapsed)))}")
        
        status_placeholder.info(f"Fetching: {entity['label']}")
        
        # Fetch publications
        lang_filter = "english_only" if config['language_filter'] == "English Only" else "all_languages"
        
        entity_pubs = fetch_publications_parallel(
            session,
            entity['id'],
            entity['label'],
            entity['type'],
            config['start_year'],
            config['end_year'],
            config['doc_types'],
            config['metadata'],
            lang_filter
        )
        
        all_publications.extend(entity_pubs)
        
        # Update metrics
        metrics_placeholder.metric(
            label="Progress",
            value=f"{i+1}/{len(entities)} entities",
            delta=f"{len(all_publications)} publications fetched"
        )
        
        # Memory management
        if len(all_publications) > 10000 and i % 3 == 0:
            gc.collect()
    
    # Deduplication
    if all_publications:
        status_placeholder.info("Deduplicating publications...")
        
        total_before_dedup = len(all_publications)
        merged_publications = deduplicate_publications_optimized(all_publications)
        
        del all_publications
        gc.collect()
        
        duplicates_removed = total_before_dedup - len(merged_publications)
        
        # Create output dataframe
        df_output = pd.DataFrame(merged_publications)
        
        # Clean text fields
        for col in df_output.columns:
            if df_output[col].dtype == 'object':
                df_output[col] = df_output[col].apply(lambda x: clean_text_field(x) if isinstance(x, str) else x)
        
        # Reorder and rename columns
        columns_order = ["id"] + [col for col in config['metadata'] if col != "id"]
        columns_order.extend(["institutions_extracted", "authors_extracted", "position_extracted"])
        
        # Only include columns that exist
        columns_order = [col for col in columns_order if col in df_output.columns]
        df_output = df_output[columns_order]
        
        # Rename columns
        column_mapping = {field: METADATA_FIELDS.get(field, field) for field in df_output.columns}
        column_mapping.update({
            "institutions_extracted": "Institutions Extracted",
            "authors_extracted": "Authors Extracted",
            "position_extracted": "Author Position"
        })
        df_output = df_output.rename(columns=column_mapping)
        
        # Generate filename
        num_entities = len(entities)
        entity_type = "institutions" if st.session_state.selection_mode == "institutions" else "authors"
        timestamp = datetime.now().strftime("%H%M")
        
        # Calculate total time
        total_time = time.time() - start_time
        timer_placeholder.success(f"✅ Total processing time: {str(timedelta(seconds=int(total_time)))}")
        
        # Save output
        if config['output_format'] == "CSV":
            filename = f"pubs_{num_entities}_{entity_type}_{timestamp}.csv"
            
            csv_buffer = io.BytesIO()
            csv_string = df_output.to_csv(index=False, lineterminator='\n')
            csv_buffer.write(csv_string.encode('utf-8-sig'))
            csv_data = csv_buffer.getvalue()
            
            st.success(f"✅ Retrieved {len(merged_publications)} unique publications from {num_entities} {entity_type}")
            
            st.download_button(
                label=f"📥 Download {filename}",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                type="primary"
            )
        else:  # Parquet
            filename = f"pubs_{num_entities}_{entity_type}_{timestamp}.parquet"
            
            parquet_buffer = io.BytesIO()
            df_output.to_parquet(parquet_buffer, index=False, compression='snappy')
            parquet_data = parquet_buffer.getvalue()
            
            file_size_mb = len(parquet_data) / (1024 * 1024)
            
            st.success(f"✅ Retrieved {len(merged_publications)} unique publications from {num_entities} {entity_type}")
            st.info(f"File size: {file_size_mb:.1f} MB")
            
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