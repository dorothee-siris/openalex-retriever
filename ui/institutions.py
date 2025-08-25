"""Institution selection UI"""
import streamlit as st
import pandas as pd
import numpy as np
from typing import Optional

@st.cache_data
def load_institutions():
    """Load institutions from parquet file"""
    try:
        df = pd.read_parquet('institutions_master.parquet')
        return df
    except Exception as e:
        st.error(f"Error loading institutions file: {e}")
        return None

@st.cache_data(ttl=300)
def search_institutions_cached(df, query):
    """Cached search for institutions"""
    if not query or len(query) < 2:
        return pd.DataFrame()
    
    query_lower = query.lower()
    
    # Vectorized search
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

def render_institution_selector():
    """Render the institution selection interface"""
    st.header("1️⃣ Select Institutions")
    
    # Load institutions data
    institutions_df = load_institutions()
    
    if institutions_df is None:
        st.error("Could not load institutions data. Please ensure 'institutions_master.parquet' is in the app directory.")
        return
    
    # Search input
    search_query = st.text_input(
        "Search institutions by name, acronym, alternative names, or city:", 
        placeholder="Type at least 2 characters to search..."
    )
    
    # Search results
    if search_query:
        results_df = search_institutions_cached(institutions_df, search_query)
        
        if not results_df.empty:
            max_remaining = 10 - len(st.session_state.selected_entities)
            st.info(f"🔊 Found {len(results_df)} matching institutions. Select up to {max_remaining} more institutions.")
            
            # Interactive selection
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
                            entity = {
                                'type': 'institution',
                                'id': results_df.loc[idx, 'openalex_id'],
                                'label': row['Name'],
                                'metadata': {
                                    'avg_works_per_year': row['Avg. Works/Year']
                                }
                            }
                            
                            if len(st.session_state.selected_entities) < 10:
                                # Check for duplicates
                                if not any(e['id'] == entity['id'] for e in st.session_state.selected_entities):
                                    st.session_state.selected_entities.append(entity)
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
    display_selected_institutions()

def display_selected_institutions():
    """Display the list of selected institutions"""
    institution_entities = [e for e in st.session_state.selected_entities if e['type'] == 'institution']
    
    if not institution_entities:
        st.info("No institutions selected yet. Search and select institutions above.")
        return
    
    st.divider()
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"📋 Selected Institutions ({len(institution_entities)}/10)")
    with col3:
        if st.button("🗑️ Clear All"):
            st.session_state.selected_entities = []
            st.rerun()
    
    # Calculate estimated publications
    total_avg_works = sum(
        e['metadata'].get('avg_works_per_year', 0) 
        for e in institution_entities
    )
    
    if total_avg_works > 0:
        st.markdown(
            f"<p style='color: red;'>These institutions produce an estimated "
            f"<b>{total_avg_works:.0f}</b> publications per year (all document types).</p>",
            unsafe_allow_html=True
        )
    
    # List institutions
    for i, entity in enumerate(institution_entities):
        col1, col2 = st.columns([11, 1])
        with col1:
            st.write(f"**{i+1}.** {entity['label']}")
        with col2:
            if st.button("❌", key=f"remove_inst_{entity['id']}", help=f"Remove {entity['label']}"):
                st.session_state.selected_entities.remove(entity)
                st.rerun()