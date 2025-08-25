"""
OpenAlex Publications Retriever
Main application entry point
"""
import streamlit as st
from ui.landing import show_landing_page
from ui.institutions import render_institution_selector
from ui.authors import render_author_selector
from ui.common import render_config_section, render_retrieval_section

# Constants
MAILTO = "theodore.hervieux@sirisacademic.com"

def main():
    st.set_page_config(
        page_title="OpenAlex Publications Retriever",
        page_icon="📚",
        layout="wide"
    )
    
    # Initialize session state
    if 'selection_mode' not in st.session_state:
        st.session_state.selection_mode = None
    if 'selected_entities' not in st.session_state:
        st.session_state.selected_entities = []
    
    st.title("📚 OpenAlex Publications Retriever")
    st.markdown(f"*SIRIS Academic Research Tool - Contact: {MAILTO}*")
    
    # Show landing page or main workflow
    if st.session_state.selection_mode is None:
        show_landing_page()
    else:
        # Show switch method button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col3:
            if st.button("↩️ Switch Method", help="Go back to method selection"):
                st.session_state.selection_mode = None
                st.session_state.selected_entities = []
                if 'author_candidates' in st.session_state:
                    del st.session_state.author_candidates
                if 'config' in st.session_state:
                    del st.session_state.config
                st.rerun()
        
        # Display current mode
        mode_icon = "🏛️" if st.session_state.selection_mode == "institutions" else "👤"
        st.info(f"{mode_icon} **Mode:** Retrieving by {st.session_state.selection_mode.title()}")
        
        # Phase 1: Entity Selection (Institutions or Authors)
        if st.session_state.selection_mode == "institutions":
            render_institution_selector()
        else:
            render_author_selector()
        
        # Only show phases 2 & 3 if entities are selected
        if st.session_state.selected_entities:
            st.divider()
            # Phase 2: Configure Retrieval Parameters (shared)
            render_config_section()
            
            st.divider()
            # Phase 3: Retrieve Publications (shared)
            render_retrieval_section()

if __name__ == "__main__":
    main()