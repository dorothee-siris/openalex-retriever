"""Landing page for method selection"""
import streamlit as st

def show_landing_page():
    """Display the landing page with method selection"""
    st.markdown("### Choose Your Retrieval Method")
    st.markdown("Select how you want to retrieve publications from OpenAlex:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style='text-align: center; padding: 20px; background-color: #f0f2f6; border-radius: 10px; min-height: 200px;'>
            <h2>🏛️ By Institutions</h2>
            <p>Search and select institutions to retrieve all publications affiliated with them.</p>
            <p><b>Best for:</b> Institutional reports, department analysis, university rankings</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("")  # Spacer
        
        if st.button("Select Institutions", key="inst_btn", use_container_width=True, type="primary"):
            st.session_state.selection_mode = "institutions"
            st.session_state.selected_entities = []
            st.rerun()
    
    with col2:
        st.markdown("""
        <div style='text-align: center; padding: 20px; background-color: #f0f2f6; border-radius: 10px; min-height: 200px;'>
            <h2>👤 By Authors</h2>
            <p>Upload a list of author names to retrieve their publications.</p>
            <p><b>Best for:</b> Team publications, researcher tracking, collaboration analysis</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("")  # Spacer
        
        if st.button("Select Authors", key="auth_btn", use_container_width=True, type="primary"):
            st.session_state.selection_mode = "authors"
            st.session_state.selected_entities = []
            st.rerun()