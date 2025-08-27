# ui/landing.py
"""Landing page for method selection with equal-sized cards and output note below."""
import streamlit as st

def show_landing_page():
    st.markdown("### Choose Your Retrieval Method")
    st.markdown("Select how you want to retrieve publications from OpenAlex:")

    col1, col2 = st.columns(2)

    # Common card style to keep same visual size
    card_style = (
        "text-align: left; padding: 20px; background-color: #f0f2f6; "
        "border-radius: 10px; min-height: 260px; display:flex; flex-direction:column; justify-content:flex-start;"
    )

    with col1:
        st.markdown(
            f"""
            <div style='{card_style}'>
                <h2>🏛️ By Institutions</h2>
                <p style="margin-bottom:6px;">
                  Search and select institutions to retrieve all publications affiliated with them.
                </p>
                <p style="margin: 10px 0 6px 0;"><b>Best for:</b></p>
                <ul style="margin: 0 0 6px 18px;">
                  <li>Rebuilding a <b>disambiguated dataset</b> for a lab/faculty/department (using ROR sub-orgs)</li>
                  <li>Studying <b>intra-group collaboration</b> between units of a university/federation</li>
                  <li>Downloading a clean dataset for a <b>small consortium/facility cluster</b> with known ROR IDs</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("")  # spacer
        if st.button("Select Institutions", key="inst_btn", use_container_width=True, type="primary"):
            st.session_state.selection_mode = "institutions"
            st.session_state.selected_entities = []
            st.rerun()

    with col2:
        st.markdown(
            f"""
            <div style='{card_style}'>
                <h2>👤 By Authors</h2>
                <p style="margin-bottom:6px;">
                  Upload an Excel list of names to retrieve their publications
                  (you can select multiple OpenAlex profiles per person).
                </p>
                <p style="margin: 10px 0 6px 0;"><b>Best for:</b></p>
                <ul style="margin: 0 0 6px 18px;">
                  <li>Analysing <b>non-institutional perimeters</b> — transversal initiatives, institutes, teams, networks</li>
                  <li>Measuring an organisation’s <b>true research footprint</b> via its people
                      (reduces affiliation biases: wrong/lagging affiliations, retirees, etc.)</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("")  # spacer
        if st.button("Select Authors", key="auth_btn", use_container_width=True, type="primary"):
            st.session_state.selection_mode = "authors"
            st.session_state.selected_entities = []
            st.rerun()

    # --- Output behavior banner (after the choice) ---
    st.markdown("")
    st.markdown(
        """
        <div style="
            border-left: 6px solid #4F8BF9;
            background: #eef4ff;
            padding: 14px 16px;
            border-radius: 8px;">
          <h3 style="margin: 0 0 6px 0;">About the output</h3>
          <p style="margin: 0;">
            Whatever method you choose, the downloaded file is <b>deduplicated at the work level</b> (OpenAlex ID).
            If the same publication involves several of your selected entities, it appears <b>once</b> with:
          </p>
          <ul style="margin: 8px 0 0 18px;">
            <li><b>Institutions Extracted</b> — all selected institutions present on that work</li>
            <li><b>Authors Extracted</b> — all selected authors present on that work (and their <b>Author Position</b>)</li>
          </ul>
          <p style="margin: 6px 0 0 0;">
            This makes it easy to read <b>collaborations inside your selection</b> without duplicates inflating counts.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
