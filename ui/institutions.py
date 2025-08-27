# ui/institutions.py
"""Institution selection UI (no selection cap, with soft warnings)."""

from typing import Optional
import pandas as pd
import streamlit as st

# ---------- Data load & search ----------

@st.cache_data
def load_institutions():
    """Load institutions from parquet file (expected at app root)."""
    try:
        df = pd.read_parquet("institutions_master.parquet")
        return df
    except Exception as e:
        st.error(f"Error loading institutions file: {e}")
        return None

@st.cache_data(ttl=300)
def search_institutions_cached(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Vectorized search across several name/acronym/city fields."""
    if not query or len(query) < 2:
        return pd.DataFrame()

    q = query.lower()
    mask = (
        df["display_name"].str.lower().str.contains(q, na=False, regex=False)
        | df["display_name_alternatives"].str.lower().str.contains(q, na=False, regex=False)
        | df["display_name_acronyms"].str.lower().str.contains(q, na=False, regex=False)
        | df["city"].str.lower().str.contains(q, na=False, regex=False)
    )

    results = df[mask]

    # Table for UI display (we keep openalex_id hidden)
    display_df = pd.DataFrame(
        {
            "Select": False,
            "Name": results["display_name"].values,
            "Acronym": results["display_name_acronyms"].values,
            "Type": results["type"].values,
            "Country": results["country_code"].values,
            "City": results["city"].values,
            "ROR": results["ror_id"].apply(lambda x: f"https://ror.org/{x}" if x else ""),
            "Total Works": results["total_works_count"].values,
            "Avg. Works/Year": results["avg_works_per_year_2021_2023"].values,
            "openalex_id": results["openalex_id"].values,
        }
    )
    return display_df


# ---------- Main renderers ----------

def render_institution_selector():
    """Phase 1: Institution selection."""
    st.header("1️⃣ Select Institutions")

    # Load
    institutions_df = load_institutions()
    if institutions_df is None:
        st.error("Could not load institutions data. Make sure 'institutions_master.parquet' is in the app directory.")
        return

    # Search
    search_query = st.text_input(
        "Search institutions by name, acronym, alternative names, or city:",
        placeholder="Type at least 2 characters to search…",
    )

    if search_query:
        results_df = search_institutions_cached(institutions_df, search_query)

        if not results_df.empty:
            st.info(f"🔊 Found {len(results_df)} matching institutions.")

            edited_df = st.data_editor(
                results_df.drop(columns=["openalex_id"]),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn(
                        "Select", help="Check to add institution to selection", default=False, width="small"
                    ),
                    "Name": st.column_config.TextColumn("Institution Name", width="large"),
                    "ROR": st.column_config.LinkColumn("ROR", width="medium"),
                    "Total Works": st.column_config.NumberColumn("Total Works", format="%d", width="small"),
                    "Avg. Works/Year": st.column_config.NumberColumn("Avg. Works/Year", format="%.1f", width="small"),
                },
                disabled=["Name", "Acronym", "Type", "Country", "City", "ROR", "Total Works", "Avg. Works/Year"],
                key="institution_selector",
            )

            # Add selected
            col1, col2 = st.columns([1, 5])
            with col1:
                if st.button("➕ Add Selected", type="primary"):
                    selected_rows = edited_df[edited_df["Select"] == True]

                    if not selected_rows.empty:
                        added_count = 0
                        for idx, row in selected_rows.iterrows():
                            entity = {
                                "type": "institution",
                                "id": results_df.loc[idx, "openalex_id"],
                                "label": row["Name"],
                                "metadata": {
                                    "avg_works_per_year": row["Avg. Works/Year"],
                                },
                            }
                            # no cap – just prevent duplicates
                            if not any(e["id"] == entity["id"] for e in st.session_state.get("selected_entities", [])):
                                st.session_state.selected_entities.append(entity)
                                added_count += 1

                        if added_count > 0:
                            st.success(f"Added {added_count} institution(s)")
                            st.rerun()
                        else:
                            st.warning("All selected institutions are already in your list")
                    else:
                        st.warning("Please select at least one institution")
        else:
            st.info("No institutions found matching your search.")

    # Selected list + warnings
    display_selected_institutions()


def display_selected_institutions():
    """Show selected institutions and soft warnings."""
    # Ensure the container exists
    st.session_state.setdefault("selected_entities", [])

    institution_entities = [e for e in st.session_state.selected_entities if e["type"] == "institution"]

    if not institution_entities:
        st.info("No institutions selected yet. Search and select institutions above.")
        return

    st.divider()
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"📋 {len(institution_entities)} Selected Institutions")
    with col3:
        if st.button("🗑️ Clear All"):
            # only remove institutions; keep other selections (e.g., authors) if any
            st.session_state.selected_entities = [e for e in st.session_state.selected_entities if e["type"] != "institution"]
            st.rerun()

    # ---- Soft warnings ----
    # 1) Many institutions
    if len(institution_entities) > 30:
        st.warning(
            "You have selected more than **30 institutions**. "
            "Retrieval can be heavy—consider filtering document types or narrowing the year range."
        )

    # 2) Estimated total publications > 100k (avg works/year × selected year span)
    total_avg_per_year = sum(e["metadata"].get("avg_works_per_year", 0) for e in institution_entities)
    years_span = None
    if "config" in st.session_state:
        cfg = st.session_state.config
        try:
            years_span = int(cfg["end_year"]) - int(cfg["start_year"]) + 1
        except Exception:
            years_span = None

    if years_span is not None and total_avg_per_year:
        estimated_total = total_avg_per_year * years_span
        if estimated_total > 100_000:
            st.warning(
                f"Estimated total publications for the current time range "
                f"(**{years_span} years**) exceed **{estimated_total:,.0f}** "
                f"(based on the institutions’ average output). "
                "Consider reducing years, filtering document types, or exporting as Parquet."
            )
    else:
        # Optional informational line (per-year estimate), shown when Phase 2 not yet configured
        st.info(
            f"Estimated **per-year** output from the selected institutions: **{total_avg_per_year:,.0f}**. "
            "Once you set the time range in step 2, we’ll warn if the total exceeds 100k."
        )

    # List selected institutions with per-item remove button
    for i, entity in enumerate(institution_entities, start=1):
        colL, colR = st.columns([11, 1])
        with colL:
            st.write(f"**{i}.** {entity['label']}")
        with colR:
            if st.button("❌", key=f"remove_inst_{entity['id']}", help=f"Remove {entity['label']}"):
                st.session_state.selected_entities.remove(entity)
                st.rerun()
