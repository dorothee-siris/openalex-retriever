# ui/institutions.py
"""
Institution selector (flicker-free)
- Uses a form to buffer checkbox ticks; only commits on "Add Selected"
- Accent-insensitive search across: Institution name, Alternative name(s), Acronym(s)
- Clean results table: Institution name, Type, Country, City, ROR link, Total works, Avg works per year (2021–23)
  • Hidden by default but revealable: OpenAlex ID, Alternative name(s), Acronym(s)
- Selected list shows avg works/year per institution and a total on top
"""

from __future__ import annotations

import os
import unicodedata
from typing import Optional

import pandas as pd
import streamlit as st

PARQUET_PATH_ENV = "INSTITUTIONS_PARQUET_PATH"  # optional override


# ---------------- Cache & data loading ----------------

@st.cache_data(show_spinner=False)
def _load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

@st.cache_data(show_spinner=False)
def load_institutions() -> Optional[pd.DataFrame]:
    """
    Load the 'institutions_master.parquet' with the new, explicit headers:
      - OpenAlex ID, Institution name, Alternative name(s), Acronym(s),
        Type, Country, City, ROR ID, Total works, Avg works per year (2021-23)

    Adds a computed 'ROR link' column (https://ror.org/<id>).
    """
    path = os.getenv(PARQUET_PATH_ENV, "institutions_master.parquet")
    if not os.path.exists(path):
        return None
    df = _load_parquet(path).copy()

    # Ensure all expected columns exist
    expected = [
        "OpenAlex ID", "Institution name", "Alternative name(s)", "Acronym(s)",
        "Type", "Country", "City", "ROR ID", "Total works", "Avg works per year (2021-23)"
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = ""

    # Numeric coercions
    df["Total works"] = pd.to_numeric(df["Total works"], errors="coerce").fillna(0).astype(int)
    df["Avg works per year (2021-23)"] = pd.to_numeric(
        df["Avg works per year (2021-23)"], errors="coerce"
    ).fillna(0.0)

    # ROR link
    df["ROR link"] = df["ROR ID"].astype(str).str.strip().apply(
        lambda rid: f"https://ror.org/{rid}" if rid and rid != "nan" else ""
    )

    return df


# ---------------- Helpers ----------------

def _strip_accents_lower(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

@st.cache_data(show_spinner=False)
def _search_df(df: pd.DataFrame, raw_query: str) -> pd.DataFrame:
    """Accent-insensitive substring search across 3 columns."""
    q = _strip_accents_lower((raw_query or "").strip())
    if not q:
        return df.head(0)

    cols = ["Institution name", "Alternative name(s)", "Acronym(s)"]
    # Precompute normalized columns once to make searching snappy
    ndf = pd.DataFrame({c: df[c].astype(str).map(_strip_accents_lower) for c in cols})

    mask = False
    for c in cols:
        mask = mask | ndf[c].str.contains(q, na=False)

    return df.loc[mask].copy()


def _ensure_session_buffers():
    if "pending_inst_selection" not in st.session_state:
        st.session_state.pending_inst_selection = set()  # OpenAlex ID
    if "institution_search_cache" not in st.session_state:
        st.session_state.institution_search_cache = {}   # {query -> DataFrame}
    if "selected_entities" not in st.session_state:
        st.session_state.selected_entities = []


def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def display_selected_institutions():
    """Summary of chosen institutions (with total)."""
    sel = [e for e in st.session_state.selected_entities if e.get("type") == "institution"]
    if not sel:
        st.info("No institutions selected yet.")
        return

    total_avg = sum(_safe_float((e.get("metadata") or {}).get("avg_works_per_year")) for e in sel)
    st.subheader(f"Selected institutions — total avg works/year (2021–23): **{total_avg:,.1f}**")

    for e in sel:
        name = e.get("label", e.get("id"))
        avg = _safe_float((e.get("metadata") or {}).get("avg_works_per_year"))
        st.write(f"• **{name}** — avg works/year (2021–23): {avg:,.1f}")

    # Just a single "Clear all" control (you asked to remove 'remove by ID')
    if st.button("Clear all institutions"):
        st.session_state.selected_entities = [e for e in st.session_state.selected_entities if e.get("type") != "institution"]
        st.success("Cleared all institutions.")


# ---------------- Main selector ----------------

def render_institution_selector():
    """Render the institution selection interface (form-buffered to avoid flicker)."""
    _ensure_session_buffers()

    st.header("1️⃣ Select Institutions")

    # Load data
    institutions_df = load_institutions()
    if institutions_df is None:
        st.error("Could not load institutions data. Ensure 'institutions_master.parquet' is present.")
        return

    # Search input (outside form; cached results avoid jumpiness)
    search_query = st.text_input(
        "Search institutions (Institution name, Alternative name(s), Acronym(s)) — accent-insensitive:",
        placeholder="Type at least 2 characters…",
        key="inst_query",
    )

    results_df = institutions_df.head(0)
    if search_query and len(search_query.strip()) >= 2:
        cache = st.session_state.institution_search_cache
        if search_query not in cache:
            cache[search_query] = _search_df(institutions_df, search_query)
        results_df = cache[search_query]

    if not results_df.empty:
        st.info(f"🔎 Found {len(results_df)} matching institutions. Tick the ones to add, then press **Add Selected**.")

        # Build the display frame + checkbox column
        display_df = results_df.copy()
        for col in [
            "OpenAlex ID", "Institution name", "Alternative name(s)", "Acronym(s)",
            "Type", "Country", "City", "ROR link", "Total works", "Avg works per year (2021-23)"
        ]:
            if col not in display_df.columns:
                display_df[col] = ""

        # Pre-fill Select column based on pending set
        display_df.insert(
            0, "Select",
            display_df["OpenAlex ID"].apply(lambda x: x in st.session_state.pending_inst_selection)
        )

        # Default visible columns (as requested) — hide extras by default
        visible_cols = [
            "Select",
            "Institution name",
            # Acronym(s) hidden by default
            "Type",
            "Country",
            "City",
            "ROR link",
            "Total works",
            "Avg works per year (2021-23)",
            # "OpenAlex ID" hidden by default
        ]
        extra_cols = ["Acronym(s)", "Alternative name(s)", "OpenAlex ID"]

        # Freeze interactions in a form
        with st.form("institutions_form", clear_on_submit=False):
            show_extra = st.checkbox("Show extra columns", value=False, key="inst_show_extra")

            column_order = visible_cols + (extra_cols if show_extra else [])
            edited_df = st.data_editor(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_order=[c for c in column_order if c in display_df.columns],
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Tick to add institution"),
                    "Institution name": st.column_config.TextColumn("Institution Name", width="large"),
                    "Acronym(s)": st.column_config.TextColumn("Acronym(s)", width="small"),
                    "Alternative name(s)": st.column_config.TextColumn("Alternative name(s)", width="large"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Country": st.column_config.TextColumn("Country", width="small"),
                    "City": st.column_config.TextColumn("City", width="small"),
                    "ROR link": st.column_config.LinkColumn("ROR link", width="medium"),
                    "Total works": st.column_config.NumberColumn("Total works", format="%d"),
                    "Avg works per year (2021-23)": st.column_config.NumberColumn("Avg works/year (2021–23)", format="%.1f"),
                    "OpenAlex ID": st.column_config.TextColumn("OpenAlex ID", width="medium"),
                },
                disabled=[
                    "Institution name", "Acronym(s)", "Alternative name(s)", "Type", "Country", "City",
                    "ROR link", "Total works", "Avg works per year (2021-23)", "OpenAlex ID"
                ],
                key="institution_selector_editor",
            )

            try:
                selected_ids = set(edited_df.loc[edited_df["Select"] == True, "OpenAlex ID"].tolist())
            except Exception:
                selected_ids = set()
            st.session_state.pending_inst_selection = selected_ids

            c1, c2 = st.columns([1, 2])
            with c1:
                submitted = st.form_submit_button("➕ Add Selected", type="primary")
            with c2:
                cleared = st.form_submit_button("Clear ticks (pending)")

        if cleared:
            st.session_state.pending_inst_selection = set()
            st.success("Pending ticks cleared.")

        if submitted:
            added = 0
            for oid in st.session_state.pending_inst_selection:
                row = results_df[results_df["OpenAlex ID"] == oid]
                if row.empty:
                    continue
                r = row.iloc[0]
                entity = {
                    "type": "institution",
                    "id": r["OpenAlex ID"],
                    "label": r["Institution name"] or r["OpenAlex ID"],
                    "metadata": {
                        "avg_works_per_year": float(r["Avg works per year (2021-23)"]) if "Avg works per year (2021-23)" in r.index else None
                    },
                }
                if not any(e["type"] == "institution" and e["id"] == entity["id"] for e in st.session_state.selected_entities):
                    st.session_state.selected_entities.append(entity)
                    added += 1
            st.session_state.pending_inst_selection = set()
            if added:
                st.success(f"✅ Added {added} institution(s).")

    display_selected_institutions()
