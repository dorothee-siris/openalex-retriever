# ui/institutions.py
"""
Institution selector (flicker-free)
- Uses a form to buffer checkbox ticks; only commits on "Add Selected"
- Caches search results per query to reduce UI churn while typing
- Search across Name, Acronym, and Alternative names
- Clean headers; clickable ROR link; default visible columns; option to show extra columns
- Selected list shows avg works/year (2021–23) and a total on top
"""

from __future__ import annotations

import os
import re
from typing import List, Dict, Optional

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
    Load institutions_master.parquet and normalize column naming.
    We try to standardize these names:
      - openalex_id, Name, Acronym, Type, Country, City, ROR, Total Works,
        Avg. Works/Year, Alternative Names
    """
    path = os.getenv(PARQUET_PATH_ENV, "institutions_master.parquet")
    if not os.path.exists(path):
        return None
    df = _load_parquet(path)

    # Normalize columns
    rename_map = {}
    for c in df.columns:
        lc = c.lower().strip()
        if lc == "openalex_id":
            rename_map[c] = "openalex_id"
        elif lc in ("name", "display_name"):
            rename_map[c] = "Name"
        elif lc in ("acronym", "short_name"):
            rename_map[c] = "Acronym"
        elif lc == "type":
            rename_map[c] = "Type"
        elif lc == "country":
            rename_map[c] = "Country"
        elif lc == "city":
            rename_map[c] = "City"
        elif lc in ("ror", "ror_id", "ror_url"):
            rename_map[c] = "ROR"
        elif lc in ("total works", "total_works", "works_total"):
            rename_map[c] = "Total Works"
        elif lc in ("avg. works/year", "avg_works_per_year", "avg works per year"):
            rename_map[c] = "Avg. Works/Year"
        elif lc in ("alternative names", "name_alternatives", "aliases", "alt_names"):
            rename_map[c] = "Alternative Names"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Ensure core columns
    for col in ["openalex_id", "Name"]:
        if col not in df.columns:
            df[col] = ""

    # Type cleanup
    for col in ["Name", "Acronym", "Type", "Country", "City", "ROR", "Alternative Names"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", "")

    if "Total Works" in df.columns:
        df["Total Works"] = pd.to_numeric(df["Total Works"], errors="coerce").fillna(0).astype(int)
    if "Avg. Works/Year" in df.columns:
        df["Avg. Works/Year"] = pd.to_numeric(df["Avg. Works/Year"], errors="coerce").fillna(0.0)

    return df


@st.cache_data(show_spinner=False)
def _search_df(df: pd.DataFrame, q: str) -> pd.DataFrame:
    """Case-insensitive substring search over Name, Acronym, Alternative Names."""
    q = q.strip()
    if not q:
        return df.head(0)

    cols = [c for c in ["Name", "Acronym", "Alternative Names"] if c in df.columns]
    if not cols:
        return df.head(0)

    qlc = q.casefold()
    mask = False
    for c in cols:
        mask = mask | df[c].astype(str).str.casefold().str.contains(qlc, na=False)

    res = df.loc[mask].copy()

    # Light ranking: name startswith gets a small boost
    if "Name" in res.columns:
        res["_rank"] = res["Name"].str.casefold().str.startswith(qlc).astype(int)
        res = res.sort_values(["_rank", "Total Works" if "Total Works" in res.columns else "Name"], ascending=[False, False])
        res = res.drop(columns=["_rank"], errors="ignore")

    return res


# ---------------- UI helpers ----------------

def _ensure_session_buffers():
    if "pending_inst_selection" not in st.session_state:
        st.session_state.pending_inst_selection = set()  # openalex_id
    if "institution_search_cache" not in st.session_state:
        st.session_state.institution_search_cache = {}   # {query -> pd.DataFrame}
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
    """Summary of already chosen institutions (with totals)."""
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

    # Clear/remove controls
    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("Clear all institutions"):
            st.session_state.selected_entities = [e for e in st.session_state.selected_entities if e.get("type") != "institution"]
            st.success("Cleared all institutions.")
    with c2:
        remove_id = st.text_input("Remove by OpenAlex ID", placeholder="e.g., I123456789")
        if remove_id and st.button("Remove institution"):
            before = len(st.session_state.selected_entities)
            st.session_state.selected_entities = [
                e for e in st.session_state.selected_entities
                if not (e.get("type") == "institution" and e.get("id") == remove_id)
            ]
            after = len(st.session_state.selected_entities)
            if after < before:
                st.success(f"Removed {remove_id}.")


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
        "Search institutions (Name, Acronym, Alternative Names):",
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

        # Construct display frame with a checkbox column pre-filled from pending set
        display_df = results_df.copy()

        # Ensure existence of useful columns
        for col in ["openalex_id", "Name", "Acronym", "Type", "Country", "City", "ROR", "Total Works", "Avg. Works/Year"]:
            if col not in display_df.columns:
                display_df[col] = ""

        display_df.insert(0, "Select", display_df["openalex_id"].apply(lambda x: x in st.session_state.pending_inst_selection))

        base_column_order = [
            "Select",
            "Name",
            "Acronym",
            "Type",
            "Country",
            "City",
            "ROR",
            "Total Works",
            "Avg. Works/Year",
            "openalex_id",  # kept visible for reliability; you can scroll right
        ]

        # Freeze interactions in a form
        with st.form("institutions_form", clear_on_submit=False):
            edited_df = st.data_editor(
                display_df,
                hide_index=True,
                use_container_width=True,
                column_order=[c for c in base_column_order if c in display_df.columns],
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Tick to add institution"),
                    "Name": st.column_config.TextColumn("Institution Name", width="large"),
                    "Acronym": st.column_config.TextColumn("Acronym", width="small"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Country": st.column_config.TextColumn("Country", width="small"),
                    "City": st.column_config.TextColumn("City", width="small"),
                    "ROR": st.column_config.LinkColumn("ROR link", width="medium"),
                    "Total Works": st.column_config.NumberColumn("Total works", format="%d"),
                    "Avg. Works/Year": st.column_config.NumberColumn("Avg. works/year (2021–23)", format="%.1f"),
                    "openalex_id": st.column_config.TextColumn("OpenAlex ID", width="medium"),
                },
                disabled=["Name", "Acronym", "Type", "Country", "City", "ROR", "Total Works", "Avg. Works/Year", "openalex_id"],
                key="institution_selector_editor",
            )

            try:
                selected_ids = set(edited_df.loc[edited_df["Select"] == True, "openalex_id"].tolist())
            except Exception:
                selected_ids = set()
            st.session_state.pending_inst_selection = selected_ids

            # Optional: extra columns
            show_extra = st.checkbox("Show extra columns", value=False, key="inst_show_extra")

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
                row = results_df[results_df["openalex_id"] == oid]
                if row.empty:
                    continue
                r = row.iloc[0]
                entity = {
                    "type": "institution",
                    "id": r["openalex_id"],
                    "label": r["Name"] or r["openalex_id"],
                    "metadata": {"avg_works_per_year": float(r["Avg. Works/Year"]) if "Avg. Works/Year" in r.index else None},
                }
                if not any(e["type"] == "institution" and e["id"] == entity["id"] for e in st.session_state.selected_entities):
                    st.session_state.selected_entities.append(entity)
                    added += 1
            st.session_state.pending_inst_selection = set()
            if added:
                st.success(f"✅ Added {added} institution(s).")

        # Show extra columns (optional view-only table)
        if st.session_state.get("inst_show_extra"):
            st.dataframe(results_df, use_container_width=True)

    display_selected_institutions()
