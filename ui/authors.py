# ui/authors.py
"""Author selection UI for OpenAlex retriever"""

from typing import Optional, List, Dict

import pandas as pd
import streamlit as st

from core.api_client import (
    get_session,                # factory for requests.Session
    search_author_by_name,
    search_author_by_orcid,
    get_author_details,
)


# --------- CACHED RESOURCES / HELPERS ---------

@st.cache_resource
def get_http_session():
    """One shared HTTP session for the whole app (safe for non-serializable objects)."""
    return get_session()


def _author_input_key(surname: str, name: str) -> str:
    """Stable key per uploaded row (used to group selections)."""
    return f"{surname.strip().upper()}|{name.strip()}"


def auto_detect_column(columns: List[str], possible_names: List[str]) -> Optional[str]:
    """Auto-detect a column by common header names."""
    lower = {c.lower(): c for c in columns}
    for p in possible_names:
        if p.lower() in lower:
            return lower[p.lower()]
    return None


# --------- CACHED DATA CALLS ---------

@st.cache_data(ttl=300)
def fetch_author_candidates(first_name: str, last_name: str, orcid: Optional[str] = None) -> List[Dict]:
    """
    Fetch & cache candidate OpenAlex profiles for one (first,last,orcid).
    Uses the shared HTTP session resource.
    """
    session = get_http_session()
    candidates: List[Dict] = []
    seen_ids: set = set()

    # If ORCID present, search it — but DO NOT stop; we still search by name to catch extra IDs.
    if orcid and str(orcid).strip().lower() not in {"", "nan", "none"}:
        for match in search_author_by_orcid(session, str(orcid).strip()):
            author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
            if author_id and author_id not in seen_ids:
                details = get_author_details(session, author_id)
                if details:
                    candidates.append(extract_author_info(details))
                    seen_ids.add(author_id)

    # Always search by name as well (limit top 10 to keep UI snappy)
    for match in search_author_by_name(session, first_name, last_name)[:10]:
        author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
        if author_id and author_id not in seen_ids:
            details = get_author_details(session, author_id)
            if details:
                candidates.append(extract_author_info(details))
                seen_ids.add(author_id)

    return candidates


# --------- RENDER FUNCTIONS ---------

def render_author_selector():
    """Phase 1 — Author selection workflow."""
    st.header("1️⃣ Select Authors")

    uploaded_file = st.file_uploader(
        "Upload Excel file with author names",
        type=["xlsx"],
        help="File must contain at least 'surname' and 'name' columns. 'orcid' is optional.",
    )

    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Error reading file: {e}")
            return

        # Column mapping (auto-preselect if possible)
        st.subheader("Column Mapping")
        columns = list(df.columns)

        col1, col2, col3 = st.columns(3)
        with col1:
            surname_col = st.selectbox(
                "Surname column",
                options=columns,
                index=columns.index(auto_detect_column(columns, ["surname", "last_name", "family_name"])) if auto_detect_column(columns, ["surname", "last_name", "family_name"]) else 0,
            )
        with col2:
            name_col = st.selectbox(
                "Name column",
                options=columns,
                index=columns.index(auto_detect_column(columns, ["name", "first_name", "given_name", "firstname"])) if auto_detect_column(columns, ["name", "first_name", "given_name", "firstname"]) else 0,
            )
        with col3:
            orcid_guess = auto_detect_column(columns, ["orcid", "orcid_id"])
            orcid_options = ["None"] + columns
            orcid_col = st.selectbox(
                "ORCID column (optional)",
                options=orcid_options,
                index=orcid_options.index(orcid_guess) if orcid_guess else 0,
            )
            if orcid_col == "None":
                orcid_col = None

        if st.button("🔍 Search for Authors", type="primary"):
            process_author_file(df, surname_col, name_col, orcid_col)

        if st.session_state.get("author_candidates"):
            display_author_candidates()

    # Summary of selected author IDs (if any)
    display_selected_authors_summary()


def process_author_file(df: pd.DataFrame, surname_col: str, name_col: str, orcid_col: Optional[str]):
    """Iterate rows, fetch & store candidates per uploaded person."""
    progress = st.progress(0)
    status = st.empty()

    st.session_state.setdefault("author_candidates", {})

    total = len(df)
    for idx, row in df.iterrows():
        surname = str(row[surname_col]).strip() if pd.notna(row[surname_col]) else ""
        name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        orcid = str(row[orcid_col]).strip() if orcid_col and pd.notna(row[orcid_col]) else None
        if not surname or not name:
            continue

        key = _author_input_key(surname, name)
        status.text(f"Searching: {name} {surname}  ({idx + 1}/{total})")
        progress.progress((idx + 1) / max(total, 1))

        candidates = fetch_author_candidates(first_name=name, last_name=surname, orcid=orcid)

        st.session_state.author_candidates[key] = {
            "input_name": f"{surname}, {name}",
            "surname": surname,
            "name": name,
            "orcid": orcid,
            "candidates": candidates,
            "selected": [],
        }

    status.success(f"✅ Found candidates for {len(st.session_state.author_candidates)} authors")
    progress.empty()


def display_author_candidates():
    """Show candidate tables and allow multi-selection per uploaded person."""
    st.subheader("Select OpenAlex Profiles for Each Author")
    st.info("💡 You can select multiple profiles if the same person has several OpenAlex IDs.")

    # Quick helper: auto-select best per author (max works_count)
    if st.button("⚡ Auto-select best match for each author"):
        for key, data in st.session_state.author_candidates.items():
            if data["candidates"]:
                best = max(data["candidates"], key=lambda x: x.get("works_count", 0))
                data["selected"] = [best["id"]]
        st.success("Auto-selected best matches.")
        st.rerun()

    for key, data in st.session_state.author_candidates.items():
        with st.expander(f"📝 {data['input_name']}", expanded=False):
            cands = data["candidates"]
            if not cands:
                st.warning("No matches found.")
                continue

            table_rows = []
            for c in cands:
                table_rows.append({
                    "Select": c["id"] in data.get("selected", []),
                    "Name": c.get("display_name", ""),
                    "ID": c.get("id", ""),
                    "ORCID": c.get("orcid", ""),
                    "Publications": c.get("works_count", 0),
                    "Affiliations": ", ".join(c.get("affiliations", [])[:2]),
                    "Topics": ", ".join(c.get("topics", [])[:3]),
                })

            df_display = pd.DataFrame(table_rows)

            edited = st.data_editor(
                df_display,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Check to include this profile"),
                    "ID": st.column_config.TextColumn("OpenAlex ID", width="medium"),
                    "Publications": st.column_config.NumberColumn("Publications", format="%d"),
                },
                disabled=["Name", "ID", "ORCID", "Publications", "Affiliations", "Topics"],
                key=f"editor_{key}",
            )

            # Persist selection
            st.session_state.author_candidates[key]["selected"] = edited[edited["Select"]]["ID"].tolist()

            # Confirm button for this author
            if st.button("✅ Confirm selection", key=f"confirm_{key}"):
                update_selected_entities_for_author(key)
                st.success("Selection confirmed.")


def update_selected_entities_for_author(author_key: str):
    """Replace any previous selections for this uploaded person with the current ones."""
    data = st.session_state.author_candidates[author_key]
    surname = data["surname"]
    name = data["name"]
    selected_ids = set(data.get("selected", []))

    # Remove older entries for this author_key
    st.session_state.selected_entities = [
        e for e in st.session_state.selected_entities
        if not (e.get("type") == "author" and e.get("metadata", {}).get("input_key") == author_key)
    ]

    # Add current selections
    for cand_id in selected_ids:
        cand = next((c for c in data["candidates"] if c["id"] == cand_id), None)
        if not cand:
            continue
        st.session_state.selected_entities.append({
            "type": "author",
            "id": cand_id,
            "label": f"{surname.upper()} {name} → {cand.get('display_name', '')}",
            "metadata": {**cand, "input_key": author_key},
        })


def display_selected_authors_summary():
    """Compact list of selected author profiles (across all uploaded rows)."""
    author_entities = [e for e in st.session_state.get("selected_entities", []) if e.get("type") == "author"]
    if not author_entities:
        return

    st.divider()
    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader(f"📋 Selected Author Profiles ({len(author_entities)})")
    with col2:
        if st.button("🗑️ Clear All"):
            st.session_state.selected_entities = [e for e in st.session_state.selected_entities if e.get("type") != "author"]
            st.rerun()

    for ent in author_entities:
        c1, c2 = st.columns([11, 1])
        with c1:
            st.write(f"• {ent['label']}")
        with c2:
            if st.button("❌", key=f"remove_auth_{ent['id']}"):
                st.session_state.selected_entities.remove(ent)
                st.rerun()


# --------- DATA EXTRACTION UTILS ---------

def extract_author_info(author_data: Dict) -> Dict:
    """
    Normalize author detail payload into a compact dict for UI selection.
    Works for /people/{id} responses.
    """
    affiliations: List[str] = []

    # affiliations (historical)
    if isinstance(author_data.get("affiliations"), list):
        for aff in author_data["affiliations"][:3]:
            inst = aff.get("institution") if isinstance(aff, dict) else None
            if inst:
                affiliations.append(f"{inst.get('display_name', '')} ({inst.get('country_code', '')})")

    # last_known_institutions
    if isinstance(author_data.get("last_known_institutions"), list):
        for inst in author_data["last_known_institutions"][:2]:
            s = f"{inst.get('display_name', '')} ({inst.get('country_code', '')})"
            if s and s not in affiliations:
                affiliations.append(s)

    # topics
    topics: List[str] = []
    if isinstance(author_data.get("topics"), list):
        topics = [t.get("display_name", "") for t in author_data["topics"][:5]]

    return {
        "id": (author_data.get("id", "") or "").replace("https://openalex.org/", ""),
        "display_name": author_data.get("display_name", ""),
        "orcid": (author_data.get("orcid", "") or "").replace("https://orcid.org/", ""),
        "works_count": author_data.get("works_count", 0),
        "affiliations": affiliations,
        "topics": topics,
    }