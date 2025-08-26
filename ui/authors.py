# ui/authors.py
"""Author selection UI for OpenAlex retriever (enhanced, stable selections)
- Cap upload at 10 MB
- Global confirm (top and bottom), no per-author confirm
- Single Expand/Collapse all toggle (robust)
- Show number of names to review
- Compact summary of selected profiles & total works
- Stable row identity in st.data_editor using ID as index (prevents auto-unselect)
"""

from typing import Optional, List, Dict

import pandas as pd
import streamlit as st

from core.api_client import (
    get_session,                # requests.Session factory
    search_author_by_name,
    search_author_by_orcid,
    get_author_details,
)

# ---------- Cached resources / helpers ----------

@st.cache_resource
def get_http_session():
    """One shared HTTP session for the whole app (resource, not data)."""
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


# ---------- Cached data calls ----------

@st.cache_data(ttl=300)
def fetch_author_candidates(first_name: str, last_name: str, orcid: Optional[str] = None) -> List[Dict]:
    """
    Fetch & cache candidate OpenAlex profiles for one (first,last,orcid).
    Uses the shared HTTP session resource.
    """
    session = get_http_session()
    candidates: List[Dict] = []
    seen_ids: set = set()

    # If ORCID present, search it — but DO NOT stop; also search by name to catch extra IDs.
    if orcid and str(orcid).strip().lower() not in {"", "nan", "none"}:
        for match in search_author_by_orcid(session, str(orcid).strip()):
            author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
            if author_id and author_id not in seen_ids:
                details = get_author_details(session, author_id)
                if details:
                    candidates.append(extract_author_info(details))
                    seen_ids.add(author_id)

    # Always search by name as well (limit top 10 for responsiveness)
    for match in search_author_by_name(session, first_name, last_name)[:10]:
        author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
        if author_id and author_id not in seen_ids:
            details = get_author_details(session, author_id)
            if details:
                candidates.append(extract_author_info(details))
                seen_ids.add(author_id)

    return candidates


# ---------- Render functions ----------

def render_author_selector():
    """Phase 1 — Author selection workflow."""
    st.header("1️⃣ Select Authors")

    # Init UI state
    st.session_state.setdefault("author_candidates", {})
    st.session_state.setdefault("expand_all_authors", False)
    # Used to force a remount of expanders so 'expanded' param takes effect
    st.session_state.setdefault("expander_toggle_nonce", 0)

    uploaded_file = st.file_uploader(
        "Upload Excel file with author names",
        type=["xlsx"],
        help="File must contain at least 'surname' and 'name' columns. 'orcid' is optional.",
    )

    if uploaded_file is not None:
        # 🚦 Hard cap at 10 MB
        if getattr(uploaded_file, "size", 0) > 10 * 1024 * 1024:
            st.error("The file is larger than 10 MB. Please upload a smaller file.")
            return

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
            surname_col_guess = auto_detect_column(columns, ["surname", "last_name", "family_name"])
            surname_col = st.selectbox(
                "Surname column",
                options=columns,
                index=columns.index(surname_col_guess) if surname_col_guess else 0,
            )
        with col2:
            name_col_guess = auto_detect_column(columns, ["name", "first_name", "given_name", "firstname"])
            name_col = st.selectbox(
                "Name column",
                options=columns,
                index=columns.index(name_col_guess) if name_col_guess else 0,
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

        # Only show candidates after processing
        if st.session_state.get("author_candidates"):
            display_author_candidates()


def process_author_file(df: pd.DataFrame, surname_col: str, name_col: str, orcid_col: Optional[str]):
    """Iterate rows, fetch & store candidates per uploaded person."""
    progress = st.progress(0)
    status = st.empty()

    # reset previous run’s candidates (keep selections list intact until confirm)
    st.session_state.author_candidates = {}

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
            "selected": [],   # list of selected OpenAlex IDs for this person
        }

    status.success(f"✅ Found candidates for {len(st.session_state.author_candidates)} authors")
    progress.empty()


def display_author_candidates():
    """Show candidate tables and allow multi-selection per uploaded person (global confirm)."""
    st.subheader("Select OpenAlex Profiles for Each Author")

    total_names = len(st.session_state.author_candidates)
    st.info(f"🧾 Names to review: **{total_names}**")

    # Top actions row (auto-select, expand/collapse toggle, global confirm)
    c1, c2, c3 = st.columns([1.7, 1.5, 1.9])
    with c1:
        if st.button("⚡ Auto-select best match for each author"):
            for key, data in st.session_state.author_candidates.items():
                if data["candidates"]:
                    best = max(data["candidates"], key=lambda x: x.get("works_count", 0))
                    data["selected"] = [best["id"]]
            st.success("Auto-selected best matches.")
            st.rerun()

    with c2:
        # Single toggle button that flips expand/collapse for all
        is_expanded = st.session_state.expand_all_authors
        label = "▾ Expand all" if not is_expanded else "▸ Collapse all"
        if st.button(label):
            st.session_state.expand_all_authors = not is_expanded
            # Force remount of all expanders by changing a nonce used in their labels
            st.session_state.expander_toggle_nonce += 1
            st.rerun()

    with c3:
        if st.button("✅ Confirm ALL selections (add to list)"):
            commit_all_selected_authors()
            st.success("All selections confirmed.")
            st.rerun()

    # Build a suffix that changes whenever the toggle flips; this forces expander remount
    nonce_suffix = "\u200b" * (st.session_state.expander_toggle_nonce % 2)

    # Per-author panels
    for key, data in st.session_state.author_candidates.items():
        # Changing the label by a tiny zero-width suffix forces a new widget -> expanded param is honored
        label = f"📝 {data['input_name']}{nonce_suffix}"
        with st.expander(label, expanded=st.session_state.expand_all_authors):
            cands = data["candidates"]
            if not cands:
                st.warning("No matches found.")
                continue

            # Build display with STABLE row identity: set index to 'ID'
            table_rows = []
            for c in cands:
                table_rows.append({
                    "Name": c.get("display_name", ""),
                    "ID": c.get("id", ""),
                    "ORCID": c.get("orcid", ""),
                    "Publications": c.get("works_count", 0),
                    "Affiliations": ", ".join(c.get("affiliations", [])[:2]),
                    "Topics": ", ".join(c.get("topics", [])[:3]),
                    "Select": c["id"] in data.get("selected", []),  # selection comes from session
                })

            df_display = pd.DataFrame(table_rows).set_index("ID")  # << stable row keys

            edited = st.data_editor(
                df_display,
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Check to include this profile"),
                    "Publications": st.column_config.NumberColumn("Publications", format="%d"),
                },
                # Note: 'ID' is now the index; don't list it in disabled columns.
                disabled=["Name", "ORCID", "Publications", "Affiliations", "Topics"],
                key=f"editor_{key}",
            )

            # Persist selection (no per-author confirm). 'edited' keeps our row index (ID)
            selected_ids = [idx for idx, val in edited["Select"].items() if bool(val)]
            st.session_state.author_candidates[key]["selected"] = selected_ids

    # --- Compact summary + bottom global confirm ---
    selected_ids_all = []
    id_to_works = {}
    for data in st.session_state.author_candidates.values():
        for c in data["candidates"]:
            id_to_works[c["id"]] = c.get("works_count", 0)
        selected_ids_all.extend(data.get("selected", []))

    selected_profiles = len(selected_ids_all)
    total_works = sum(id_to_works.get(_id, 0) for _id in selected_ids_all)

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Selected author profiles", selected_profiles)
    with c2:
        st.metric("Sum of publications across selected profiles", total_works)

    if st.button("✅ Confirm ALL selections (add to list)", key="confirm_all_bottom"):
        commit_all_selected_authors()
        st.success("All selections confirmed.")
        st.rerun()


# ---------- Commit helpers ----------

def commit_all_selected_authors():
    """Commit ALL current selections to st.session_state.selected_entities."""
    if "author_candidates" not in st.session_state:
        return

    # Remove previous selections for any of the currently uploaded names
    input_keys = set(st.session_state.author_candidates.keys())
    st.session_state.setdefault("selected_entities", [])
    st.session_state.selected_entities = [
        e for e in st.session_state.selected_entities
        if not (e.get("type") == "author" and e.get("metadata", {}).get("input_key") in input_keys)
    ]

    # Add new selections
    for key, data in st.session_state.author_candidates.items():
        surname = data["surname"]
        name = data["name"]
        for sid in data.get("selected", []):
            cand = next((c for c in data["candidates"] if c["id"] == sid), None)
            if not cand:
                continue
            st.session_state.selected_entities.append({
                "type": "author",
                "id": sid,
                "label": f"{surname.upper()} {name} → {cand.get('display_name', '')}",
                "metadata": {**cand, "input_key": key},
            })


# ---------- Data extraction utils ----------

def extract_author_info(author_data: Dict) -> Dict:
    """Normalize author detail payload into a compact dict for UI selection."""
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