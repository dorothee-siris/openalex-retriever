# ui/authors.py
"""Author selection UI for OpenAlex retriever (staged + stable)
- 10 MB upload cap
- Global Confirm (top + bottom); no per-author confirm
- Single Expand/Collapse-all toggle (works)
- Select column pinned left; stable row identity (index=ID)
- No summary/commit until Confirm ALL
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

# ---------- Cached resources ----------

@st.cache_resource
def get_http_session():
    return get_session()

# ---------- Small helpers ----------

def _author_input_key(surname: str, name: str) -> str:
    return f"{surname.strip().upper()}|{name.strip()}"

def auto_detect_column(columns: List[str], possible_names: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in columns}
    for p in possible_names:
        if p.lower() in lower:
            return lower[p.lower()]
    return None

# ---------- Cached API calls ----------

@st.cache_data(ttl=300)
def fetch_author_candidates(first_name: str, last_name: str, orcid: Optional[str] = None) -> List[Dict]:
    session = get_http_session()
    candidates: List[Dict] = []
    seen_ids: set = set()

    # Search by ORCID (but DO NOT stop; still search by name)
    if orcid and str(orcid).strip().lower() not in {"", "nan", "none"}:
        for match in search_author_by_orcid(session, str(orcid).strip()):
            author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
            if author_id and author_id not in seen_ids:
                details = get_author_details(session, author_id)
                if details:
                    candidates.append(extract_author_info(details))
                    seen_ids.add(author_id)

    # Also search by name (limit top 10)
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
    st.header("1️⃣ Select Authors")

    # Init UI state
    st.session_state.setdefault("author_candidates", {})   # {input_key: {...}}
    st.session_state.setdefault("staged_selections", {})   # {input_key: set(ids)}
    st.session_state.setdefault("editor_frames", {})       # {input_key: pd.DataFrame (index=ID)}
    st.session_state.setdefault("expand_all_authors", False)
    st.session_state.setdefault("expander_toggle_nonce", 0)

    uploaded_file = st.file_uploader(
        "Upload Excel file with author names",
        type=["xlsx"],
        help="File must contain at least 'surname' and 'name' columns. 'orcid' is optional.",
    )

    if uploaded_file is not None:
        # 10 MB cap
        if getattr(uploaded_file, "size", 0) > 10 * 1024 * 1024:
            st.error("The file is larger than 10 MB. Please upload a smaller file.")
            return

        try:
            df = pd.read_excel(uploaded_file)
        except Exception as e:
            st.error(f"Error reading file: {e}")
            return

        # Column mapping
        st.subheader("Column Mapping")
        columns = list(df.columns)

        col1, col2, col3 = st.columns(3)
        with col1:
            g = auto_detect_column(columns, ["surname", "last_name", "family_name"])
            surname_col = st.selectbox("Surname column", options=columns,
                                       index=columns.index(g) if g else 0)
        with col2:
            g = auto_detect_column(columns, ["name", "first_name", "given_name", "firstname"])
            name_col = st.selectbox("Name column", options=columns,
                                    index=columns.index(g) if g else 0)
        with col3:
            g = auto_detect_column(columns, ["orcid", "orcid_id"])
            opts = ["None"] + columns
            orcid_col = st.selectbox("ORCID column (optional)", options=opts,
                                     index=opts.index(g) if g else 0)
            if orcid_col == "None":
                orcid_col = None

        if st.button("🔍 Search for Authors", type="primary"):
            process_author_file(df, surname_col, name_col, orcid_col)

        if st.session_state.get("author_candidates"):
            display_author_candidates()

def process_author_file(df: pd.DataFrame, surname_col: str, name_col: str, orcid_col: Optional[str]):
    progress = st.progress(0)
    status = st.empty()

    # Reset for a new upload
    st.session_state.author_candidates = {}
    st.session_state.staged_selections = {}
    st.session_state.editor_frames = {}

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
        }
        st.session_state.staged_selections[key] = set()  # start empty

    status.success(f"✅ Found candidates for {len(st.session_state.author_candidates)} authors")
    progress.empty()

def display_author_candidates():
    st.subheader("Select OpenAlex Profiles for Each Author")

    total_names = len(st.session_state.author_candidates)
    st.info(f"🧾 Names to review: **{total_names}**")

    # Top actions row
    c1, c2, c3 = st.columns([1.7, 1.5, 1.9])
    with c1:
        if st.button("⚡ Auto-select best match for each author"):
            # Stage only; do not commit
            for key, data in st.session_state.author_candidates.items():
                cands = data["candidates"]
                if cands:
                    best = max(cands, key=lambda x: x.get("works_count", 0))
                    st.session_state.staged_selections[key] = {best["id"]}
            st.success("Auto-selected best matches (staged).")
            st.rerun()

    with c2:
        label = "▾ Expand all" if not st.session_state.expand_all_authors else "▸ Collapse all"
        if st.button(label):
            st.session_state.expand_all_authors = not st.session_state.expand_all_authors
            st.session_state.expander_toggle_nonce += 1  # force remount so expanded= applies
            st.rerun()

    with c3:
        if st.button("✅ Confirm ALL selections (add to list)"):
            commit_all_selected_authors()
            st.success("All selections confirmed.")
            st.rerun()

    # Suffix toggles to remount expanders
    nonce_suffix = "\u200b" * (st.session_state.expander_toggle_nonce % 2)

    frames = st.session_state.editor_frames  # persistent per-author tables

    # Per-author panels (pure staging; no committing)
    for key, data in st.session_state.author_candidates.items():
        label = f"📝 {data['input_name']}{nonce_suffix}"
        with st.expander(label, expanded=st.session_state.expand_all_authors):
            cands = data["candidates"]
            if not cands:
                st.warning("No matches found.")
                continue

            staged = st.session_state.staged_selections.get(key, set())

            # Create table once, or rebuild if candidate IDs changed
            if key not in frames:
                frames[key] = _build_editor_frame(cands, staged)
            else:
                df = frames[key]
                current_ids = list(df.index)
                new_ids = [c.get("id", "") for c in cands]
                if current_ids != new_ids:
                    frames[key] = _build_editor_frame(cands, staged)
                else:
                    # Keep existing object; just sync Select from staged
                    df["Select"] = df.index.isin(staged)

            df_display = frames[key]

            # Stable object; stable key
            edited = st.data_editor(
                df_display,
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                column_order=["Select", "Name", "ORCID", "Publications", "Affiliations", "Topics"],
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Tick to stage this profile"),
                    "Publications": st.column_config.NumberColumn("Publications", format="%d"),
                },
                disabled=["Name", "ORCID", "Publications", "Affiliations", "Topics"],
                key=f"editor_{key}",
            )

            # Update staged set from edited table; mirror back to stored frame (no rebuild)
            sel_series = edited["Select"].astype(bool).fillna(False)
            st.session_state.staged_selections[key] = set(sel_series[sel_series].index)
            frames[key]["Select"] = sel_series

    # ---- Committed summary (ONLY after Confirm) ----
    st.divider()
    committed_authors = [e for e in st.session_state.get("selected_entities", []) if e.get("type") == "author"]
    committed_profiles = len(committed_authors)
    committed_works = sum((e.get("metadata", {}) or {}).get("works_count", 0) for e in committed_authors)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Committed author profiles", committed_profiles)
    with c2:
        st.metric("Sum of publications (committed)", committed_works)

    if st.button("✅ Confirm ALL selections (add to list)", key="confirm_all_bottom"):
        commit_all_selected_authors()
        st.success("All selections confirmed.")
        st.rerun()

# ---------- Commit helpers ----------

def commit_all_selected_authors():
    """Commit staged selections into selected_entities. Only this unlocks next sections & updates summary."""
    if "author_candidates" not in st.session_state:
        return

    input_keys = set(st.session_state.author_candidates.keys())
    st.session_state.setdefault("selected_entities", [])

    # Drop previous committed selections for the current upload
    st.session_state.selected_entities = [
        e for e in st.session_state.selected_entities
        if not (e.get("type") == "author" and e.get("metadata", {}).get("input_key") in input_keys)
    ]

    # Commit staged
    for key, data in st.session_state.author_candidates.items():
        surname = data["surname"]
        name = data["name"]
        cands_by_id = {c["id"]: c for c in data["candidates"]}
        for sid in st.session_state.staged_selections.get(key, set()):
            cand = cands_by_id.get(sid)
            if not cand:
                continue
            st.session_state.selected_entities.append({
                "type": "author",
                "id": sid,
                "label": f"{surname.upper()} {name} → {cand.get('display_name', '')}",
                "metadata": {**cand, "input_key": key},
            })

# ---------- Editor frame builder ----------

def _build_editor_frame(cands: List[Dict], staged: set) -> pd.DataFrame:
    """Create the editor DataFrame once, with stable row identity (index=ID)."""
    rows = []
    for c in cands:
        cid = c.get("id", "")
        rows.append({
            "ID": cid,
            "Select": cid in staged,
            "Name": c.get("display_name", ""),
            "ORCID": c.get("orcid", ""),
            "Publications": c.get("works_count", 0),
            "Affiliations": ", ".join(c.get("affiliations", [])[:2]),
            "Topics": ", ".join(c.get("topics", [])[:3]),
        })
    df = pd.DataFrame(rows).set_index("ID")
    df["Select"] = df["Select"].astype(bool)
    return df

# ---------- Data extraction utils ----------

def extract_author_info(author_data: Dict) -> Dict:
    affiliations: List[str] = []

    if isinstance(author_data.get("affiliations"), list):
        for aff in author_data["affiliations"][:3]:
            inst = aff.get("institution") if isinstance(aff, dict) else None
            if inst:
                affiliations.append(f"{inst.get('display_name', '')} ({inst.get('country_code', '')})")

    if isinstance(author_data.get("last_known_institutions"), list):
        for inst in author_data["last_known_institutions"][:2]:
            s = f"{inst.get('display_name', '')} ({inst.get('country_code', '')})"
            if s and s not in affiliations:
                affiliations.append(s)

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