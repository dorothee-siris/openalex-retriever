# ui/authors.py
"""Author selection UI for OpenAlex retriever (form-based, no flicker, safe submit)
- 10 MB upload cap
- One big form: ticking checkboxes does NOT rerun the app
- Two global Confirm buttons (top and bottom of the form)
- Per-author tables are stable (index=ID); persistent frames for display
- On submit, we read the editors' return DataFrames (not session_state) and commit
- Summary updates ONLY after Confirm ALL
"""

from typing import Optional, List, Dict

import pandas as pd
import streamlit as st

from core.api_client import (
    get_session,
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
    """Fetch possible OpenAlex profiles for a given person."""
    session = get_http_session()
    candidates: List[Dict] = []
    seen_ids: set = set()

    # ORCID search (do not stop here; we still search by name)
    if orcid and str(orcid).strip().lower() not in {"", "nan", "none"}:
        for match in search_author_by_orcid(session, str(orcid).strip()):
            author_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
            if author_id and author_id not in seen_ids:
                details = get_author_details(session, author_id)
                if details:
                    candidates.append(extract_author_info(details))
                    seen_ids.add(author_id)

    # Name search (limit top 10)
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
    st.session_state.setdefault("editor_frames", {})       # {input_key: pd.DataFrame (index=ID)}
    st.session_state.setdefault("prefilled", False)        # whether we've auto-prefilled best matches

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

        if st.button("🔍 Load candidates", type="primary"):
            process_author_file(df, surname_col, name_col, orcid_col)

        if st.session_state.get("author_candidates"):
            display_author_candidates()

def process_author_file(df: pd.DataFrame, surname_col: str, name_col: str, orcid_col: Optional[str]):
    progress = st.progress(0)
    status = st.empty()

    # Reset for a new upload
    st.session_state.author_candidates = {}
    st.session_state.editor_frames = {}
    st.session_state.prefilled = False

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

        # Build persistent editor frame (unchecked by default)
        st.session_state.editor_frames[key] = _build_editor_frame(candidates, checked_ids=set())

    status.success(f"✅ Found candidates for {len(st.session_state.author_candidates)} authors")
    progress.empty()

def display_author_candidates():
    st.subheader("Select OpenAlex Profiles for Each Author")

    total_names = len(st.session_state.author_candidates)
    st.info(f"🧾 Names to review: **{total_names}**")

    # Optional prefill (best match by works_count)
    if st.button("⚡ Prefill best match for each author (optional)"):
        prefill_best_matches()
        st.success("Prefilled best matches. You can adjust before confirming.")
        st.rerun()

    # ---- BIG FORM: ticking inside does NOT rerun ----
    with st.form("authors_selection_form", clear_on_submit=False):
        # Top submit (user asked for top & bottom)
        submitted_top = st.form_submit_button("✅ Confirm ALL selections (add to list)", type="primary")

        # Capture editors' return values per author
        form_edits: Dict[str, pd.DataFrame] = {}

        for key, data in st.session_state.author_candidates.items():
            with st.expander(f"📝 {data['input_name']}", expanded=False):
                cands = data["candidates"]
                if not cands:
                    st.warning("No matches found.")
                    continue

                df_display = st.session_state.editor_frames[key]
                edited_df = st.data_editor(
                    df_display,
                    hide_index=True,
                    use_container_width=True,
                    num_rows="fixed",
                    column_order=["Select", "Name", "ORCID", "Publications", "Affiliations", "Topics"],
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select", help="Tick to choose this profile"),
                        "Publications": st.column_config.NumberColumn("Publications", format="%d"),
                    },
                    disabled=["Name", "ORCID", "Publications", "Affiliations", "Topics"],
                    key=f"editor_{key}",
                )
                # Store the returned DataFrame (real pandas object)
                form_edits[key] = edited_df

        submitted_bottom = st.form_submit_button("✅ Confirm ALL selections (add to list)", type="primary")

    submitted = submitted_top or submitted_bottom

    if submitted:
        commit_all_selected_authors(form_edits)
        st.success("All selections confirmed.")
        show_committed_summary()
    else:
        # Committed summary stays unchanged until confirm
        show_committed_summary()

def show_committed_summary():
    st.divider()
    committed_authors = [e for e in st.session_state.get("selected_entities", []) if e.get("type") == "author"]
    committed_profiles = len(committed_authors)
    committed_works = sum((e.get("metadata", {}) or {}).get("works_count", 0) for e in committed_authors)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Committed author profiles", committed_profiles)
    with c2:
        st.metric("Sum of publications (committed)", committed_works)

# ---------- Commit / Prefill helpers ----------

def prefill_best_matches():
    """Set the 'Select' column to true for the best match per author (before submit)."""
    if st.session_state.prefilled:
        return
    for key, data in st.session_state.author_candidates.items():
        cands = data["candidates"]
        if not cands:
            continue
        best = max(cands, key=lambda x: x.get("works_count", 0))
        df = st.session_state.editor_frames[key]
        df["Select"] = df.index == best["id"]
    st.session_state.prefilled = True

def commit_all_selected_authors(form_edits: Dict[str, pd.DataFrame]):
    """Commit selected profiles using the editors' returned DataFrames from the form."""
    if "author_candidates" not in st.session_state:
        return

    input_keys = set(st.session_state.author_candidates.keys())
    st.session_state.setdefault("selected_entities", [])

    # Remove any prior committed selections belonging to the current upload
    st.session_state.selected_entities = [
        e for e in st.session_state.selected_entities
        if not (e.get("type") == "author" and e.get("metadata", {}).get("input_key") in input_keys)
    ]

    # Iterate over the edited DataFrames captured from the form
    for key, data in st.session_state.author_candidates.items():
        edited_df = form_edits.get(key)
        if edited_df is None or "Select" not in edited_df.columns:
            continue

        # Ensure index=ID is preserved; if not, try to recover
        if edited_df.index.name != "ID":
            # If ID became a column, restore index; otherwise skip gracefully
            if "ID" in edited_df.columns:
                edited_df = edited_df.set_index("ID")
            else:
                continue

        selected_ids = list(edited_df.index[edited_df["Select"].astype(bool)])

        # Map to candidate dicts for metadata
        cands_by_id = {c["id"]: c for c in data["candidates"]}
        for sid in selected_ids:
            cand = cands_by_id.get(sid)
            if not cand:
                continue
            st.session_state.selected_entities.append({
                "type": "author",
                "id": sid,
                "label": f"{data['surname'].upper()} {data['name']} → {cand.get('display_name', '')}",
                "metadata": {**cand, "input_key": key},
            })

# ---------- Editor frame builder ----------

def _build_editor_frame(cands: List[Dict], checked_ids: set) -> pd.DataFrame:
    """Create a persistent editor DataFrame with stable row identity (index=ID)."""
    rows = []
    for c in cands:
        cid = c.get("id", "")
        rows.append({
            "ID": cid,
            "Select": cid in checked_ids,
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
