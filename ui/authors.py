# ui/authors.py
"""Author selection UI for OpenAlex retriever (parallel candidate fetch, form-based stable UI)
- Name search only (no ORCID, no /people details calls)
- Parallel candidate discovery for speed
- Up to 20 profiles per author (from /authors search)
- One big form: ticking checkboxes does NOT rerun the app
- Single Confirm button at bottom
- Per-author tables are stable (index=ID); persistent frames for display
- Summary updates ONLY after Confirm ALL
- NEW: Two columns in candidate table:
    * 'Affiliations 2025' — affiliations whose 'years' include 2025
    * 'Last known institutions' — from last_known_institutions
  Both formatted as 'Display Name, CC' joined with ' | '
"""

from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import streamlit as st

from core.api_client import (
    get_session,
    search_author_by_name,    # name-only search
)

# ---------- Config for parallel fetch ----------
WORKERS = 10  # tune 8–12 safely; global rate limiter still keeps requests polite


# ---------- Small helpers ----------

def _author_input_key(surname: str, name: str) -> str:
    return f"{surname.strip().upper()}|{name.strip()}"

def auto_detect_column(columns: List[str], possible_names: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in columns}
    for p in possible_names:
        if p.lower() in lower:
            return lower[p.lower()]
    return None

def _zwsp_salt(key: str) -> str:
    """Invisible suffix so expander labels are unique without visible clutter."""
    count = (abs(hash(key)) % 3) + 1
    return "\u200b" * count  # zero-width space(s)


# ---------- Candidate extraction from /authors results ----------

def _fmt_inst(display_name: str, cc: str) -> str:
    dn = display_name or ""
    cc = (cc or "").upper()
    return f"{dn}, {cc}".strip().strip(",")  # avoid dangling comma if cc empty

def _candidate_from_authors_result(match: Dict) -> Dict:
    """Build a candidate row from an /authors search result (no /people details call)."""
    # Basic fields
    openalex_id = (match.get("id", "") or "").replace("https://openalex.org/", "")
    display_name = match.get("display_name", "") or ""
    orcid = (match.get("orcid", "") or "").replace("https://orcid.org/", "")
    works_count = match.get("works_count", 0) or 0

    # 1) Affiliations 2025: from 'affiliations' list where 2025 in 'years'
    aff_2025_list: List[str] = []
    affs = match.get("affiliations") or []
    if isinstance(affs, list):
        for aff in affs:
            if not isinstance(aff, dict):
                continue
            years = aff.get("years") or []
            if 2025 in years:
                inst = aff.get("institution") or {}
                aff_2025_list.append(_fmt_inst(inst.get("display_name", ""), inst.get("country_code", "")))

    # 2) Last known institutions
    lki_list: List[str] = []
    lki = match.get("last_known_institutions") or []
    if isinstance(lki, list):
        for inst in lki:
            if isinstance(inst, dict):
                lki_list.append(_fmt_inst(inst.get("display_name", ""), inst.get("country_code", "")))

    # Topics sometimes present on /authors; keep if available
    topics: List[str] = []
    if isinstance(match.get("topics"), list):
        topics = [t.get("display_name", "") for t in match["topics"][:5]]

    return {
        "id": openalex_id,
        "display_name": display_name,
        "orcid": orcid,
        "works_count": works_count,
        "affiliations_2025": aff_2025_list,      # NEW
        "last_known_insts": lki_list,            # NEW
        "topics": topics,
    }


# ---------- Editor frame builder ----------

def _build_editor_frame(cands: List[Dict], checked_ids: set) -> pd.DataFrame:
    """Create a persistent editor DataFrame with stable row identity (index=ID)."""
    # Ensure we always have the columns so set_index('ID') never fails
    base_cols = [
        "ID", "Select", "Name", "ORCID", "Publications",
        "Affiliations 2025", "Last known institutions", "Topics"
    ]
    rows = []
    for c in cands or []:
        cid = c.get("id", "")
        rows.append({
            "ID": cid,
            "Select": cid in checked_ids,
            "Name": c.get("display_name", ""),
            "ORCID": c.get("orcid", ""),
            "Publications": c.get("works_count", 0),
            "Affiliations 2025": " | ".join(c.get("affiliations_2025", [])[:4]),
            "Last known institutions": " | ".join(c.get("last_known_insts", [])[:4]),
            "Topics": " | ".join(c.get("topics", [])[:3]),
        })

    df = pd.DataFrame(rows, columns=base_cols)
    # Even if empty, this works because the "ID" column exists
    df = df.set_index("ID")
    if "Select" in df.columns:
        df["Select"] = df["Select"].astype(bool)
    return df


# ---------- Parallel candidate discovery ----------

def _make_session_pool(n: int) -> List:
    """Create n independent sessions (requests.Session is not strictly thread-safe)."""
    return [get_session() for _ in range(n)]

def _fetch_candidates_for_one(session, first: str, last: str) -> Dict:
    """Name search only. Return a payload with up to 20 candidates built from /authors results."""
    candidates: List[Dict] = []
    try:
        matches = search_author_by_name(session, first, last) or []
        # keep top 20
        matches = matches[:20]
        for m in matches:
            candidates.append(_candidate_from_authors_result(m))
    except Exception:
        # swallow this person; the UI will show "No matches found."
        pass

    # Best-first by works_count
    candidates.sort(key=lambda c: c.get("works_count", 0), reverse=True)
    return {"candidates": candidates}

def prefetch_author_candidates_parallel(df: pd.DataFrame, surname_col: str, name_col: str):
    """Fetch candidates for all authors in parallel."""
    st.session_state.author_candidates = {}
    st.session_state.editor_frames = {}
    st.session_state.prefilled = False

    total = len(df)
    progress = st.progress(0)
    status = st.empty()

    sessions = _make_session_pool(WORKERS)

    def job(idx: int, row: pd.Series) -> Tuple[int, Optional[Dict]]:
        surname = str(row[surname_col]).strip() if pd.notna(row[surname_col]) else ""
        name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
        if not surname or not name:
            return idx, None
        s = sessions[idx % WORKERS]
        payload = _fetch_candidates_for_one(s, name, surname)  # first=name, last=surname
        payload.update({
            "input_name": f"{surname}, {name}",            # original file format if ever needed
            "input_name_file_order": f"{name}, {surname}", # for UI headers
            "surname": surname,
            "name": name,
            "selected": []
        })
        return idx, payload

    futures = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, row in df.reset_index(drop=True).iterrows():
            futures.append(ex.submit(job, i, row))

        done = 0
        for fut in as_completed(futures):
            idx, payload = fut.result()
            done += 1
            status.text(f"Fetching candidates… {done}/{total}")
            progress.progress(done / max(total, 1))
            if payload:
                key = _author_input_key(payload["surname"], payload["name"])
                st.session_state.author_candidates[key] = payload
                st.session_state.editor_frames[key] = _build_editor_frame(payload["candidates"], checked_ids=set())

    status.success(f"✅ Found candidates for {len(st.session_state.author_candidates)} authors")
    progress.empty()
    status.empty()
    for s in sessions:
        try:
            s.close()
        except Exception:
            pass


# ---------- Render functions ----------

def render_author_selector():
    st.header("1️⃣ Select Authors")

    # Init UI state (if not already)
    st.session_state.setdefault("author_candidates", {})
    st.session_state.setdefault("editor_frames", {})
    st.session_state.setdefault("prefilled", False)

    uploaded_file = st.file_uploader(
        "Upload Excel file with author names",
        type=["xlsx"],
        help="File must contain 'surname' and 'name' columns.",
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

        # Column mapping (no ORCID)
        st.subheader("Column Mapping")
        columns = list(df.columns)

        col1, col2 = st.columns(2)
        with col1:
            g = auto_detect_column(columns, ["surname", "last_name", "family_name"])
            surname_col = st.selectbox("Surname column", options=columns,
                                       index=columns.index(g) if g else 0)
        with col2:
            g = auto_detect_column(columns, ["name", "first_name", "given_name", "firstname"])
            name_col = st.selectbox("Name column", options=columns,
                                    index=columns.index(g) if g else 0)

        if st.button("🔍 Load candidates", type="primary"):
            prefetch_author_candidates_parallel(df, surname_col, name_col)

        if st.session_state.get("author_candidates"):
            display_author_candidates()

def display_author_candidates():
    st.subheader("Select OpenAlex Profiles for Each Author")

    # NEW: totals and "no match" feedback
    total_names = len(st.session_state.author_candidates)
    unmatched = [
        data["input_name_file_order"]
        for data in st.session_state.author_candidates.values()
        if not data.get("candidates")
    ]
    st.info(f"🧾 Names to review: **{total_names}**  •  No match: **{len(unmatched)}**")

    if unmatched:
        with st.expander(f"Show authors with no matches ({len(unmatched)})", expanded=False):
            # bullet list for readability
            for nm in sorted(unmatched, key=lambda s: s.lower()):
                st.markdown(f"- {nm}")

    # Optional prefill (best match by works_count)
    if st.button("⚡ Prefill best match for each author (optional)"):
        prefill_best_matches()
        st.success("Prefilled best matches. You can adjust before confirming.")
        st.rerun()

    # ---- BIG FORM: ticking inside does NOT rerun ----
    with st.form("authors_selection_form", clear_on_submit=False):
        # Capture editors' return values per author
        form_edits: Dict[str, pd.DataFrame] = {}

        # Sort authors A→Z by name, then surname
        for key, data in sorted(
            st.session_state.author_candidates.items(),
            key=lambda kv: (kv[1]["name"].lower(), kv[1]["surname"].lower())
        ):
            label = f"📝 {data['input_name_file_order']}{_zwsp_salt(key)}"
            with st.expander(label, expanded=False):
                cands = data["candidates"]
                if not cands:
                    st.warning("No matches found.")
                    df_display = _build_editor_frame([], checked_ids=set())
                else:
                    df_display = st.session_state.editor_frames[key]

                edited_df = st.data_editor(
                    df_display,
                    hide_index=True,
                    use_container_width=True,
                    num_rows="fixed",
                    column_order=[
                        "Select", "Name", "ORCID", "Publications",
                        "Affiliations 2025", "Last known institutions", "Topics"
                    ],
                    column_config={
                        "Select": st.column_config.CheckboxColumn("Select", help="Tick to choose this profile"),
                        "Publications": st.column_config.NumberColumn("Publications", format="%d"),
                    },
                    disabled=[
                        "Name", "ORCID", "Publications",
                        "Affiliations 2025", "Last known institutions", "Topics"
                    ],
                    key=f"editor_{key}",
                )
                form_edits[key] = edited_df

        submitted = st.form_submit_button("✅ Confirm ALL selections", type="primary")

    if submitted:
        commit_all_selected_authors(form_edits)
        st.success("All selections confirmed.")
        show_committed_summary()
    else:
        show_committed_summary()

def show_committed_summary():
    st.divider()
    committed_authors = [e for e in st.session_state.get("selected_entities", []) if e.get("type") == "author"]
    committed_profiles = len(committed_authors)
    committed_works = sum((e.get("metadata", {}) or {}).get("works_count", 0) for e in committed_authors)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total matching profiles selected", committed_profiles)
    with c2:
        st.metric("Total corresponding publications", committed_works)


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
        df = st.session_state.editor_frames.get(key)
        if df is None or df.empty or best.get("id", "") not in df.index:
            continue
        df.loc[:, "Select"] = False
        df.loc[best["id"], "Select"] = True
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
                "file_label": f"{data['name']}, {data['surname']}",
                "metadata": {**cand, "input_key": key, "file_surname": data["surname"], "file_name": data["name"]},
            })
