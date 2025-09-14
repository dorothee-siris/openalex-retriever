# ui/common.py
"""
Shared UI (Streamlit) for configuration & retrieval.

- Center-panel configuration with a FORM (freeze while adjusting)
- API parameters (experts only) section (always visible)
- Select/Unselect all for Document types and Metadata (within the form)
- Entity-level parallelism (in retrieve_publications)
- Cursor pagination end-to-end; no year chunking
- Phase 3 is gated by an explicit **Start retrieval** button
"""

from __future__ import annotations

import io
import gc
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

from core.api_client import (
    get_session,
    PARALLEL_ENTITIES,
    set_rate_limit,
)
from core.processors import (
    fetch_publications_parallel,
    deduplicate_publications_optimized,
    clean_text_field,
)

CURRENT_YEAR = datetime.now().year

DOCUMENT_TYPES = [
    "article", "book-chapter", "dataset", "preprint", "dissertation", "book",
    "review", "paratext", "libguides", "letter", "other", "reference-entry",
    "report", "editorial", "peer-review", "erratum", "standard", "grant",
    "supplementary-materials", "retraction",
]

METADATA_FIELDS: Dict[str, str] = {
    "id": "OpenAlex ID",
    "doi": "DOI",
    "display_name": "Title",
    "publication_year": "Publication Year",
    "publication_date": "Publication Date",
    "language": "Language",
    "type": "Publication Type",
    "abstract_inverted_index": "Abstract",
    "has_fulltext": "Full Text Available",
    "is_retracted": "Is Retracted",
    "open_access.is_oa": "Is OA",
    "open_access.oa_status": "OA Status",
    "apc_paid.value_usd": "Paid APC in USD",
    "primary_location.source.display_name": "Source",
    "primary_location.source.type": "Source Type",
    "primary_location.source.issn": "ISSN",
    "primary_location.source.host_organization_name": "Publisher",
    "primary_location.pdf_url": "PDF",
    "primary_location.license": "License",
    "cited_by_count": "Citation Count",
    "biblio.volume": "Volume",
    "biblio.issue": "Issue",
    "biblio.first_page": "First Page",
    "biblio.last_page": "Last Page",
}

DEFAULT_METADATA = list(METADATA_FIELDS.keys())  # default: all selected


def ensure_defaults():
    st.session_state.setdefault("selection_mode", "institutions")
    st.session_state.setdefault("selected_entities", [])
    st.session_state.setdefault("config", {
        # Retrieval
        "start_year": CURRENT_YEAR - 5,
        "end_year": CURRENT_YEAR,
        "doc_types": DOCUMENT_TYPES[:],          # UI default = all selected
        "metadata": DEFAULT_METADATA[:],         # UI default = all selected
        "language_filter": "All Languages",      # or "English Only"
        "output_format": "Parquet",              # or "CSV"
        # API params (experts only)
        "requests_per_second": 8.0,
        "max_workers": 10,
        "parallel_entities": PARALLEL_ENTITIES,
        "per_page": 200,
    })


# ---------------- Center-panel CONFIG (form) ----------------

def render_config_section():
    """Center-panel config with a form that applies on click (avoids flicker)."""
    ensure_defaults()
    cfg = st.session_state.config

    st.header("2️⃣ Configure Retrieval Parameters")

    # Seed one-time form state mirrors so +/- clicks don’t live-write into cfg
    mirrors = {
        "cfg_start_year": cfg["start_year"],
        "cfg_end_year": cfg["end_year"],
        "cfg_language": cfg["language_filter"],
        "cfg_output": cfg["output_format"],
        "cfg_doc_types": cfg["doc_types"][:],
        "cfg_metadata": cfg["metadata"][:],
        "cfg_rps": float(cfg.get("requests_per_second", 8.0)),
        "cfg_workers": int(cfg.get("max_workers", 10)),
        "cfg_parallel": int(cfg.get("parallel_entities", PARALLEL_ENTITIES)),
        "cfg_per_page": int(cfg.get("per_page", 200)),
    }
    for k, v in mirrors.items():
        st.session_state.setdefault(k, v)

    with st.form("config_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.number_input("Start Year", min_value=1900, max_value=CURRENT_YEAR, key="cfg_start_year")
        with c2:
            st.number_input("End Year", min_value=1900, max_value=CURRENT_YEAR, key="cfg_end_year")
        with c3:
            st.radio("Language", ["All Languages", "English Only"], key="cfg_language")
        with c4:
            st.radio("Output", ["Parquet", "CSV"], key="cfg_output", help="Parquet compresses better for large files")

        # ---- Document types (with select/unselect all) ----
        st.subheader("Document types")
        dtop, dmid, dbot = st.columns([1, 2, 3])
        with dtop:
            sel_all_dt = st.checkbox("Select all", value=(len(st.session_state.cfg_doc_types) == len(DOCUMENT_TYPES)))
        with dmid:
            unsel_all_dt = st.checkbox("Unselect all", value=(len(st.session_state.cfg_doc_types) == 0))
        if sel_all_dt and not unsel_all_dt:
            st.session_state.cfg_doc_types = DOCUMENT_TYPES[:]
        elif unsel_all_dt and not sel_all_dt:
            st.session_state.cfg_doc_types = []

        doc_cols = st.columns(5)
        new_doc_types = []
        for i, dt in enumerate(DOCUMENT_TYPES):
            with doc_cols[i % 5]:
                if st.checkbox(dt, value=(dt in st.session_state.cfg_doc_types), key=f"cfg_doc_{dt}"):
                    new_doc_types.append(dt)
        st.session_state.cfg_doc_types = new_doc_types

        # ---- Metadata (with select/unselect all) ----
        st.subheader("Metadata fields")
        mtop, mmid, mbot = st.columns([1, 2, 3])
        with mtop:
            sel_all_md = st.checkbox("Select all", value=(set(st.session_state.cfg_metadata) == set(DEFAULT_METADATA)))
        with mmid:
            unsel_all_md = st.checkbox("Unselect all", value=(len(st.session_state.cfg_metadata) == 0))
        if sel_all_md and not unsel_all_md:
            st.session_state.cfg_metadata = DEFAULT_METADATA[:]
        elif unsel_all_md and not sel_all_md:
            st.session_state.cfg_metadata = []

        meta_cols = st.columns(3)
        new_meta = []
        for i, m in enumerate(DEFAULT_METADATA):
            with meta_cols[i % 3]:
                # keep id checked if present in choices
                default_on = (m in st.session_state.cfg_metadata) or (m == "id")
                disabled = (m == "id")
                if st.checkbox(METADATA_FIELDS.get(m, m), value=default_on, disabled=disabled, key=f"cfg_meta_{m}"):
                    new_meta.append(m)
        # ensure id is always present
        if "id" not in new_meta:
            new_meta.insert(0, "id")
        st.session_state.cfg_metadata = new_meta

        # ---- API parameters (experts only) ----
        st.subheader("API parameters (experts only)")
        e1, e2, e3, e4 = st.columns(4)
        with e1:
            st.slider("Requests per second", 1.0, 30.0, float(st.session_state.cfg_rps), 0.5, key="cfg_rps")
        with e2:
            st.slider("Max workers per entity", 1, 32, int(st.session_state.cfg_workers), key="cfg_workers")
        with e3:
            st.slider("Parallel entities", 1, 16, int(st.session_state.cfg_parallel), key="cfg_parallel")
        with e4:
            st.selectbox(
                "Results per page",
                [50, 100, 200],
                index=[50, 100, 200].index(int(st.session_state.cfg_per_page)),
                key="cfg_per_page",
            )

        applied = st.form_submit_button("Apply configuration", type="primary")

    if applied:
        # Basic validation
        if st.session_state.cfg_start_year > st.session_state.cfg_end_year:
            st.error("Start year cannot be after end year.")
            return

        # If user selected all doc types, store [] to mean "no filtering" (most efficient).
        doc_types_to_store = st.session_state.cfg_doc_types[:]
        if len(doc_types_to_store) == len(DOCUMENT_TYPES):
            doc_types_to_store = []

        cfg.update({
            "start_year": int(st.session_state.cfg_start_year),
            "end_year": int(st.session_state.cfg_end_year),
            "language_filter": st.session_state.cfg_language,
            "output_format": st.session_state.cfg_output,
            "doc_types": doc_types_to_store,
            "metadata": st.session_state.cfg_metadata[:],
            "requests_per_second": float(st.session_state.cfg_rps),
            "max_workers": int(st.session_state.cfg_workers),
            "parallel_entities": int(st.session_state.cfg_parallel),
            "per_page": int(st.session_state.cfg_per_page),
        })
        st.success("Configuration applied.")


# ---------------- Retrieval gating (Phase 3) ----------------

def render_retrieval_section():
    """Phase 3: gated by an explicit button; no automatic start."""
    ensure_defaults()
    entities: List[Dict] = st.session_state.get("selected_entities", [])
    st.header("3️⃣ Retrieve Publications")
    if not entities:
        st.info("Select at least one institution or author in Phase 1.")
        return

    st.write(f"**Selected entities:** {len(entities)}")
    start = st.button("▶️ Start retrieval", type="primary")
    if start:
        retrieve_publications()
    else:
        st.info("Adjust configuration above, then press **Start retrieval** to begin.")


# ---------------- Retrieval core ----------------

def _safe_avg_works(e: Dict) -> float:
    """Return a numeric avg_works_per_year or 0.0 if missing/invalid."""
    try:
        v = (e.get("metadata") or {}).get("avg_works_per_year")
        if v is None:
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0

def _estimate_and_warn_if_large():
    if st.session_state.get("selection_mode") != "institutions":
        return
    entities = st.session_state.get("selected_entities") or []
    if not entities:
        return
    total_avg_works = sum(_safe_avg_works(e) for e in entities)
    if total_avg_works <= 0:
        return
    cfg = st.session_state.config
    years_span = cfg["end_year"] - cfg["start_year"] + 1
    estimated = total_avg_works * years_span
    if estimated > 100_000:
        st.warning(
            f"⚠️ The file might contain more than {estimated:,.0f} publications. Consider:\n"
            "- Filtering document types\n"
            "- Selecting fewer metadata fields\n"
            "- Removing abstracts\n"
            "- Choosing Parquet format"
        )

def retrieve_publications():
    ensure_defaults()
    cfg = st.session_state.config
    entities: List[Dict] = st.session_state.get("selected_entities", [])
    if not entities:
        st.info("Select at least one institution or author to begin.")
        return
    if cfg["start_year"] > cfg["end_year"]:
        st.error("Start year cannot be after end year.")
        return

    # Always apply API params from config
    set_rate_limit(cfg.get("requests_per_second", 8.0))
    parallel_entities = cfg.get("parallel_entities", PARALLEL_ENTITIES)
    per_page = cfg.get("per_page", 200)
    max_workers = cfg.get("max_workers", None)

    entity_label = "institutions" if st.session_state.get("selection_mode") == "institutions" else "profiles"

    # Progress UI
    start_time = time.time()
    progress_bar = st.progress(0.0)
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()
    pubs_placeholder = st.empty()
    timer_placeholder = st.empty()

    _estimate_and_warn_if_large()

    def job(entity: Dict) -> Tuple[str, List[Dict]]:
        s = get_session()
        try:
            lang_filter = "english_only" if cfg["language_filter"] == "English Only" else "all_languages"
            name_for_output = entity["label"] if entity.get("type") == "institution" else entity.get("file_label", entity["label"])
            pubs = fetch_publications_parallel(
                s,
                entity["id"],
                name_for_output,
                entity.get("type", "institution"),
                cfg["start_year"],
                cfg["end_year"],
                cfg.get("doc_types", []),
                cfg.get("metadata", DEFAULT_METADATA),
                lang_filter,
                max_workers=max_workers,
                per_page=per_page,
            )
            return entity.get("label", "(unknown)"), pubs
        finally:
            try:
                s.close()
            except Exception:
                pass

    all_publications: List[Dict] = []
    done = 0
    total = len(entities)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    status_placeholder.info(f"Fetching: {total} {entity_label} in parallel…")
    progress_bar.progress(0.02)

    with ThreadPoolExecutor(max_workers=parallel_entities) as ex:
        futures = [ex.submit(job, e) for e in entities]
        for fut in as_completed(futures):
            label, pubs = fut.result()
            done += 1
            all_publications.extend(pubs or [])

            progress_bar.progress(done / max(total, 1))
            metrics_placeholder.metric(
                label="Progress",
                value=f"{done}/{total} {entity_label}",
                delta=f"{len(all_publications):,} publications fetched (pre-dedup)",
            )
            pubs_placeholder.info(f"📄 Publications fetched so far: {len(all_publications):,}")
            elapsed = time.time() - start_time
            timer_placeholder.info(f"⏱️ Time elapsed: {str(timedelta(seconds=int(elapsed)))}")

    progress_bar.progress(0.9)
    status_placeholder.info("Deduplicating publications…")

    if not all_publications:
        st.warning("No publications found for the selected criteria.")
        return

    total_before = len(all_publications)
    merged_publications = deduplicate_publications_optimized(all_publications)
    del all_publications
    gc.collect()

    duplicates_removed = total_before - len(merged_publications)
    df_output = pd.DataFrame(merged_publications)

    # Clean text columns
    for col in df_output.columns:
        if df_output[col].dtype == "object":
            df_output[col] = df_output[col].apply(lambda x: clean_text_field(x) if isinstance(x, str) else x)

    # Column ordering
    columns_order = ["id"] + [c for c in cfg.get("metadata", DEFAULT_METADATA) if c != "id"]
    columns_order.extend(["institutions_extracted", "authors_extracted", "position_extracted"])
    columns_order = [c for c in columns_order if c in df_output.columns]
    if columns_order:
        df_output = df_output[columns_order]

    # Friendly names
    column_mapping = {field: METADATA_FIELDS.get(field, field) for field in df_output.columns}
    column_mapping.update({
        "institutions_extracted": "Institutions Extracted",
        "authors_extracted": "Authors Extracted",
        "position_extracted": "Author Position",
    })
    df_output = df_output.rename(columns=column_mapping)

    # Export
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    num_entities = total
    entity_type = "institutions" if st.session_state.get("selection_mode") == "institutions" else "profiles"

    if cfg.get("output_format") == "CSV":
        filename = f"pubs_{num_entities}_{entity_type}_{timestamp}.csv"
        csv_data = df_output.to_csv(index=False).encode("utf-8")
        st.success(
            f"✅ Retrieved {len(df_output):,} unique publications from {num_entities} {entity_type}. "
            f"Removed {duplicates_removed:,} duplicates."
        )
        st.download_button(
            label=f"📥 Download {filename}",
            data=csv_data,
            file_name=filename,
            mime="text/csv",
            type="primary",
        )
    else:
        filename = f"pubs_{num_entities}_{entity_type}_{timestamp}.parquet"
        pq = io.BytesIO()
        df_output.to_parquet(pq, index=False, compression="snappy")
        data = pq.getvalue()
        st.success(
            f"✅ Retrieved {len(df_output):,} unique publications from {num_entities} {entity_type}. "
            f"Removed {duplicates_removed:,} duplicates."
        )
        st.download_button(
            label=f"📥 Download {filename}",
            data=data,
            file_name=filename,
            mime="application/octet-stream",
            type="primary",
        )

    del df_output
    gc.collect()
