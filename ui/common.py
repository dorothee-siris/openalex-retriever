# ui/common.py
"""
Shared UI (Streamlit) for configuration & retrieval.

Changes:
- **Expert mode** in the sidebar to tune:
  - requests per second (global)
  - per-entity max workers (doc-type fan-out)
  - parallel entities (UI-level pool)
  - per_page (50/100/200)
- Entity-level parallelism lives in the UI (safe main-thread updates).
- Per-entity doc-type parallelism remains in core.processors.
- Cursor pagination end-to-end (no 10k cap), no year chunking.
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
    set_rate_limit,       # runtime RPS knob
)
from core.processors import (
    fetch_publications_parallel,
    deduplicate_publications_optimized,
    clean_text_field,
)

# -------------------- Constants & defaults --------------------

CURRENT_YEAR = datetime.now().year

DOCUMENT_TYPES = [
    "article", "book-chapter", "dataset", "preprint", "dissertation", "book",
    "review", "paratext", "libguides", "letter", "other", "reference-entry",
    "report", "editorial", "peer-review", "erratum", "standard", "grant",
    "supplementary-materials", "retraction",
]

# Display names for export columns (subset elsewhere is OK)
METADATA_FIELDS: Dict[str, str] = {
    # Core Publication Information
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

    # Access Information
    "open_access.is_oa": "Is OA",
    "open_access.oa_status": "OA Status",
    "apc_paid.value_usd": "Paid APC in USD",

    # Publication Source
    "primary_location.source.display_name": "Source",
    "primary_location.source.type": "Source Type",
    "primary_location.source.issn": "ISSN",
    "primary_location.source.host_organization_name": "Publisher",
    "primary_location.pdf_url": "PDF",
    "primary_location.license": "License",

    # Aggregates / counts
    "cited_by_count": "Citation Count",
    "biblio.volume": "Volume",
    "biblio.issue": "Issue",
    "biblio.first_page": "First Page",
    "biblio.last_page": "Last Page",
}

DEFAULT_METADATA = [
    "id", "doi", "display_name", "publication_year", "type",
    "cited_by_count", "publication_date",
]

# -------------------- Config UI helpers --------------------

def ensure_defaults():
    """Seed Streamlit session defaults used across pages."""
    st.session_state.setdefault("selection_mode", "institutions")  # or "authors"
    st.session_state.setdefault("selected_entities", [])
    st.session_state.setdefault("config", {
        "start_year": CURRENT_YEAR - 5,
        "end_year": CURRENT_YEAR,
        "doc_types": [],                 # [] means "all"
        "metadata": DEFAULT_METADATA.copy(),
        "language_filter": "All Languages",  # or "English Only"
        "output_format": "Parquet",     # or "CSV"
        # expert mode fields will be added when toggled
    })


def render_config_sidebar():
    """Render the sidebar with retrieval settings and save to st.session_state.config."""
    ensure_defaults()
    cfg = st.session_state.config

    with st.sidebar:
        st.header("⚙️ Retrieval Settings")

        y1, y2 = st.columns(2)
        with y1:
            start_year = st.number_input("Start year", min_value=1900, max_value=CURRENT_YEAR, value=int(cfg["start_year"]))
        with y2:
            end_year = st.number_input("End year", min_value=1900, max_value=CURRENT_YEAR, value=int(cfg["end_year"]))

        st.markdown("**Document types** (empty = all)")
        selected_types = st.multiselect(" ", options=DOCUMENT_TYPES, default=cfg.get("doc_types", []))

        st.markdown("**Metadata fields** (export columns)")
        selected_metadata = st.multiselect(" ", options=list(METADATA_FIELDS.keys()), default=cfg.get("metadata", DEFAULT_METADATA))

        language_filter = st.radio("Language", ["All Languages", "English Only"], index=(0 if cfg.get("language_filter") != "English Only" else 1))
        output_format = st.radio("Output format", ["Parquet", "CSV"], index=(0 if cfg.get("output_format") != "CSV" else 1))

        # Persist basic config
        cfg.update({
            "start_year": int(start_year),
            "end_year": int(end_year),
            "doc_types": selected_types,
            "metadata": selected_metadata,
            "language_filter": language_filter,
            "output_format": output_format,
        })

        st.caption("Tip: fewer metadata fields and Parquet output produce smaller files.")

        # ---- Expert mode ----
        st.markdown("---")
        expert = st.checkbox("Activate expert mode")
        cfg["expert_mode"] = bool(expert)

        if expert:
            st.caption("Tune advanced parameters. Be polite with OpenAlex: keep a valid mailto and sensible RPS.")
            rps = st.slider("Max requests per second (global)", 1.0, 30.0, float(cfg.get("requests_per_second", 8.0)), 0.5)
            mw  = st.slider("Max workers per entity (doc types)", 1, 32, int(cfg.get("max_workers", 10)))
            pe  = st.slider("Parallel entities", 1, 16, int(cfg.get("parallel_entities", PARALLEL_ENTITIES)))
            pp  = st.selectbox("Results per page", options=[50, 100, 200], index=[50, 100, 200].index(int(cfg.get("per_page", 200))))
            cfg.update({
                "requests_per_second": float(rps),
                "max_workers": int(mw),
                "parallel_entities": int(pe),
                "per_page": int(pp),
            })


# -------------------- Retrieval orchestration --------------------

def _estimate_and_warn_if_large():
    """Optional soft estimate to warn for huge downloads (institutions only)."""
    if st.session_state.get("selection_mode") != "institutions":
        return
    entities = st.session_state.get("selected_entities") or []
    total_avg_works = sum(e.get("metadata", {}).get("avg_works_per_year", 0) for e in entities)
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
            "- Removing abstracts (can save up to 40% space)\n"
            "- Choosing Parquet format for better compression"
        )


def retrieve_publications():
    """Main retrieval entry point (entity-level parallelism + safe main-thread UI updates)."""
    ensure_defaults()
    cfg = st.session_state.config
    entities: List[Dict] = st.session_state.get("selected_entities", [])
    if not entities:
        st.info("Select at least one institution or author to begin.")
        return
    if cfg["start_year"] > cfg["end_year"]:
        st.error("Start year cannot be after end year.")
        return

    entity_label = "institutions" if st.session_state.get("selection_mode") == "institutions" else "profiles"

    # Apply expert settings (if any)
    if cfg.get("expert_mode"):
        set_rate_limit(cfg.get("requests_per_second", 8.0))
        parallel_entities = cfg.get("parallel_entities", PARALLEL_ENTITIES)
        per_page = cfg.get("per_page", 200)
        max_workers = cfg.get("max_workers", None)
    else:
        parallel_entities = PARALLEL_ENTITIES
        per_page = 200
        max_workers = None

    # Top-of-page progress widgets
    start_time = time.time()
    progress_bar = st.progress(0.0)
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()
    pubs_placeholder = st.empty()
    timer_placeholder = st.empty()

    _estimate_and_warn_if_large()

    # One job per entity: its own pooled session
    def job(entity: Dict) -> Tuple[str, List[Dict]]:
        s = get_session()
        try:
            lang_filter = "english_only" if cfg["language_filter"] == "English Only" else "all_languages"
            name_for_output = (
                entity["label"] if entity.get("type") == "institution"
                else entity.get("file_label", entity["label"])
            )
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

    # Fan out entities in parallel
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

            # Safe UI updates (main thread)
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

    # ------------- Deduplication & Export -------------
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

    # Column ordering (id first, then chosen metadata, then extracted fields if present)
    columns_order = ["id"] + [c for c in cfg.get("metadata", DEFAULT_METADATA) if c != "id"]
    columns_order.extend(["institutions_extracted", "authors_extracted", "position_extracted"])  # if present
    columns_order = [c for c in columns_order if c in df_output.columns]
    if columns_order:
        df_output = df_output[columns_order]

    # Human-friendly column names
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
    else:  # Parquet
        filename = f"pubs_{num_entities}_{entity_type}_{timestamp}.parquet"
        parquet_buffer = io.BytesIO()
        df_output.to_parquet(parquet_buffer, index=False, compression="snappy")
        parquet_data = parquet_buffer.getvalue()
        file_size_mb = len(parquet_data) / (1024 * 1024)

        st.success(
            f"✅ Retrieved {len(df_output):,} unique publications from {num_entities} {entity_type}. "
            f"Removed {duplicates_removed:,} duplicates. File size: {file_size_mb:.1f} MB"
        )
        st.download_button(
            label=f"📥 Download {filename}",
            data=parquet_data,
            file_name=filename,
            mime="application/octet-stream",
            type="primary",
        )

    # Final cleanup
    del df_output
    gc.collect()
