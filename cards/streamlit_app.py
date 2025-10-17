from __future__ import annotations

import io
from typing import Dict

from . import simple_pandas as pd
import streamlit as st

from . import init_db
from .header_mapping import HeaderMappingResult, map_headers
from .processing import DataProcessor


def load_dataframe(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()
    content = uploaded_file.read()
    buffer = io.BytesIO(content)
    if uploaded_file.name.endswith(".xlsx"):
        return pd.read_excel(buffer)
    try:
        return pd.read_csv(io.BytesIO(content))
    except Exception:
        buffer.seek(0)
        return pd.read_csv(io.BytesIO(content), sep="\t")


def header_wizard(df: pd.DataFrame, list_type: str) -> HeaderMappingResult:
    suggestions = map_headers(df.columns, list_type)
    mapping: Dict[str, str] = {}
    st.markdown("### Header Mapping")
    for canonical in suggestions.mapping.keys():
        default = suggestions.mapping[canonical]
        mapping[canonical] = st.selectbox(
            f"Map {canonical}",
            options=list(df.columns),
            index=list(df.columns).index(default) if default in df.columns else 0,
            key=f"{list_type}_{canonical}",
        )
    missing = [field for field in suggestions.missing if field not in mapping]
    return HeaderMappingResult(mapping=mapping, missing=missing, extras=[])


def render():
    st.set_page_config(page_title="Card Printing Table", layout="wide")
    st.title("Card Printing Table")

    init_db()
    processor = DataProcessor()

    if "phase" not in st.session_state:
        st.session_state.phase = "new"
    if "new_df" not in st.session_state:
        st.session_state.new_df = None
    if "re_df" not in st.session_state:
        st.session_state.re_df = None
    if "report" not in st.session_state:
        st.session_state.report = None

    if st.session_state.phase == "new":
        st.header("Phase A: Upload New Visits")
        new_file = st.file_uploader("Upload New Visits", type=["csv", "xlsx", "tsv"], key="new_upload")
        if new_file:
            st.session_state.new_df = load_dataframe(new_file)
            st.success("List 1 (New Visits) received. Ready for List 2 (Reprints)?")
            st.session_state.phase = "reprint"
        st.stop()

    if st.session_state.phase == "reprint":
        st.header("Phase B: Upload Reprints")
        re_file = st.file_uploader("Upload Reprints", type=["csv", "xlsx", "tsv"], key="reprint_upload")
        if re_file:
            st.session_state.re_df = load_dataframe(re_file)
            st.success("List 2 (Reprints) received. Would you like me to process and merge both lists now?")
            if st.button("Process Lists"):
                st.session_state.phase = "process"
        st.stop()

    if st.session_state.phase == "process":
        new_df: pd.DataFrame = st.session_state.new_df
        re_df: pd.DataFrame = st.session_state.re_df
        st.subheader("Header Mapping - New Visits")
        new_mapping = header_wizard(new_df, "new_visit")
        st.subheader("Header Mapping - Reprints")
        re_mapping = header_wizard(re_df, "reprint")
        if st.button("Run Merge"):
            report = processor.process(new_df, re_df, new_mapping, re_mapping)
            st.session_state.report = report
            st.session_state.phase = "review"
        st.stop()

    if st.session_state.phase == "review":
        report = st.session_state.report
        st.header("Review")
        st.write("Summary", report.summary)
        attention = sum(1 for entry in report.normalized_entries if entry.validation_status == "attention")
        normalized = sum(1 for entry in report.normalized_entries if entry.validation_status == "ok")
        st.info(f"{normalized} rows were normalized; {attention} rows require attention.")
        st.write("Issues", report.issues)
        st.write("Duplicates", report.duplicate_reports)

        st.subheader("Combined Master Preview")
        combined_df = pd.DataFrame(
            [
                {
                    "Name": entry.name,
                    "Clinic Name": entry.clinic_name,
                    "Address": entry.address,
                    "City": entry.city,
                    "State": entry.state,
                    "Zip Code": entry.zip_code,
                    "Heally Link": entry.heally_link,
                    "Date and Time": entry.date_time,
                    "List Source": entry.list_source,
                    "Duplicate?": entry.normalized_payload.get("duplicate", "No"),
                }
                for entry in report.normalized_entries
            ]
        )
        st.dataframe(combined_df)

        st.info("Would you like me to export this as a CSV or make any modifications?")
        if st.button("Export CSVs"):
            processor.export(report.normalized_entries, report.batch_id)
            st.success("Exports created. You can download them from the exports directory.")


if __name__ == "__main__":
    render()
