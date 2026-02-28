"""
pages/upload.py
----------------
Main data upload interface for authenticated data scientists.
Supports CSV upload → DataFrame preview → validation → MySQL load.
"""

import uuid
import streamlit as st
import pandas as pd
from datetime import datetime

from utils.db import load_dataframe_to_mysql, test_connection, get_recent_uploads

# Expected columns for customer default data (flexible — warnings not blocks)
EXPECTED_COLUMNS = [
    "customer_id", "age", "gender", "income", "loan_amount",
    "loan_term", "credit_score", "default_flag"
]

TARGET_TABLE = "customer_default_data"


def render_upload_page():
    # ── Top navigation bar ────────────────────────────────────────────────────
    st.markdown(f"""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 1rem 2rem;
        background: rgba(15,32,64,0.8);
        border-bottom: 1px solid rgba(201,168,76,0.2);
        backdrop-filter: blur(20px);
        margin: -1rem -1rem 2rem;
        position: sticky; top: 0; z-index: 100;
    ">
        <div style="display:flex; align-items:center; gap:0.75rem;">
            <span style="font-size:1.4rem;">🏦</span>
            <span style="
                font-family: 'DM Serif Display', serif;
                font-size: 1.1rem;
                color: #f5f0e8;
                letter-spacing: -0.01em;
            ">Deta Bank</span>
            <span style="
                font-family: 'DM Mono', monospace;
                font-size: 0.65rem;
                color: rgba(201,168,76,0.7);
                letter-spacing: 0.15em;
                text-transform: uppercase;
                border-left: 1px solid rgba(201,168,76,0.3);
                padding-left: 0.75rem;
                margin-left: 0.25rem;
            ">Data Portal</span>
        </div>
        <div style="display:flex; align-items:center; gap:1.5rem;">
            <span style="
                font-family: 'DM Mono', monospace;
                font-size: 0.7rem;
                color: rgba(245,240,232,0.5);
                letter-spacing: 0.05em;
            ">Signed in as <span style="color:#c9a84c;">{st.session_state.username}</span>
            · {st.session_state.role}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Page title ─────────────────────────────────────────────────────────────
    col_title, col_logout = st.columns([5, 1])
    with col_title:
        st.markdown("""
        <h2 style="
            font-family: 'DM Serif Display', serif;
            font-size: 1.8rem;
            color: #f5f0e8;
            margin: 0 0 0.3rem;
            letter-spacing: -0.02em;
        ">Customer Default Data Upload</h2>
        <p style="
            font-family: 'DM Sans', sans-serif;
            font-size: 0.85rem;
            color: rgba(245,240,232,0.5);
            margin: 0 0 2rem;
        ">Upload new batch data to the credit risk database</p>
        """, unsafe_allow_html=True)

    with col_logout:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Sign Out", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.session_state.role = None
            st.rerun()

    # ── DB Connection Status ───────────────────────────────────────────────────
    db_ok, db_msg = test_connection()
    if db_ok:
        st.markdown(f"""
        <div style="
            display: inline-flex; align-items: center; gap: 0.5rem;
            background: rgba(26,107,58,0.15);
            border: 1px solid rgba(26,107,58,0.4);
            border-radius: 6px; padding: 0.4rem 1rem;
            font-family: 'DM Mono', monospace;
            font-size: 0.72rem; color: #4caf7d;
            letter-spacing: 0.05em;
            margin-bottom: 1.5rem;
        ">
            <span style="width:7px;height:7px;border-radius:50%;
                background:#4caf7d;display:inline-block;
                box-shadow:0 0 6px #4caf7d;"></span>
            MySQL Connected · {db_msg}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.error(f"⚠️ Database connection failed: {db_msg}")
        st.stop()

    st.markdown("---")

    # ── Two column layout ──────────────────────────────────────────────────────
    left, right = st.columns([1.4, 1], gap="large")

    with left:
        # ── Step 1: Upload ─────────────────────────────────────────────────────
        st.markdown("""
        <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
            letter-spacing:0.15em;text-transform:uppercase;
            color:rgba(201,168,76,0.8);margin-bottom:0.5rem;">
            Step 01 — Upload File
        </p>
        """, unsafe_allow_html=True)

        uploaded_file = st.file_uploader(
            "Drop a CSV file with customer default data",
            type=["csv"],
            help="Accepted format: CSV with headers. Max size: 200MB"
        )

        if uploaded_file is not None:
            # ── Step 2: Preview & Validate ─────────────────────────────────────
            df = pd.read_csv(uploaded_file)

            st.markdown(f"""
            <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
                letter-spacing:0.15em;text-transform:uppercase;
                color:rgba(201,168,76,0.8);margin:1.5rem 0 0.5rem;">
                Step 02 — Preview & Validate
            </p>
            """, unsafe_allow_html=True)

            # File stats
            col_a, col_b, col_c = st.columns(3)
            _stat_card(col_a, "Rows",    f"{len(df):,}")
            _stat_card(col_b, "Columns", f"{len(df.columns)}")
            _stat_card(col_c, "Nulls",   f"{df.isna().sum().sum():,}")

            st.markdown("<br>", unsafe_allow_html=True)

            # Column warnings
            missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
            extra_cols   = [c for c in df.columns if c not in EXPECTED_COLUMNS]

            if missing_cols:
                st.warning(f"⚠️ Expected columns not found: `{'`, `'.join(missing_cols)}`")
            if extra_cols:
                st.info(f"ℹ️ Extra columns detected (will still be uploaded): `{'`, `'.join(extra_cols)}`")

            # Data preview
            st.markdown("""
            <p style="font-family:'DM Mono',monospace;font-size:0.68rem;
                color:rgba(245,240,232,0.4);letter-spacing:0.08em;
                margin-bottom:0.3rem;">DATA PREVIEW (first 5 rows)</p>
            """, unsafe_allow_html=True)
            st.dataframe(
                df.head(5),
                use_container_width=True,
                hide_index=True
            )

            # Null analysis
            with st.expander("📊 Null value analysis per column"):
                null_df = df.isna().sum().reset_index()
                null_df.columns = ["Column", "Null Count"]
                null_df["Null %"] = (null_df["Null Count"] / len(df) * 100).round(2)
                null_df = null_df[null_df["Null Count"] > 0]
                if len(null_df):
                    st.dataframe(null_df, use_container_width=True, hide_index=True)
                else:
                    st.success("No null values found in any column!")

            st.markdown("---")

            # ── Step 3: Upload options ─────────────────────────────────────────
            st.markdown("""
            <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
                letter-spacing:0.15em;text-transform:uppercase;
                color:rgba(201,168,76,0.8);margin-bottom:0.5rem;">
                Step 03 — Configure & Upload
            </p>
            """, unsafe_allow_html=True)

            table_name = st.text_input(
                "Target Table Name",
                value=TARGET_TABLE,
                help="The MySQL table to load data into"
            )

            handle_nulls = st.selectbox(
                "Handle null values",
                ["Keep as NULL in database",
                 "Drop rows with any null",
                 "Fill nulls with 0 / empty string"]
            )

            # Apply null handling
            df_to_upload = df.copy()
            if handle_nulls == "Drop rows with any null":
                before = len(df_to_upload)
                df_to_upload = df_to_upload.dropna()
                st.info(f"Dropped {before - len(df_to_upload):,} rows with nulls. {len(df_to_upload):,} rows remain.")
            elif handle_nulls == "Fill nulls with 0 / empty string":
                num_cols = df_to_upload.select_dtypes(include="number").columns
                str_cols = df_to_upload.select_dtypes(include="object").columns
                df_to_upload[num_cols] = df_to_upload[num_cols].fillna(0)
                df_to_upload[str_cols] = df_to_upload[str_cols].fillna("")

            batch_id = str(uuid.uuid4())[:8].upper()
            st.markdown(f"""
            <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
                color:rgba(245,240,232,0.4);">
                Batch ID: <span style="color:#c9a84c;">{batch_id}</span>
                · Uploaded by: <span style="color:#c9a84c;">{st.session_state.username}</span>
            </p>
            """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Upload Button ──────────────────────────────────────────────────
            if st.button(f"⬆ Upload {len(df_to_upload):,} Rows to MySQL", use_container_width=True):
                with st.spinner("Loading data to MySQL..."):
                    success, message, rows_inserted = load_dataframe_to_mysql(
                        df=df_to_upload,
                        table_name=table_name,
                        uploaded_by=st.session_state.username,
                        batch_id=batch_id,
                    )

                if success:
                    st.success(f"✅ Successfully uploaded **{rows_inserted:,} rows** to `{table_name}` (Batch: {batch_id})")
                    st.balloons()
                else:
                    st.error(f"❌ Upload failed: {message}")

    # ── Right panel: audit log ─────────────────────────────────────────────────
    with right:
        st.markdown("""
        <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
            letter-spacing:0.15em;text-transform:uppercase;
            color:rgba(201,168,76,0.8);margin-bottom:0.5rem;">
            Recent Uploads
        </p>
        """, unsafe_allow_html=True)

        audit_df = get_recent_uploads(TARGET_TABLE)
        if not audit_df.empty:
            st.dataframe(audit_df, use_container_width=True, hide_index=True)
        else:
            st.markdown("""
            <div style="
                background: rgba(255,255,255,0.02);
                border: 1px dashed rgba(201,168,76,0.2);
                border-radius: 8px; padding: 2rem;
                text-align: center;
                font-family: 'DM Mono', monospace;
                font-size: 0.75rem;
                color: rgba(245,240,232,0.3);
            ">No uploads yet.<br>Upload history will appear here.</div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Expected schema reference
        st.markdown("""
        <p style="font-family:'DM Mono',monospace;font-size:0.7rem;
            letter-spacing:0.15em;text-transform:uppercase;
            color:rgba(201,168,76,0.8);margin-bottom:0.5rem;">
            Expected Schema
        </p>
        """, unsafe_allow_html=True)

        schema_data = {
            "Column": EXPECTED_COLUMNS,
            "Type":   ["INT","INT","VARCHAR","FLOAT","FLOAT",
                       "INT","INT","TINYINT(1)"]
        }
        st.dataframe(pd.DataFrame(schema_data), use_container_width=True, hide_index=True)


def _stat_card(col, label: str, value: str):
    """Render a small metric card."""
    with col:
        st.markdown(f"""
        <div style="
            background: rgba(201,168,76,0.07);
            border: 1px solid rgba(201,168,76,0.2);
            border-radius: 8px; padding: 0.75rem 1rem;
            text-align: center;
        ">
            <div style="font-family:'DM Mono',monospace;font-size:0.65rem;
                color:rgba(245,240,232,0.4);letter-spacing:0.1em;
                text-transform:uppercase;">{label}</div>
            <div style="font-family:'DM Serif Display',serif;font-size:1.5rem;
                color:#c9a84c;margin-top:0.2rem;">{value}</div>
        </div>
        """, unsafe_allow_html=True)