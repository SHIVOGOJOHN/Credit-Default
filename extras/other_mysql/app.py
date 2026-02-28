"""
Deta Bank — Customer Default Data Upload Portal
================================================
Entry point. Handles login and routing to the upload page.
"""

import streamlit as st
from utils.auth import authenticate, logout
from utils.styles import apply_styles
import pages.upload as upload_page

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Deta Bank | Data Portal",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

apply_styles()

# ── Session state defaults ────────────────────────────────────────────────────
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None


# ══════════════════════════════════════════════════════════════════════════════
# LOGIN PAGE
# ══════════════════════════════════════════════════════════════════════════════
def show_login():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div class="login-container">
            <div class="bank-logo">🏦</div>
            <div class="bank-name">DETA BANK</div>
            <div class="portal-subtitle">Data Science Portal</div>
            <div class="divider-line"></div>
        </div>
        """, unsafe_allow_html=True)

        with st.form("login_form", clear_on_submit=False):
            st.markdown('<p class="field-label">USERNAME</p>', unsafe_allow_html=True)
            username = st.text_input("", placeholder="Enter your username",
                                     key="username_input", label_visibility="collapsed")

            st.markdown('<p class="field-label">PASSWORD</p>', unsafe_allow_html=True)
            password = st.text_input("", placeholder="Enter your password",
                                     type="password", key="password_input",
                                     label_visibility="collapsed")

            st.markdown("<br>", unsafe_allow_html=True)
            submitted = st.form_submit_button("SIGN IN →", use_container_width=True)

            if submitted:
                if not username or not password:
                    st.error("Please enter both username and password.")
                elif authenticate(username, password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("⛔ Invalid credentials. Access denied.")

        st.markdown("""
        <div class="login-footer">
            Authorized personnel only · Deta Bank Internal Systems
        </div>
        """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN APP (post-login)
# ══════════════════════════════════════════════════════════════════════════════
def show_app():
    with st.sidebar:
        st.markdown(f"""
        <div class="sidebar-header">
            <div class="sidebar-logo">🏦</div>
            <div class="sidebar-bank">DETA BANK</div>
            <div class="sidebar-user">👤 {st.session_state.username}</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("**Navigation**")
        page = st.radio("", ["📤 Upload Customer Data", "📊 View Uploaded Data"],
                        label_visibility="collapsed")
        st.markdown("---")

        if st.button("🚪 Sign Out", use_container_width=True):
            logout()
            st.rerun()

    if page == "📤 Upload Customer Data":
        upload_page.show()
    elif page == "📊 View Uploaded Data":
        upload_page.show_uploaded()


# ── Router ────────────────────────────────────────────────────────────────────
if st.session_state.authenticated:
    show_app()
else:
    show_login()