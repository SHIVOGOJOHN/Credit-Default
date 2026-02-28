"""
utils/auth.py
--------------
Handles login authentication using credentials stored in .env file.

.env format:
    DS_USERS=alice:pass123,bob:securepass,carol:mypassword

Each user has their own username:password pair separated by commas.
"""

import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def _get_users() -> dict:
    """
    Parse DS_USERS from .env into a dict of {username: password}.

    .env example:
        DS_USERS=alice:pass123,bob:securepass,carol:mypassword
    """
    raw = os.getenv("DS_USERS", "")
    users = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            username, password = entry.split(":", 1)
            users[username.strip()] = password.strip()
    return users


def authenticate(username: str, password: str) -> bool:
    """
    Returns True if username/password match a registered data scientist.
    """
    users = _get_users()
    return users.get(username) == password


def logout():
    """Clear session state to log the user out."""
    st.session_state.authenticated = False
    st.session_state.username = None