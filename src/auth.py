"""Authentication: users stored in Neo4j, salted-hash passwords, in-memory sessions.

- Passwords are stored as PBKDF2-HMAC-SHA256 with a per-user salt (never plaintext).
- Users live in Neo4j (:User {username, password}) when NEO4J_URI is set; otherwise
  an in-memory store is used so local dev works without the graph.
- Sessions are server-side tokens (set as an HttpOnly cookie). Each session also
  carries the NL-to-SQL conversation history for that login.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time

import config

_ITER = 200_000
_SESSIONS: dict[str, dict] = {}     # token -> {username, created, history:[...]}
_MEM_USERS: dict[str, str] = {}     # username -> stored hash (fallback store)


# ------------------------------ password hashing ----------------------------

def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), _ITER).hex()
    return f"pbkdf2${salt}${h}"


def _verify_hash(password: str, stored: str) -> bool:
    try:
        _, salt, _h = stored.split("$")
    except ValueError:
        return False
    return hmac.compare_digest(hash_password(password, salt), stored)


# ------------------------------- user store ---------------------------------

def _driver():
    import knowledge_graph
    return knowledge_graph._driver() if knowledge_graph.available() else None


def create_user(username: str, password: str) -> None:
    stored = hash_password(password)
    drv = _driver()
    if drv:
        try:
            with drv.session() as s:
                s.run("MERGE (u:User {username:$u}) SET u.password=$p", u=username, p=stored)
        finally:
            drv.close()
    else:
        _MEM_USERS[username] = stored


def get_user_hash(username: str) -> str | None:
    drv = _driver()
    if drv:
        try:
            with drv.session() as s:
                rec = s.run("MATCH (u:User {username:$u}) RETURN u.password AS p", u=username).single()
                return rec["p"] if rec else None
        finally:
            drv.close()
    return _MEM_USERS.get(username)


def verify(username: str, password: str) -> bool:
    stored = get_user_hash(username)
    return bool(stored) and _verify_hash(password, stored)


def seed_default_user() -> str:
    """Ensure a default login exists (APP_USER / APP_PASSWORD, default admin/admin123)."""
    u = os.environ.get("APP_USER", "admin")
    p = os.environ.get("APP_PASSWORD", "admin123")
    if not get_user_hash(u):
        create_user(u, p)
    return u


# -------------------------------- sessions -----------------------------------

def new_session(username: str) -> str:
    token = secrets.token_urlsafe(24)
    _SESSIONS[token] = {"username": username, "created": time.time(), "history": []}
    return token


def get_session(token: str | None) -> dict | None:
    return _SESSIONS.get(token) if token else None


def end_session(token: str | None) -> None:
    if token:
        _SESSIONS.pop(token, None)


def add_history(token: str, entry: dict) -> None:
    sess = _SESSIONS.get(token)
    if sess is not None:
        utc = config.now_utc_iso()                  # stored in UTC (GMT)
        entry = {**entry, "ts_utc": utc, "ts": config.to_display(utc)}  # shown in display tz
        sess["history"].append(entry)


def history(token: str | None) -> list:
    sess = get_session(token)
    return sess["history"] if sess else []
