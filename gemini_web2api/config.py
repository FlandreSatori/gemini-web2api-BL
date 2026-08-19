"""Configuration management."""
import json
import os
import threading
from contextvars import ContextVar

DEFAULT_CONFIG = {
    "port": 8081,
    "host": "0.0.0.0",
    "retry_attempts": 3,
    "retry_delay_sec": 2,
    "rate_limit_cooldown_sec": 60,
    "rate_limit_max_cooldown_sec": 900,
    "request_timeout_sec": 180,
    "gemini_bl": "boq_assistant-bard-web-server_20260716.08_p0",
    "auth_user": None,
    "xsrf_token": None,
    "default_model": "gemini-3.6-flash",
    "log_requests": True,
    "cookie_file": None,
    "proxy": None,
    "api_keys": [],
    "temporary_chats": False,
    "bl_retry_delay_sec": 10,
}

CONFIG = dict(DEFAULT_CONFIG)
_ACTIVE_CONFIG = ContextVar("gemini_active_config", default=CONFIG)
_BL_LOCK = threading.Lock()
_BL_READY = threading.Event()
_BL_READY.set()
_SHARED_BL = CONFIG["gemini_bl"]


def current_config():
    """Return the configuration bound to the current request thread."""
    return _ACTIVE_CONFIG.get()


def bind_config(config: dict):
    """Bind a per-user configuration and return its reset token."""
    return _ACTIVE_CONFIG.set(config)


def reset_config(token):
    _ACTIVE_CONFIG.reset(token)


def shared_bl() -> str:
    with _BL_LOCK:
        return _SHARED_BL


def set_shared_bl(value: str) -> None:
    global _SHARED_BL
    with _BL_LOCK:
        _SHARED_BL = value
    CONFIG["gemini_bl"] = value


def invalidate_bl() -> None:
    _BL_READY.clear()


def wait_for_bl() -> None:
    _BL_READY.wait()


def mark_bl_ready(value: str) -> None:
    set_shared_bl(value)
    _BL_READY.set()


def load_config(path: str = None):
    """Load config from JSON file."""
    if path and os.path.exists(path):
        with open(path) as f:
            CONFIG.update(json.load(f))
    return CONFIG


def find_config():
    """Search for config file in standard locations."""
    for p in ["./config.json", os.path.expanduser("~/.config/gemini-web2api/config.json")]:
        if os.path.exists(p):
            return p
    return None
