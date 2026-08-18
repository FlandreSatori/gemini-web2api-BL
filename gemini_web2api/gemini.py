"""Gemini StreamGenerate protocol implementation with httpx streaming."""
import json
import time
import uuid
import re
import urllib.request
import urllib.parse
import ssl
import os
import hashlib
import threading

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

from .config import (current_config, invalidate_bl, mark_bl_ready,
                     shared_bl, wait_for_bl)

_ssl_ctx = None
_cookie_cache = {"path": None, "str": "", "sapisid": None, "mtime": 0}
_httpx_client = None
_bl_update_lock = threading.Lock()
_bl_pattern = re.compile(r'boq_assistant-bard-web-server_(\d{8})\.(\d+)_p(\d+)')


def log(msg: str):
    if current_config()["log_requests"]:
        import sys
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
        sys.stderr.flush()


def _get_ssl_ctx():
    global _ssl_ctx
    if _ssl_ctx is None:
        _ssl_ctx = ssl.create_default_context()
    return _ssl_ctx


def _get_httpx_client():
    global _httpx_client
    if _httpx_client is None and HAS_HTTPX:
        proxy = current_config().get("proxy")
        transport = httpx.HTTPTransport(proxy=proxy) if proxy else None
        _httpx_client = httpx.Client(transport=transport, timeout=current_config()["request_timeout_sec"], verify=True)
    return _httpx_client


def load_cookie() -> tuple:
    """Load cookie from file with mtime-based caching."""
    cookie_file = current_config().get("cookie_file")
    if not cookie_file or not os.path.exists(cookie_file):
        return "", None
    try:
        mtime = os.path.getmtime(cookie_file)
        if (cookie_file == _cookie_cache["path"]
            and mtime == _cookie_cache["mtime"] and _cookie_cache["str"]):
            return _cookie_cache["str"], _cookie_cache["sapisid"]
        with open(cookie_file, "r") as f:
            content = f.read().strip()
        if content.startswith("{"):
            data = json.loads(content)
            cookie_str = data.get("cookie", "")
            sapisid = data.get("sapisid", "")
        else:
            cookie_str = content
            pairs = dict(p.split("=", 1) for p in cookie_str.split("; ") if "=" in p)
            sapisid = pairs.get("SAPISID", "")
        _cookie_cache.update({"path": cookie_file, "str": cookie_str,
                      "sapisid": sapisid or None, "mtime": mtime})
        return cookie_str, sapisid if sapisid else None
    except Exception as e:
        log(f"Cookie load error: {e}")
        return _cookie_cache["str"], _cookie_cache["sapisid"]


def make_sapisidhash(sapisid: str) -> str:
    ts = int(time.time())
    h = hashlib.sha1(f"{ts} {sapisid} https://gemini.google.com".encode()).hexdigest()
    return f"SAPISIDHASH {ts}_{h}"


def _account_prefix() -> str:
    """Return the Gemini account path prefix for non-default Google accounts."""
    auth_user = current_config().get("auth_user")
    if auth_user is None or auth_user == "":
        return ""
    return f"/u/{auth_user}"


def _build_headers() -> dict:
    account_prefix = _account_prefix()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://gemini.google.com",
        "Referer": f"https://gemini.google.com{account_prefix}/app",
        "X-Same-Domain": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if account_prefix:
        headers["X-Goog-AuthUser"] = str(current_config()["auth_user"])
    cookie_str, sapisid = load_cookie()
    if cookie_str:
        headers["Cookie"] = cookie_str
    if sapisid:
        headers["Authorization"] = make_sapisidhash(sapisid)
    return headers


def _apply_chat_persistence_flags(inner: list) -> None:
    """Apply Gemini Web persistence flags to an outgoing request payload."""
    if current_config().get("temporary_chats", False):
        # Match Gemini Web temporary-chat requests.
        inner[41] = [1]
        inner[45] = 1
    else:
        inner[41] = [2]


def _build_payload(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None) -> str:
    inner = [None] * 102
    if file_refs:
        refs = [[None, None, ref] for ref in file_refs]
        inner[0] = [prompt, 0, None, refs, None, None, 0]
    else:
        inner[0] = [prompt, 0, None, None, None, None, 0]
    inner[1] = ["en"]
    inner[2] = ["", "", "", None, None, None, None, None, None, ""]
    inner[6] = [0]
    inner[7] = 1
    inner[10] = 1
    inner[11] = 0
    inner[17] = [[think_mode]]
    inner[18] = 0
    inner[27] = 1
    inner[30] = [4]
    _apply_chat_persistence_flags(inner)
    inner[53] = 0
    inner[59] = str(uuid.uuid4())
    inner[61] = []
    inner[68] = 1
    inner[79] = model_id
    if extra_fields:
        for k, v in extra_fields.items():
            inner[k] = v
    outer = [None, json.dumps(inner)]
    params = {"f.req": json.dumps(outer)}
    if current_config().get("xsrf_token"):
        params["at"] = current_config()["xsrf_token"]
    return urllib.parse.urlencode(params)


def _get_url() -> str:
    reqid = int(time.time()) % 1000000
    account_prefix = _account_prefix()
    return (
        f"https://gemini.google.com{account_prefix}/_/BardChatUi/data/"
        "assistant.lamda.BardFrontendService/StreamGenerate"
        f"?bl={shared_bl()}&hl=en&_reqid={reqid}&rt=c"
    )


def _fetch_latest_bl():
    """Fetch the newest Gemini frontend build label without following redirects."""
    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, new):
            return None

    try:
        req = urllib.request.Request(
            "https://gemini.google.com/app?hl=en",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        handlers = [_NoRedirect(), urllib.request.HTTPSHandler(context=_get_ssl_ctx())]
        proxy = current_config().get("proxy")
        if proxy:
            handlers.insert(0, urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        opener = urllib.request.build_opener(*handlers)
        try:
            response = opener.open(req, timeout=15)
        except urllib.error.HTTPError as exc:
            if 300 <= exc.code < 400:
                return None
            raise
        matches = _bl_pattern.findall(response.read().decode("utf-8", errors="replace"))
        if not matches:
            return None
        latest = max(matches, key=lambda item: (item[0], int(item[1]), int(item[2])))
        return f"boq_assistant-bard-web-server_{latest[0]}.{latest[1]}_p{latest[2]}"
    except Exception as exc:
        log(f"BL fetch failed: {exc}")
        return None


def _refresh_bl_until_ready(failed_bl: str = None) -> bool:
    """Fetch a new shared BL and report whether it actually changed."""
    with _bl_update_lock:
        if shared_bl() != failed_bl and failed_bl is not None:
            mark_bl_ready(shared_bl())
            return True
        invalidate_bl()
        latest_bl = _fetch_latest_bl()
        current_bl = shared_bl()
        if latest_bl and latest_bl != current_bl:
            log(f"BL auto-updated: {current_bl} -> {latest_bl}")
            mark_bl_ready(latest_bl)
            return True
        mark_bl_ready(current_bl)
        if latest_bl == current_bl:
            log(f"BL unchanged after 405: {current_bl}")
        return False


def clean_text(text: str, strip: bool = True) -> str:
    text = re.sub(
        r'```(?:python|javascript|text)\?code_(?:reference|stdout)&code_event_index=\d+\n.*?```\n?',
        '', text, flags=re.DOTALL
    )
    text = re.sub(r'http://googleusercontent\.com/card_content/\d+\n?', '', text)
    return text.strip() if strip else text


def _extract_texts_from_line(line: str) -> list:
    """Parse a single wrb.fr line and return list of text strings found."""
    if '"wrb.fr"' not in line or len(line) < 200:
        return []
    try:
        arr = json.loads(line)
        inner_str = arr[0][2]
        if not inner_str or len(inner_str) < 50:
            return []
        inner = json.loads(inner_str)
        if not (isinstance(inner, list) and len(inner) > 4 and inner[4]):
            return []
        texts = []
        for part in inner[4]:
            if isinstance(part, list) and len(part) > 1 and part[1] and isinstance(part[1], list):
                for t in part[1]:
                    if isinstance(t, str) and t:
                        texts.append(t)
        return texts
    except (json.JSONDecodeError, IndexError, TypeError):
        return []


def extract_response_text(raw: str) -> str:
    """Parse full response to get final text."""
    bard_err = re.search(r'BardErrorInfo\s*\[(\d+)\]', raw)
    if bard_err:
        raise RuntimeError(f"Gemini upstream rejected request: BardErrorInfo [{bard_err.group(1)}]")
    last_text = ""
    for line in raw.split("\n"):
        for t in _extract_texts_from_line(line):
            if len(t) > len(last_text):
                last_text = t
    return clean_text(last_text)


def generate(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None) -> str:
    """Non-streaming generation with retry."""
    wait_for_bl()
    body = _build_payload(prompt, model_id, think_mode, file_refs, extra_fields).encode()
    url = _get_url()
    headers = _build_headers()
    ctx = _get_ssl_ctx()
    proxy = current_config().get("proxy")

    last_err = None
    attempt = 0
    retry_attempts = max(1, int(current_config().get("retry_attempts", 1)))
    while attempt < retry_attempts:
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            if proxy:
                opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({"http": proxy, "https": proxy}),
                    urllib.request.HTTPSHandler(context=ctx)
                )
                resp = opener.open(req, timeout=current_config()["request_timeout_sec"])
            else:
                resp = urllib.request.urlopen(
                    req, context=ctx, timeout=current_config()["request_timeout_sec"])
            raw = resp.read().decode("utf-8", errors="replace")
            return extract_response_text(raw)
        except urllib.error.HTTPError as e:
            if e.code == 405:
                failed_bl = url.split("bl=", 1)[1].split("&", 1)[0]
                if _refresh_bl_until_ready(failed_bl):
                    url = _get_url()
                    continue
            last_err = e
            attempt += 1
            if attempt < retry_attempts:
                log(f"Retry {attempt}/{retry_attempts}: {e}")
                time.sleep(current_config()["retry_delay_sec"])
        except Exception as e:
            last_err = e
            attempt += 1
            if attempt < retry_attempts:
                log(f"Retry {attempt}/{retry_attempts}: {e}")
                time.sleep(current_config()["retry_delay_sec"])
    raise last_err or RuntimeError("Gemini generation failed without an exception")


def generate_stream(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None):
    """Streaming generation via httpx with retry on connection failure."""
    if not HAS_HTTPX:
        text = generate(prompt, model_id, think_mode, file_refs, extra_fields)
        if text:
            yield text
        return

    wait_for_bl()
    body = _build_payload(prompt, model_id, think_mode, file_refs, extra_fields)
    url = _get_url()
    headers = _build_headers()
    client = _get_httpx_client()

    last_err = None
    emitted_raw_text = ""
    attempt = 0
    retry_attempts = max(1, int(current_config().get("retry_attempts", 1)))
    while attempt < retry_attempts:
        try:
            with client.stream("POST", url, content=body, headers=headers) as resp:
                resp.raise_for_status()
                buf = ""
                for chunk in resp.iter_text():
                    buf += chunk
                    if "BardErrorInfo" in buf:
                        bard_err = re.search(r'BardErrorInfo\s*\[(\d+)\]', buf)
                        if bard_err:
                            raise RuntimeError(
                                f"Gemini upstream rejected request: BardErrorInfo [{bard_err.group(1)}]"
                            )
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        for t in _extract_texts_from_line(line):
                            if t == emitted_raw_text or emitted_raw_text.startswith(t):
                                continue
                            if not t.startswith(emitted_raw_text):
                                raise RuntimeError("Gemini stream content changed during retry")
                            delta = clean_text(t[len(emitted_raw_text):], strip=False)
                            emitted_raw_text = t
                            if delta:
                                yield delta
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 405:
                failed_bl = url.split("bl=", 1)[1].split("&", 1)[0]
                if _refresh_bl_until_ready(failed_bl):
                    url = _get_url()
                    continue
            last_err = e
            attempt += 1
            if attempt < retry_attempts:
                log(f"Stream retry {attempt}/{retry_attempts}: {e}")
                time.sleep(current_config()["retry_delay_sec"])
        except Exception as e:
            last_err = e
            attempt += 1
            if attempt < retry_attempts:
                log(f"Stream retry {attempt}/{retry_attempts}: {e}")
                time.sleep(current_config()["retry_delay_sec"])
    raise last_err or RuntimeError("Gemini streaming failed without an exception")
