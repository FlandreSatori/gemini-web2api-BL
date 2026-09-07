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
import urllib.error

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

from .config import (current_config, invalidate_bl, mark_bl_ready,
                     shared_bl, wait_for_bl)

_ssl_ctx = None
_cookie_cache = {
    "path": None,
    "str": "",
    "sapisid": None,
    "xsrf_token": None,
    "auth_user": None,
    "mtime": 0,
}
_httpx_client = None
_bl_update_lock = threading.Lock()
_rate_limit_lock = threading.Lock()
_rate_limit_state = {}
_clash_lock = threading.Lock()
_upstream_failure_state = {}
_bl_pattern = re.compile(r'boq_assistant-bard-web-server_(\d{8})\.(\d+)_p(\d+)')


class RateLimitError(RuntimeError):
    """Raised when the local upstream rate-limit circuit is open."""

    def __init__(self, retry_after: int):
        self.retry_after = retry_after
        super().__init__(f"upstream rate limit circuit open; retry after {retry_after}s")


def log(msg: str):
    if current_config()["log_requests"]:
        import sys
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
        sys.stderr.flush()


def _rate_limit_key() -> str:
    config = current_config()
    return str(config.get("user_id") or config.get("cookie_file") or "default")


def _rate_limit_retry_after() -> int:
    now = time.monotonic()
    with _rate_limit_lock:
        state = _rate_limit_state.get(_rate_limit_key())
        if not state or state["until"] <= now:
            return 0
        return max(1, int(state["until"] - now))


def _record_rate_limit(error=None) -> int:
    config = current_config()
    base = max(1, int(config.get("rate_limit_cooldown_sec", 60)))
    maximum = max(base, int(config.get("rate_limit_max_cooldown_sec", 900)))
    now = time.monotonic()
    retry_after = None
    headers = getattr(error, "headers", None)
    response = getattr(error, "response", None)
    if headers is None and response is not None:
        headers = response.headers
    if headers:
        try:
            retry_after = int(float(headers.get("Retry-After", 0)))
        except (TypeError, ValueError):
            retry_after = None

    with _rate_limit_lock:
        previous = _rate_limit_state.get(_rate_limit_key())
        cooldown = previous["cooldown"] * 2 if previous else base
        cooldown = min(maximum, max(cooldown, retry_after or 0))
        _rate_limit_state[_rate_limit_key()] = {"until": now + cooldown, "cooldown": cooldown}
    return cooldown


def _raise_if_rate_limited() -> None:
    retry_after = _rate_limit_retry_after()
    if retry_after:
        raise RateLimitError(retry_after)


def _failure_key() -> str:
    config = current_config()
    return str(config.get("user_id") or config.get("cookie_file") or "default")


def _failure_state() -> dict:
    with _clash_lock:
        return _upstream_failure_state.setdefault(_failure_key(), {
            "consecutive": 0,
            "switched": False,
            "backoff_until": 0.0,
            "backoff_level": 0,
            "pending_switch": False,
        })


def _clash_headers() -> dict:
    secret = current_config().get("clash_secret")
    return {"Authorization": f"Bearer {secret}"} if secret else {}


def _clash_request(method: str, path: str, data=None):
    controller = current_config().get("clash_controller")
    if not controller:
        return None
    body = json.dumps(data).encode() if data is not None else None
    headers = {"Accept": "application/json", **_clash_headers()}
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        controller.rstrip("/") + path, data=body, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        raw = response.read()
    return json.loads(raw) if raw else None


def _display_name(value: str) -> str:
    """Keep non-ASCII names readable in terminals with the wrong code page."""
    return value.encode("unicode_escape").decode("ascii")


def _repair_mojibake(value: str) -> str:
    """Recover common UTF-8-as-GBK mojibake from Windows config files."""
    try:
        repaired = value.encode("gb18030").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value
    return repaired if repaired != value else value


def _find_clash_proxy_group(proxies: dict, configured_name: str):
    group = proxies.get(configured_name)
    if group:
        return configured_name, group

    repaired_name = _repair_mojibake(configured_name)
    if repaired_name != configured_name and repaired_name in proxies:
        log(f"Clash proxy group name repaired: {_display_name(configured_name)} -> "
            f"{_display_name(repaired_name)}")
        return repaired_name, proxies[repaired_name]

    selector_groups = [
        (name, value) for name, value in proxies.items()
        if isinstance(value, dict)
        and value.get("type") in ("Selector", "URLTest", "Fallback")
        and len(value.get("all") or value.get("proxies") or []) > 1
    ]
    if selector_groups:
        selected_name, selected_group = max(
            selector_groups,
            key=lambda item: len(item[1].get("all") or item[1].get("proxies") or []),
        )
        log(f"Clash proxy group fallback: {_display_name(configured_name)} -> "
            f"{_display_name(selected_name)}")
        return selected_name, selected_group
    return None, None


def _select_low_latency_clash_node() -> bool:
    """Switch Clash to the best available node at or below the latency limit."""
    config = current_config()
    if not config.get("clash_controller"):
        log("Clash switching skipped: clash_controller is not configured")
        return False
    max_latency = max(1, int(config.get("clash_max_latency_ms", 200)))
    test_url = config.get("clash_test_url", "https://gemini.google.com/generate_204")
    group_name = config.get("clash_proxy_group", "GLOBAL")
    log(f"Clash switch started: group={_display_name(group_name)}, max_delay={max_latency}ms")
    try:
        proxy_response = _clash_request("GET", "/proxies") or {}
        proxies = proxy_response.get("proxies", proxy_response)
        if not isinstance(proxies, dict):
            log("Clash switching skipped: invalid proxy list response")
            return False
        group_name, group = _find_clash_proxy_group(proxies, group_name)
        if not group:
            log("Clash switching skipped: no selectable proxy group found")
            return False

        current = group.get("now")
        candidates = group.get("all") or group.get("proxies") or []
        candidates = [name for name in candidates if name not in {current, "DIRECT", "REJECT"}]
        log(f"Clash testing nodes: current={_display_name(current or 'unknown')}, "
            f"candidates={len(candidates)}")
        measured = []
        for name in candidates:
            encoded_name = urllib.parse.quote(name, safe="")
            encoded_url = urllib.parse.quote(test_url, safe="")
            try:
                result = _clash_request(
                    "GET",
                    f"/proxies/{encoded_name}/delay?url={encoded_url}&timeout=5000",
                ) or {}
            except Exception:
                log(f"Clash node test skipped: node={_display_name(name)}")
                continue
            delay = result.get("delay")
            if isinstance(delay, (int, float)) and 0 < delay <= max_latency:
                measured.append((delay, name))
        log(f"Clash node test completed: qualified={len(measured)}/{len(candidates)}")
        if not measured:
            log(f"Clash switching skipped: no node <= {max_latency}ms")
            return False
        measured.sort(key=lambda item: item[0])
        delay, selected = measured[0]
        switch_result = _clash_request(
            "PUT",
            f"/proxies/{urllib.parse.quote(group_name, safe='')}",
            {"name": selected},
        )
        if switch_result is not None and not isinstance(switch_result, (dict, list)):
            log("Clash node switch failed: invalid controller response")
            return False
        log(f"Clash node switched: group={_display_name(group_name)}, "
            f"node={_display_name(selected)}, delay={delay}ms")
        global _httpx_client
        old_client = _httpx_client
        _httpx_client = None
        if old_client is not None:
            old_client.close()
        return True
    except Exception:
        log("Clash node switch failed: controller request unsuccessful")
        return False


def _raise_if_failure_backoff() -> None:
    state = _failure_state()
    now = time.monotonic()
    if state["backoff_until"] > now:
        raise RateLimitError(max(1, int(state["backoff_until"] - now)))
    if state["pending_switch"]:
        log("Clash backoff ended: selecting a new node before resuming requests")
        switched = _select_low_latency_clash_node()
        with _clash_lock:
            state["pending_switch"] = False
            state["switched"] = switched


def _record_upstream_success() -> None:
    state = _failure_state()
    with _clash_lock:
        state["consecutive"] = 0
        state["switched"] = False
        state["backoff_level"] = 0


def _record_upstream_failure(error) -> None:
    state = _failure_state()
    threshold = max(1, int(current_config().get("upstream_failure_threshold", 3)))
    with _clash_lock:
        state["consecutive"] += 1
        consecutive = state["consecutive"]
        switched = state["switched"]
    log(f"Upstream request failed: consecutive={consecutive}/{threshold}")
    if consecutive < threshold:
        return
    if not switched:
        log("Upstream failure threshold reached: requesting Clash node switch")
        switched_now = _select_low_latency_clash_node()
        with _clash_lock:
            state["consecutive"] = 0
            state["switched"] = switched_now
        return
    with _clash_lock:
        state["backoff_level"] += 1
        level = state["backoff_level"]
        base = max(1, int(current_config().get("clash_backoff_base_sec", 60)))
        maximum = max(base, int(current_config().get("clash_backoff_max_sec", 1800)))
        cooldown = min(maximum, base * (2 ** (level - 1)))
        state["backoff_until"] = time.monotonic() + cooldown
        state["consecutive"] = 0
        state["pending_switch"] = True
    log(f"Upstream failures persisted after Clash switch: backoff={cooldown}s, level={level}")


def _failure_backoff_active() -> bool:
    return _failure_state()["backoff_until"] > time.monotonic()


def _is_rate_limited(error) -> bool:
    status = getattr(error, "code", None)
    response = getattr(error, "response", None)
    if response is not None:
        status = getattr(response, "status_code", status)
    return status == 429


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
    cookie_str, sapisid, _, _ = load_cookie_session()
    return cookie_str, sapisid


def load_cookie_session() -> tuple:
    """Load per-account cookie metadata as (cookie, sapisid, xsrf, auth_user)."""
    cookie_file = current_config().get("cookie_file")
    if not cookie_file or not os.path.exists(cookie_file):
        return "", None, current_config().get("xsrf_token"), current_config().get("auth_user")
    try:
        mtime = os.path.getmtime(cookie_file)
        if (cookie_file == _cookie_cache["path"]
            and mtime == _cookie_cache["mtime"] and _cookie_cache["str"]):
            return (
                _cookie_cache["str"],
                _cookie_cache["sapisid"],
                _cookie_cache["xsrf_token"] or current_config().get("xsrf_token"),
                _cookie_cache["auth_user"] if _cookie_cache["auth_user"] is not None
                else current_config().get("auth_user"),
            )
        with open(cookie_file, "r") as f:
            content = f.read().strip()
        xsrf_token = None
        auth_user = None
        if content.startswith("{"):
            data = json.loads(content)
            cookie_str = data.get("cookie", "")
            xsrf_token = data.get("xsrf_token")
            auth_user = data.get("auth_user")
        else:
            cookie_str = content
        pairs = dict(p.split("=", 1) for p in cookie_str.split("; ") if "=" in p)
        sapisid = pairs.get("SAPISID", "")
        _cookie_cache.update({"path": cookie_file, "str": cookie_str,
                      "sapisid": sapisid or None, "xsrf_token": xsrf_token,
                      "auth_user": auth_user, "mtime": mtime})
        return (
            cookie_str,
            sapisid if sapisid else None,
            xsrf_token or current_config().get("xsrf_token"),
            auth_user if auth_user is not None else current_config().get("auth_user"),
        )
    except Exception as e:
        log(f"Cookie load error: {e}")
        return (
            _cookie_cache["str"],
            _cookie_cache["sapisid"],
            _cookie_cache["xsrf_token"] or current_config().get("xsrf_token"),
            _cookie_cache["auth_user"] if _cookie_cache["auth_user"] is not None
            else current_config().get("auth_user"),
        )


def make_sapisidhash(sapisid: str) -> str:
    ts = int(time.time())
    h = hashlib.sha1(f"{ts} {sapisid} https://gemini.google.com".encode()).hexdigest()
    return f"SAPISIDHASH {ts}_{h}"


def _account_prefix() -> str:
    """Return the Gemini account path prefix for non-default Google accounts."""
    _, _, _, auth_user = load_cookie_session()
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
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-CH-UA": '"Google Chrome";v="136", "Chromium";v="136", "Not.A/Brand";v="24"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    }
    if account_prefix:
        _, _, _, auth_user = load_cookie_session()
        headers["X-Goog-AuthUser"] = str(auth_user)
    cookie_str, sapisid, _, _ = load_cookie_session()
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
    _, _, xsrf_token, _ = load_cookie_session()
    if xsrf_token:
        params["at"] = xsrf_token
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
    _raise_if_rate_limited()
    _raise_if_failure_backoff()
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
            text = extract_response_text(raw)
            _record_upstream_success()
            return text
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _record_upstream_failure(e)
                if _failure_backoff_active():
                    raise RateLimitError(
                        max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
            else:
                _record_upstream_failure(e)
                if _failure_backoff_active():
                    raise RateLimitError(
                        max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
            if e.code == 405:
                try:
                    detail = e.read(512).decode("utf-8", errors="replace").replace("\n", " ").strip()
                    if detail:
                        log(f"Gemini 405 response: {detail[:300]}")
                except Exception:
                    pass
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
            _record_upstream_failure(e)
            if _failure_backoff_active():
                raise RateLimitError(
                    max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
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

    _raise_if_rate_limited()
    _raise_if_failure_backoff()
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
            _record_upstream_success()
            return
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                _record_upstream_failure(e)
                if _failure_backoff_active():
                    raise RateLimitError(
                        max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
            else:
                _record_upstream_failure(e)
                if _failure_backoff_active():
                    raise RateLimitError(
                        max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
            if e.response.status_code == 405:
                detail = e.response.text[:512].replace("\n", " ").strip()
                if detail:
                    log(f"Gemini stream 405 response: {detail[:300]}")
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
            _record_upstream_failure(e)
            if _failure_backoff_active():
                raise RateLimitError(
                    max(1, int(_failure_state()["backoff_until"] - time.monotonic()))) from e
            attempt += 1
            if attempt < retry_attempts:
                log(f"Stream retry {attempt}/{retry_attempts}: {e}")
                time.sleep(current_config()["retry_delay_sec"])
    raise last_err or RuntimeError("Gemini streaming failed without an exception")
