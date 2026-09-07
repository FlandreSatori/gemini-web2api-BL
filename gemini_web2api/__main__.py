"""Entry point: python -m gemini_web2api"""
import argparse
import glob
import os
import re
import threading

from .config import CONFIG, load_config, find_config, set_shared_bl
from .models import MODELS
from .gemini import HAS_HTTPX
from .server import GeminiHandler, ThreadedServer
from . import __version__


def main():
    parser = argparse.ArgumentParser(description="Gemini Web to OpenAI API")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--cookie-file", type=str, default=None)
    parser.add_argument("--proxy", type=str, default=None, help="HTTP proxy, e.g. http://127.0.0.1:7890")
    parser.add_argument("--version", action="version", version=f"gemini-web2api {__version__}")
    args = parser.parse_args()

    config_path = args.config or os.environ.get("GEMINI_WEB2API_CONFIG") or find_config()
    if config_path:
        load_config(config_path)

    if args.port:
        CONFIG["port"] = args.port
    if args.cookie_file:
        CONFIG["cookie_file"] = args.cookie_file
    if args.proxy:
        CONFIG["proxy"] = args.proxy

    if args.cookie_file:
        cookie_files = [args.cookie_file]
    else:
        cookie_files = glob.glob("cookie*.txt")
        cookie_files.sort(key=lambda path: int(re.search(r"cookie(\d*)\.txt$", path).group(1) or 0))
    if not cookie_files:
        cookie_files = [CONFIG.get("cookie_file")]

    set_shared_bl(CONFIG["gemini_bl"])
    user_configs = []
    for index, cookie_file in enumerate(cookie_files):
        user_config = dict(CONFIG)
        user_config["cookie_file"] = cookie_file
        user_config["port"] = CONFIG["port"]
        user_config["user_id"] = f"user{index + 1}"
        user_configs.append(user_config)

    server = ThreadedServer((CONFIG["host"], CONFIG["port"]), GeminiHandler, user_configs)

    print(f"gemini-web2api v{__version__}")
    print(f"  Endpoint:  http://localhost:{CONFIG['port']}/v1")
    print(f"  Users:     {len(user_configs)} (internal round-robin)")
    for index, config in enumerate(user_configs):
        print(f"  User {index + 1}: cookie={config.get('cookie_file') or 'none'}")
    print(f"  Models:    {', '.join(MODELS.keys())}")
    print(f"  Proxy:     {CONFIG.get('proxy') or 'system env'}")
    print(f"  Streaming: {'httpx (true streaming)' if HAS_HTTPX else 'urllib (buffered)'}")
    print(f"  Temporary: {'yes' if CONFIG.get('temporary_chats', False) else 'no'}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
