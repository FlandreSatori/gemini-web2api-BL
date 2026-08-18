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
    servers = []
    for index, cookie_file in enumerate(cookie_files):
        user_config = dict(CONFIG)
        user_config["cookie_file"] = cookie_file
        user_config["port"] = CONFIG["port"] + index
        server = ThreadedServer((CONFIG["host"], user_config["port"]), GeminiHandler, user_config)
        servers.append(server)

    print(f"gemini-web2api v{__version__}")
    for index, server in enumerate(servers):
        config = server.user_config
        print(f"  User {index + 1}: http://localhost:{config['port']}/v1 "
              f"(cookie: {config.get('cookie_file') or 'none'})")
    print(f"  Users:     {len(servers)}")
    print(f"  Models:    {', '.join(MODELS.keys())}")
    print(f"  Proxy:     {CONFIG.get('proxy') or 'system env'}")
    print(f"  Streaming: {'httpx (true streaming)' if HAS_HTTPX else 'urllib (buffered)'}")
    print(f"  Temporary: {'yes' if CONFIG.get('temporary_chats', False) else 'no'}")
    print()
    try:
        threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nStopped.")
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()
