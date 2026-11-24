"""Preview the project homepage and settlement pages with cache-safe responses."""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ProjectHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # A live preview, not an immutable deployment: never serve stale assets.
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        super().end_headers()

    def do_GET(self):
        # Ignore validators from older preview responses as well.
        for header in ("If-Modified-Since", "If-None-Match"):
            if header in self.headers:
                del self.headers[header]
        if self.path.split("?", 1)[0] == "/":
            self.path = "/index.html"
        super().do_GET()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), partial(ProjectHandler, directory=str(ROOT)))
    print(f"Project homepage listening on 0.0.0.0:{args.port}", flush=True)
    server.serve_forever()
