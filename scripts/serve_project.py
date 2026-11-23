"""Preview the project homepage and settlement pages with cache-safe responses."""
import argparse
from functools import partial
from http.server import ThreadingHTTPServer

from serve_comparison_prototype import Handler, ROOT


class ProjectHandler(Handler):
    def do_GET(self):
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
