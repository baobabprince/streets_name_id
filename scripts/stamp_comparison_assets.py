"""Add content hashes to prototype asset URLs so HTML and JS cannot mix versions.

Run after editing prototype JS/CSS, including when serving on a static host.
No extra build artifacts or Node dependencies are required.
"""
import hashlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "prototype"
ASSETS = (
    "comparison.js", "score-tools.js", "comparison.css",
    "vendor/leaflet/leaflet.js", "vendor/leaflet/leaflet.css",
)


def stamp():
    page = ROOT / "index.html"
    text = page.read_text(encoding="utf-8")
    for asset in ASSETS:
        digest = hashlib.sha256((ROOT / asset).read_bytes()).hexdigest()[:16]
        pattern = r'((?:src|href)=")' + re.escape(asset) + r'(?:\?v=[^" ]+)?"'
        text, count = re.subn(pattern, lambda match: match[1] + asset + "?v=" + digest + '"', text)
        if count != 1:
            raise ValueError(f"Expected exactly one reference to {asset}; found {count}")
    page.write_text(text, encoding="utf-8")
    return text


if __name__ == "__main__":
    stamp()
    print("Updated all prototype script/style URLs with content hashes")
