"""Copies config/acronyms.json, search_concepts.json, milestone_stages.json, projects.json and curated_items.json
into the PPP_CONFIG block of index.html, so the dashboard also works when opened as a local file.
Run after editing any of those files:  python scripts/embed_config.py"""
import json, re
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
FILES = {"acronyms": ("acronyms", "acronyms"), "concepts": ("search_concepts", "concepts"),
         "stages": ("milestone_stages", "stages"), "projects": ("projects", "projects"), "curated": ("curated_items", "items")}
idx = ROOT / "index.html"
h = idx.read_text(encoding="utf-8")
m = re.search(r"/\*PPP_CONFIG_START\*/(.*?)/\*PPP_CONFIG_END\*/", h, re.S)
cfg = json.loads(m.group(1))
for k, (f, key) in FILES.items():
    p = ROOT / "config" / f"{f}.json"
    if p.exists():
        cfg[k] = json.loads(p.read_text(encoding="utf-8"))[key]
new = "/*PPP_CONFIG_START*/" + json.dumps(cfg, ensure_ascii=False, separators=(",", ":")) + "/*PPP_CONFIG_END*/"
idx.write_text(h[:m.start()] + new + h[m.end():], encoding="utf-8")
print("index.html updated with config/*.json")
