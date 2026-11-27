"""Poland Power Pulse - enrich_signals.py (stdlib only).
Runs after feed_fetcher.py. For each live item in data/signals.json that has not been enriched yet, it opens
the ORIGINAL article and writes fields the dashboard reads:
  source_text   - first paragraphs of the article (lead)          → Technology / Asset / Company tagging
  change        - most informative lead sentence  + change_src    → "What changed / Specifics"
  capacity_text - capacity stated in the article (MW, MWh, GW …)  → "Affected entities · Capacity"
  delay_text    - "from <date> to <date>" when the article reports a delay
  enriched_at   - timestamp (items are enriched once; feed_fetcher.py keeps these fields between runs)
Headlines, dates and links are never changed. Fields are only written when the article states them."""
import json, re, html, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data" / "signals.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PolandPowerPulse/10.0 (news monitor)",
      "Accept-Language": "pl,en;q=0.8"}
MAX_PER_RUN, TIMEOUT, PAUSE = 60, 15, 0.7
SKIP_HOSTS = ("news.google.com",)          # Google News redirect pages hold no article text

P_TAG = re.compile(r"<p\b[^>]*>(.*?)</p>", re.I | re.S)
DROP = re.compile(r"<(script|style|nav|header|footer|aside|form|noscript)\b.*?</\1>", re.I | re.S)
NOISE = re.compile(r"cookie|subscribe|newsletter|zaloguj|log in|sign up|all rights reserved|udostępnij|share this|"
                   r"czytaj także|read also|advertis|reklama", re.I)
NUM = r"(\d{1,3}(?:[ ,]\d{3})+(?:\.\d+)?|\d+(?:[.,]\d+)?)"
CAP = re.compile(NUM + r"\s?(GW|MWe|MWp|MWac|MWt|MW)\b(?:\s?/\s?" + NUM + r"\s?(GWh|MWh))?", re.I)
CAP_BAD = re.compile(r"(PLN|zł|EUR|€|\$|per|/kW|capacity (market|auction)|aukcj)\s*$", re.I)
DELAY = re.compile(r"\b(delay|postpon|slip|pushed back|moved to|now expected|opóźni|przesunię)\w*", re.I)
DATEW = re.compile(r"\b((?:Q[1-4]|H[12])\s?20\d\d|(?:January|February|March|April|May|June|July|August|September|"
                   r"October|November|December)\s+20\d\d|20[2-4]\d)\b")
STRONG = re.compile(r"\b(sign|award|select|acquir|sell|sold|buy|start|begin|complet|commission|approv|adopt|"
                    r"launch|secur|reach|deliver|install|win|won|order|financ|decid|podpisa|rozpocz|zakończ|wybra)\w*", re.I)

def clean(t):
    t = html.unescape(re.sub(r"<[^>]+>", " ", t or ""))
    return re.sub(r"\s+", " ", t).strip()

def get(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        raw = r.read(3_000_000)
        return raw.decode(r.headers.get_content_charset() or "utf-8", errors="replace")

def lead(page, n=4):
    page = DROP.sub(" ", page)
    paras = [clean(p) for p in P_TAG.findall(page)]
    paras = [p for p in paras if len(p.split()) >= 12 and not NOISE.search(p)]
    return " ".join(paras[:n])[:2500]

def sentences(t):
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-ZĄĆĘŁŃÓŚŹŻ0-9\"„(])", t) if s.strip()]

def best_change(text, title):
    tw = set(re.findall(r"\w{4,}", (title or "").lower()))
    best, score = "", -1
    for s in sentences(text)[:6]:
        w = len(s.split())
        if not 8 <= w <= 40:
            continue
        sw = set(re.findall(r"\w{4,}", s.lower()))
        overlap = len(sw & tw) / max(1, len(sw | tw))
        sc = (2 if STRONG.search(s) else 0) + (1 if CAP.search(s) or re.search(r"\d", s) else 0) - (3 if overlap > 0.8 else 0)
        if sc > score:
            best, score = s, sc
    return best

def capacity(text):
    out = []
    for m in CAP.finditer(text):
        if CAP_BAD.search(text[max(0, m.start() - 14):m.start()]):
            continue
        v = re.sub(r"\s*/\s*", " / ", m.group(0).strip())
        if v not in out:
            out.append(v)
    return " · ".join(out[:3])

def delay(text):
    for s in sentences(text):
        if DELAY.search(s):
            ds = DATEW.findall(s)
            if len(ds) >= 2:
                return f"from {ds[0]} to {ds[-1]}"
    return ""

def main():
    if not DATA.exists():
        print("No data/signals.json - run feed_fetcher.py first"); return
    doc = json.loads(DATA.read_text(encoding="utf-8"))
    todo = [s for s in doc.get("signals", []) if not s.get("enriched_at")]
    done = 0
    for s in todo[:MAX_PER_RUN]:
        url = (s.get("links") or [{}])[0].get("url", "")
        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if not url or any(h in url for h in SKIP_HOSTS):
            s["enriched_at"] = stamp; continue
        try:
            text = lead(get(url))
        except Exception as e:
            print(f"  SKIP {url[:80]}: {str(e)[:60]}")
            s.setdefault("enrich_tries", 0); s["enrich_tries"] += 1
            if s["enrich_tries"] >= 3:
                s["enriched_at"] = stamp
            continue
        if text:
            s["source_text"] = text
            ch = best_change(text, s.get("title_pl") or s.get("title"))
            if ch and s.get("lang") != "pl":            # Polish lead stays as context; translated title is used
                s["change"], s["change_src"] = ch, url
            cap = capacity(text)
            if cap:
                s["capacity_text"] = cap
                s["capacity_src"] = [{"label": s.get("publisher", "Source"), "url": url, "type": "press"}]
            dl = delay(text)
            if dl:
                s["delay_text"] = dl
                s["delay_src"] = [{"label": s.get("publisher", "Source"), "url": url, "type": "press"}]
        s["enriched_at"] = stamp
        done += 1
        time.sleep(PAUSE)
    tmp = DATA.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8"); tmp.replace(DATA)
    print(f"Enriched {done} items · {max(0, len(todo) - MAX_PER_RUN)} waiting for the next run")

if __name__ == "__main__":
    main()
