"""Poland Power Pulse - live feed fetcher (stdlib only, no pip installs).
Reads English RSS feeds, keeps Poland power-sector items, merges them into data/signals.json.
A failing feed is skipped, so one broken source never fails the workflow.

Scope guardrail: an item is kept only if its HEADLINE is within the monitoring scope
(policy & market design, thermal & nuclear, storage & renewables, company & transaction
signals) and is not about an off-scope topic (history/WWII, opinion, defence/NATO,
party politics, society, sport, data centres, LNG-terminal amenities)."""
import json, re, hashlib, html, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "signals.json"
FEEDS = {
    "CEEnergyNews": "https://ceenergynews.com/feed/",
    "ESS News": "https://www.ess-news.com/feed/",
    "pv magazine": "https://www.pv-magazine.com/feed/",
    "Notes From Poland": "https://notesfrompoland.com/feed/",
    "Baltic Wind": "https://balticwind.eu/feed/",
    "Renewables Now": "https://renewablesnow.com/news/rss/",
    "World Nuclear News": "https://www.world-nuclear-news.org/rss",
}
POLAND = re.compile(r"\bpoland\b|\bpolish\b|\bpge\b|tauron|\benea\b|energa|orlen|ze pak|\bpse\b|\bure\b|baltica|bałtyk|baltyk|lubiatowo|warsaw", re.I)
POWER = re.compile(r"power|energy|electric|grid|wind|solar|\bpv\b|batter|storage|bess|gas|ccgt|ocgt|nuclear|smr|coal|chp|heat|hydrogen|capacity|tariff", re.I)
COS = {"PGE": r"\bpge\b", "ENEA": r"\benea\b", "ENERGA": r"energa", "ORLEN": r"orlen", "TAURON": r"tauron",
       "ZEPAK": r"ze pak", "PEJ": r"\bpej\b|polskie elektrownie j", "POLENERGIA": r"polenergia",
       "AXPO": r"\baxpo\b", "ENGIE": r"\bengie\b", "FORTUM": r"fortum", "EQUINOR": r"equinor",
       "ORSTED": r"ørsted|orsted", "RWE": r"\brwe\b", "TOTAL": r"totalenergies", "EDPR": r"\bedpr?\b"}
UA = {"User-Agent": "Mozilla/5.0 (PolandPowerPulse feed bot)"}

# ── SCOPE GUARDRAIL ─────────────────────────────────────────────────────────
# 1) Headline must mention an in-scope energy term or a monitored company/body.
IN_SCOPE = re.compile(
    r"energ|electric|power (plant|station|market|grid|sector|price|generation|purchase)|\bgrid\b|"
    r"\bwind\b|offshore|onshore|solar|\bpv\b|photovolt|batter|storage|\bbess\b|"
    r"nuclear|\bsmr\b|reactor|\bgas\b|ccgt|ocgt|peaker|gas engine|\bcoal\b|lignite|"
    r"\bheat|\bchp\b|cogenerat|district heating|hydrogen|biomethane|"
    r"capacity market|capacity auction|\bcfd\b|contract for difference|renewabl|\bppa\b|tariff|"
    r"interconnect|transmission|substation|turbine|repower|"
    r"\b(pge|enea|energa|orlen|tauron|pej|pse|ure|ze pak|polenergia|synthos|osge|equinor|"
    r"ørsted|orsted|rwe|engie|axpo|fortum|edpr|westinghouse|bechtel|ge vernova)\b", re.I)
# 2) Headline must NOT be about an off-scope topic (blocked even if an energy word appears).
OFF_SCOPE = re.compile(
    r"\bwwii\b|world war|second world war|nazi|holocaust|reparation|\bvictims\b|"
    r"\[opinion\]|\bopinion\b|\bop-ed\b|\bcomment(ary)?:|"
    r"\bnato\b|military|\barmy\b|apache|missile|fighter jet|\btanks?\b|security forum|ready to fight|"
    r"defen[cs]e (minister|spending|industry)|"
    r"coalition partner|quits .*government|\belection\b|presidential (race|campaign)|"
    r"migra|refugee|football|\bsport|olympic|museum|exhibition|\bfilm\b|church|abortion|"
    r"\bcrime\b|police|murder|court rul|"
    r"data cent(er|re) capacity|lng terminal project brings|cycle path", re.I)

def in_scope(title):
    """True only for headlines inside the dashboard's monitoring scope."""
    t = title or ""
    return bool(IN_SCOPE.search(t)) and not OFF_SCOPE.search(t)
# ────────────────────────────────────────────────────────────────────────────

def clean(t):
    t = html.unescape(re.sub(r"<[^>]+>", " ", t or ""))
    return re.sub(r"\s+", " ", t).strip()

def parse_date(s):
    try:
        return parsedate_to_datetime(s).astimezone(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return s[:10] if re.match(r"\d{4}-\d{2}-\d{2}", s or "") else None

def fetch(name, url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        root = ET.fromstring(r.read())
    out, dropped = [], 0
    for it in root.iter("item"):
        title = clean(it.findtext("title"))
        link = (it.findtext("link") or "").strip()
        desc = clean(it.findtext("description"))
        date = parse_date(it.findtext("pubDate") or "")
        if not (title and link and date) or date < "2024-01-01":
            continue
        hay = f"{title} {desc}"
        if not (POLAND.search(hay) and POWER.search(hay)):
            continue
        if not in_scope(title):                       # ← GUARDRAIL 1: block new off-scope items
            dropped += 1
            print(f"  OUT-OF-SCOPE ({name}): {title[:90]}")
            continue
        cos = [k for k, rx in COS.items() if re.search(rx, hay, re.I)] or ["ALL"]
        words = desc.split()
        out.append({
            "id": "live-" + hashlib.md5(link.encode()).hexdigest()[:10],
            "date": date, "title": title,            # exact source headline, not rephrased
            "summary": " ".join(words[:70]) + ("…" if len(words) > 70 else ""),
            "links": [{"label": f"{name} — article", "url": link, "type": "press"}],
            "cos": cos, "publisher": name, "lang": "en", "region": "National",
        })
    return out, dropped

def main():
    old = {}
    if OUT.exists():
        try: old = {s["id"]: s for s in json.loads(OUT.read_text(encoding="utf-8")).get("signals", [])}
        except Exception: old = {}
    for name, url in FEEDS.items():
        try:
            items, dropped = fetch(name, url)
            for s in items: old[s["id"]] = s
            print(f"OK   {name}: {len(items)} Poland items ({dropped} out of scope)")
        except Exception as e:
            print(f"SKIP {name}: {e}")
    # ← GUARDRAIL 2: purge off-scope items already saved in signals.json by earlier runs
    before = len(old)
    kept = [s for s in old.values() if in_scope(s.get("title", ""))]
    print(f"Scope clean-up: removed {before - len(kept)} previously saved off-scope items")
    sigs = sorted(kept, key=lambda s: s["date"], reverse=True)[:400]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                               "signals": sigs}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Wrote {len(sigs)} live items to {OUT}")

if __name__ == "__main__":
    main()
