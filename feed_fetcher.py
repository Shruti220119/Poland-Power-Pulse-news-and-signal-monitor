"""Poland Power Pulse - live feed fetcher (stdlib only, no pip installs).
Reads English RSS feeds, keeps Poland power-sector items, merges them into data/signals.json.
A failing feed is skipped, so one broken source never fails the workflow.

Guardrails
  1. Scope      - headline must be inside the monitoring scope (see IN_SCOPE / OFF_SCOPE).
  2. Duplicates - the same event reported by several sources is stored ONCE (see same_event()).
  3. Priority   - the kept version is chosen by source_rank():
                    1 official / primary source (company, ministry, regulator, TSO)
                    2 free specialist energy press      3 free general press
                    4 subscription specialist press     5 subscription general press
                  then earliest report. Other sources are kept as "also reported" links."""
import json, re, hashlib, html, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone, date
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

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

# ── 1. SCOPE GUARDRAIL ──────────────────────────────────────────────────────
IN_SCOPE = re.compile(
    r"energ|electric|power (plant|station|market|grid|sector|price|generation|purchase)|\bgrid\b|"
    r"\bwind\b|offshore|onshore|solar|\bpv\b|photovolt|batter|storage|\bbess\b|"
    r"nuclear|\bsmr\b|reactor|\bgas\b|ccgt|ocgt|peaker|gas engine|\bcoal\b|lignite|"
    r"\bheat|\bchp\b|cogenerat|district heating|hydrogen|biomethane|"
    r"capacity market|capacity auction|\bcfd\b|contract for difference|renewabl|\bppa\b|tariff|"
    r"interconnect|transmission|substation|turbine|repower|"
    r"\b(pge|enea|energa|orlen|tauron|pej|pse|ure|ze pak|polenergia|synthos|osge|equinor|"
    r"ørsted|orsted|rwe|engie|axpo|fortum|edpr|westinghouse|bechtel|ge vernova)\b", re.I)
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
    t = title or ""
    return bool(IN_SCOPE.search(t)) and not OFF_SCOPE.search(t)

# ── 2. SOURCE PRIORITY (edit these lists as needed) ─────────────────────────
OFFICIAL = re.compile(r"(^|\.)(gov\.pl|ure\.gov\.pl|pse\.pl|gkpge\.pl|enea\.pl|energa\.pl|orlen\.pl|tauron\.pl|"
                      r"ppej\.pl|zepak\.com\.pl|polenergia\.pl|equinor\.com|orsted\.com|rwe\.com|westinghousenuclear\.com|"
                      r"europa\.eu|espi|gpw\.pl)$", re.I)
SPECIALIST = re.compile(r"(^|\.)(world-nuclear-news\.org|nucnet\.org|renewablesnow\.com|ceenergynews\.com|ess-news\.com|"
                        r"pv-magazine\.com|balticwind\.eu|energy-storage\.news|wysokienapiecie\.pl|gramwzielone\.pl|cire\.pl|"
                        r"wnp\.pl|energetyka24\.com|biznes\.pap\.pl|reuters\.com|enerdata\.net|inspiratia\.com|rivieramm\.com)$", re.I)
PAYWALL = re.compile(r"(^|\.)(renewablesnow\.com|biznes\.pap\.pl|inspiratia\.com|reuters\.com|marketscreener\.com|"
                     r"rivieramm\.com|ft\.com|bloomberg\.com|wsj\.com|montelnews\.com|argusmedia\.com)$", re.I)

def host(url):
    return urlsplit(url or "").netloc.lower().removeprefix("www.")

def is_paid(url):
    return bool(PAYWALL.search(host(url)))

def source_rank(item):
    """1 official · 2 free specialist · 3 free general · 4 paid specialist · 5 paid general"""
    url = item["links"][0]["url"]
    h = host(url)
    if OFFICIAL.search(h):
        return 1
    base = 2 if SPECIALIST.search(h) else 3
    return base + 2 if is_paid(url) else base

# ── 3. DUPLICATE DETECTION ──────────────────────────────────────────────────
SYN = [(r"\bstations?\b", "plant"), (r"\bplants?\b", "plant"), (r"\bnpp\b", "nuclear plant"),
       (r"\b(signs|signed|signing|inks|inked)\b", "sign"), (r"\b(deal|pact|accord|agreement)s?\b", "agreement"),
       (r"\b(sells?|sold|selling|offloads?|divests?)\b", "sell"), (r"\b(buys?|bought|acquires?|acquired|snaps up)\b", "buy"),
       (r"\b(orders|ordered|ordering)\b", "order"), (r"\b(places|placed|placing)\b", "place"),
       (r"\b(announces|announced|unveils)\b", "announce"), (r"\b(plans|planned|planning)\b", "plan"),
       (r"\bjoint venture\b|\bshareholders?['’]? agreement\b|\bjv\b", "jointventure"),
       (r"\bpolska grupa energetyczna\b", "pge"), (r"\bpolskie elektrownie j[aą]drowe\b", "pej"),
       (r"\b(polish|poland['’]s|poland)\b", "")]
STOP = set("the and for with from into over under that this these those has have had its their about after before "
           "will would could more than also new on of in to a an by at as is are was were be been it".split())
GENERIC = {"poland", "polish", "energy", "power", "group", "first", "new", "baltic", "europe", "offshore", "wind",
           "solar", "battery", "storage", "nuclear", "project", "projects", "plant", "farm", "auction", "contract"}
FIG = re.compile(r"\d+(?:[.,]\d+)?\s?(?:mw|mwh|gw|gwh|twh|bn|billion|million|zł|pln|eur|€|\$)", re.I)

def canon(url):
    p = urlsplit(url or "")
    return (p.netloc.lower().removeprefix("www.") + p.path.rstrip("/")).lower()   # drops ?utm=… and #…

def words(t):
    x = " " + (t or "").lower() + " "
    for rx, rep in SYN:
        x = re.sub(rx, rep, x)
    return {w for w in re.sub(r"[^a-z0-9ąćęłńóśźż ]", " ", x).split() if len(w) > 2 and w not in STOP}

def jacc(a, b):
    i = len(a & b)
    return i / max(1, len(a | b)), i

def lead_name(title):
    """First capitalised, non-generic word of the headline, e.g. 'Greenvolt' in 'Greenvolt sells off…'"""
    for w in re.findall(r"[A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż.\-]{2,}", " ".join((title or "").split()[:2])):
        if w.lower() not in GENERIC:
            return w.lower()
    return ""

def days(a, b):
    return abs((date.fromisoformat(a["date"]) - date.fromisoformat(b["date"])).days)

def same_event(a, b):
    """Return the rule name if a and b report the same event, else ''."""
    d = days(a, b)
    if d > 14:
        return ""
    if canon(a["links"][0]["url"]) == canon(b["links"][0]["url"]):
        return "R1 same article URL"
    wa, wb = words(a["title"]), words(b["title"])
    j, n = jacc(wa, wb)
    if j >= 0.6:
        return "R2 near-identical headline"
    if d > 3:
        return ""
    if j >= 0.5 and n >= 3:
        return "R6 same event wording"
    sa, sb = words(a["title"] + " " + a["summary"]), words(b["title"] + " " + b["summary"])
    j2, n2 = jacc(sa, sb)
    if j2 >= 0.30 and n2 >= 4:
        return "R6 same event content"
    la, lb = lead_name(a["title"]), lead_name(b["title"])
    if la and la == lb and j >= 0.12:
        return "R3 same company leads both headlines"
    figs = {f.replace(" ", "").lower() for f in FIG.findall(a["title"] + " " + a["summary"])} & \
           {f.replace(" ", "").lower() for f in FIG.findall(b["title"] + " " + b["summary"])}
    co = (set(a["cos"]) & set(b["cos"])) - {"ALL"}
    if co and figs and j2 >= 0.12:
        return "R5 same company + same figure"
    return ""

def better(a, b):
    """True if a should be kept over b."""
    ra, rb = source_rank(a), source_rank(b)
    return ra < rb if ra != rb else a["date"] <= b["date"]

def merge(keep, drop):
    seen = {canon(l["url"]) for l in keep["links"]}
    for l in drop["links"]:
        if canon(l["url"]) not in seen:
            label = f'{drop["publisher"]} — also reported' + (" · subscription required" if is_paid(l["url"]) else "")
            keep["links"].append({**l, "label": label})
            seen.add(canon(l["url"]))
    keep["links"] = keep["links"][:1] + sorted(keep["links"][1:], key=lambda l: is_paid(l["url"]))  # free links first
    keep["date"] = min(keep["date"], drop["date"])
    return keep

def dedupe(items):
    out = []
    for s in sorted(items, key=lambda s: s["date"]):
        for i, o in enumerate(out):
            rule = same_event(o, s)
            if rule:
                keep, drop = (s, o) if better(s, o) else (o, s)
                out[i] = merge(keep, drop)
                print(f"  DUPLICATE [{rule}] kept {keep['publisher']} · dropped {drop['publisher']}: {drop['title'][:70]}")
                break
        else:
            out.append(s)
    return out

# ── FETCH ───────────────────────────────────────────────────────────────────
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
        date_ = parse_date(it.findtext("pubDate") or "")
        if not (title and link and date_) or date_ < "2024-01-01":
            continue
        hay = f"{title} {desc}"
        if not (POLAND.search(hay) and POWER.search(hay)):
            continue
        if not in_scope(title):
            dropped += 1
            print(f"  OUT-OF-SCOPE ({name}): {title[:90]}")
            continue
        cos = [k for k, rx in COS.items() if re.search(rx, hay, re.I)] or ["ALL"]
        w = desc.split()
        out.append({
            "id": "live-" + hashlib.md5(canon(link).encode()).hexdigest()[:10],   # tracking codes no longer create new ids
            "date": date_, "title": title,                                         # exact source headline
            "summary": " ".join(w[:70]) + ("…" if len(w) > 70 else ""),
            "links": [{"label": f"{name} — article" + (" · subscription required" if is_paid(link) else ""),
                       "url": link, "type": "press"}],
            "cos": cos, "publisher": name, "lang": "en", "region": "National",
        })
    return out, dropped

def main():
    old = {}
    if OUT.exists():
        try:
            old = {s["id"]: s for s in json.loads(OUT.read_text(encoding="utf-8")).get("signals", [])}
        except Exception:
            old = {}
    for name, url in FEEDS.items():
        try:
            items, dropped = fetch(name, url)
            for s in items:
                old[s["id"]] = s
            print(f"OK   {name}: {len(items)} Poland items ({dropped} out of scope)")
        except Exception as e:
            print(f"SKIP {name}: {e}")
    kept = [s for s in old.values() if in_scope(s.get("title", ""))]
    print(f"Scope clean-up: removed {len(old) - len(kept)} off-scope items")
    before = len(kept)
    kept = dedupe(kept)                                   # ← duplicate guardrail (new + previously saved items)
    print(f"Duplicate clean-up: merged {before - len(kept)} duplicate items")
    sigs = sorted(kept, key=lambda s: s["date"], reverse=True)[:400]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                               "signals": sigs}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Wrote {len(sigs)} live items to {OUT}")

if __name__ == "__main__":
    main()
