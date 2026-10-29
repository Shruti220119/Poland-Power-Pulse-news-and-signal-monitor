"""Poland Power Pulse - live feed fetcher v7 (stdlib only, no pip installs).
Built on your v5 file; changes for dashboard v94 are marked  # v7.

Monitors ALL dashboard sources every run (the GitHub workflow runs it every 30 minutes):
  A. 15 direct RSS feeds (publishers that offer an RSS feed)
  B. Google News "site:" searches for every other publisher in the dashboard (company,
     government, regulator, press, law-firm sites), grouped into a few batched queries
Keeps Poland power-sector items, writes data/signals.json.
A failing feed or query is skipped, so one broken source never fails the workflow.

Guardrails
  1. Scope      - headline must be inside the monitoring scope (IN_SCOPE / OFF_SCOPE).
  2. Duplicates - the same event reported by several sources is stored ONCE (same_event()).
  3. Priority   - kept version: 1 official - 2 free specialist - 3 free general -
                  4 subscription specialist - 5 subscription general, then earliest report.
  4. Coverage   - the log lists every monitored publisher with items found this run.
  5. Enrichment - # v7: fields written by scripts/enrich_signals.py (source_text, change_src,
                  capacity_*, delay_*) are CARRIED OVER between runs, never overwritten.
Optional translation of Polish items: GitHub secrets AZURE_TRANSLATOR_KEY / AZURE_TRANSLATOR_REGION."""
import json, os, re, time, hashlib, html, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone, date
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, quote

OUT = Path(__file__).resolve().parent.parent / "data" / "signals.json"
UA = {"User-Agent": "Mozilla/5.0 (PolandPowerPulse feed bot)"}

# v7: fields owned by enrich_signals.py. The fetcher rebuilds every item from the RSS each run,
# so without this list the enrichment was thrown away and had to be redone on every run.
ENRICH_KEYS = ("source_text", "change_src", "capacity_text", "capacity_src",
               "delay_text", "delay_src", "enriched_at", "history")

# ── A. DIRECT RSS FEEDS ─────────────────────────────────────────────────────
FEEDS = {
    "CEEnergyNews": "https://ceenergynews.com/feed/",
    "ESS News": "https://www.ess-news.com/feed/",
    "pv magazine": "https://www.pv-magazine.com/feed/",
    "Notes From Poland": "https://notesfrompoland.com/feed/",
    "Baltic Wind": "https://balticwind.eu/feed/",
    "Renewables Now": "https://renewablesnow.com/news/rss/",
    "World Nuclear News": "https://www.world-nuclear-news.org/rss",
    "offshoreWIND.biz": "https://www.offshorewind.biz/feed",
    "Energy-Storage.News": "https://www.energy-storage.news/feed/",
    "NucNet": "https://www.nucnet.org/feed.rss",
    "Briefs Finance": "https://www.briefs.co/feed/",
    "WysokieNapiecie.pl": "https://wysokienapiecie.pl/feed/",
    "CIRE": "https://www.cire.pl/rss/energetyka.xml",
    "CIRE Ciepłownictwo": "https://www.cire.pl/rss/cieplownictwo.xml",
    "Energetyka24": "https://energetyka24.com/_rss",
}
POLISH_FEEDS = {"WysokieNapiecie.pl", "CIRE", "CIRE Ciepłownictwo", "Energetyka24"}

# ── SOURCE REGISTRY: every dashboard publisher → website, tier, language ────
# tier: official | specialist | general ; feed: "rss" (section A) or a Google News group (section B)
SOURCES = [
    # name, site (for site: search), tier, lang, group
    # Polish official — utilities, state bodies
    ("PGE", "gkpge.pl", "official", "pl", "pl-official"),
    ("ORLEN", "orlen.pl", "official", "pl", "pl-official"),
    ("Energa", "energa.pl", "official", "pl", "pl-official"),
    ("Enea", "enea.pl", "official", "pl", "pl-official"),
    ("TAURON", "tauron.pl", "official", "pl", "pl-official"),
    ("Polenergia", "polenergia.pl", "official", "pl", "pl-official"),
    ("PEJ", "pej.pl", "official", "pl", "pl-official"),
    ("Baltic Power", "balticpower.pl", "official", "pl", "pl-official"),
    ("ENGIE Polska", "engie-polska.pl", "official", "pl", "pl-official2"),
    ("Energa Storage", "energastorage.pl", "official", "pl", "pl-official2"),
    ("Energa-Operator", "energa-operator.pl", "official", "pl", "pl-official2"),
    ("VSB Energie Odnawialne Polska", "vsb.energy", "official", "pl", "pl-official2"),
    ("URE", "ure.gov.pl", "official", "pl", "pl-gov"),
    ("PSE", "pse.pl", "official", "pl", "pl-gov"),
    ("Ministry of Energy", "gov.pl/web/energia", "official", "pl", "pl-gov"),
    ("Ministry of Climate and Environment", "gov.pl/web/klimat", "official", "pl", "pl-gov"),
    ("Polish Government", "gov.pl/web/aktywa-panstwowe", "official", "pl", "pl-gov"),
    ("Chancellery of the Prime Minister (KPRM)", "gov.pl/web/premier", "official", "pl", "pl-gov"),
    ("Polish Government", "gov.pl", "official", "pl", "fallback"),   # Google News often gives only "gov.pl" as source
    # International official — companies, EU, lenders
    ("RWE", "rwe.com", "official", "en", "int-official"),
    ("Equinor", "equinor.com", "official", "en", "int-official"),
    ("Ørsted", "orsted.com", "official", "en", "int-official"),
    ("TotalEnergies", "totalenergies.com", "official", "en", "int-official"),
    ("EDP Renewables", "edp.com", "official", "en", "int-official"),   # also covers "EDP" items
    ("Fortum", "fortum.com", "official", "en", "int-official"),
    ("Axpo", "axpo.com", "official", "en", "int-official"),
    ("Westinghouse", "westinghousenuclear.com", "official", "en", "int-official"),
    ("GE Vernova", "gevernova.com", "official", "en", "int-official2"),
    ("Northland Power", "northlandpower.com", "official", "en", "int-official2"),
    ("Ocean Winds", "oceanwinds.com", "official", "en", "int-official2"),
    ("European Energy", "europeanenergy.com", "official", "en", "int-official2"),
    ("Actis", "act.is", "official", "en", "int-official2"),
    ("AtkinsRéalis", "atkinsrealis.com", "official", "en", "int-official2"),
    ("Valmet", "valmet.com", "official", "en", "int-official2"),
    ("Van Oord", "vanoord.com", "official", "en", "int-official2"),
    ("KKR", "kkr.com", "official", "en", "int-official3"),
    ("DRI", "dri-energy.com", "official", "en", "int-official3"),
    ("Litgrid", "litgrid.eu", "official", "en", "int-official3"),
    ("EIB", "eib.org", "official", "en", "int-official3"),
    ("EXIM", "exim.gov", "official", "en", "int-official3"),
    ("European Commission", "ec.europa.eu", "official", "en", "int-official3"),
    ("EUR-Lex (Official Journal of the EU)", "eur-lex.europa.eu", "official", "en", "int-official3"),
    ("Bankier.pl (ZE PAK ESPI report)", "bankier.pl", "general", "pl", "pl-press"),
    # Polish press / associations
    ("PAP Biznes", "biznes.pap.pl", "specialist", "pl", "pl-press"),
    ("StrefaInwestorow.pl (PAP Biznes)", "strefainwestorow.pl", "specialist", "pl", "pl-press"),
    ("wnp.pl", "wnp.pl", "specialist", "pl", "pl-press"),
    ("Gramwzielone.pl", "gramwzielone.pl", "specialist", "pl", "pl-press"),
    ("Polskie Radio", "polskieradio.pl", "general", "pl", "pl-press2"),
    ("Radio Gdańsk", "radiogdansk.pl", "general", "pl", "pl-press2"),
    ("Wodne Sprawy", "wodnesprawy.pl", "specialist", "pl", "pl-press2"),
    ("PSME", "psme.org.pl", "specialist", "pl", "pl-press2"),
    ("Forum Energii", "forum-energii.eu", "specialist", "pl", "pl-press2"),
    ("CEO.com.pl", "ceo.com.pl", "general", "pl", "pl-press2"),
    # International press
    ("Reuters", "reuters.com", "general", "en", "int-press"),
    ("MarketScreener", "marketscreener.com", "general", "en", "int-press"),
    ("Euronews", "euronews.com", "general", "en", "int-press"),
    ("Yahoo News", "yahoo.com", "general", "en", "int-press"),
    ("Warsaw Business Journal", "wbj.pl", "general", "en", "int-press"),
    ("Poland Insight", "polandinsight.com", "general", "en", "int-press"),
    ("Poland-24", "poland-24.com", "general", "en", "int-press"),
    ("UA.NEWS", "ua.news", "general", "en", "int-press"),
    ("Clean Energy Wire", "cleanenergywire.org", "specialist", "en", "int-press2"),
    ("Enlit World", "enlit.world", "specialist", "en", "int-press2"),
    ("Riviera", "rivieramm.com", "specialist", "en", "int-press2"),
    ("inspiratia", "inspiratia.com", "specialist", "en", "int-press2"),
    ("Industry Alarm", "industryalarm.eu", "specialist", "en", "int-press2"),
    ("Best Magazine", "bestmag.co.uk", "specialist", "en", "int-press2"),
    ("Energy Drink (BiznesAlert)", "energydrink.today", "specialist", "en", "int-press2"),
    ("Briefs Finance", "briefs.co", "general", "en", "int-press2"),
    # Law firms
    ("Dentons", "dentons.com", "general", "en", "int-law"),
    ("CMS", "cms.law", "general", "en", "int-law"),
    ("SSW", "ssw.solutions", "general", "en", "int-law"),
    # Direct-RSS publishers (section A) — listed so ranking and coverage use one registry
    ("CEEnergyNews", "ceenergynews.com", "specialist", "en", "rss"),
    ("ESS News", "ess-news.com", "specialist", "en", "rss"),
    ("pv magazine", "pv-magazine.com", "specialist", "en", "rss"),
    ("Notes From Poland", "notesfrompoland.com", "general", "en", "rss"),
    ("Baltic Wind", "balticwind.eu", "specialist", "en", "rss"),
    ("Renewables Now", "renewablesnow.com", "specialist", "en", "rss"),
    ("World Nuclear News", "world-nuclear-news.org", "specialist", "en", "rss"),
    ("offshoreWIND.biz", "offshorewind.biz", "specialist", "en", "rss"),
    ("Energy-Storage.News", "energy-storage.news", "specialist", "en", "rss"),
    ("NucNet", "nucnet.org", "specialist", "en", "rss"),
    ("WysokieNapiecie.pl", "wysokienapiecie.pl", "specialist", "pl", "rss"),
    ("CIRE", "cire.pl", "specialist", "pl", "rss"),
    ("Energetyka24", "energetyka24.com", "specialist", "pl", "rss"),
]
PAID = {"Renewables Now", "PAP Biznes", "inspiratia", "Reuters", "MarketScreener", "Riviera", "NucNet"}

# ── B. GOOGLE NEWS GROUPS (one batched query per group, last 7 days) ────────
KW_PL = '(energia OR energetyka OR elektrownia OR "magazyn energii" OR wiatrowa OR fotowoltaika OR jądrowa OR ciepłownia OR "rynek mocy" OR OZE)'
KW_POLAND = "(Poland OR Polish OR Polska)"
KW_EN = "(Poland OR Polish) (energy OR power OR wind OR solar OR battery OR storage OR nuclear OR gas OR coal OR grid OR heat)"
GROUP_KW = {"pl-official": "", "pl-official2": "", "pl-gov": KW_PL, "int-official": KW_POLAND,
            "int-official2": KW_POLAND, "int-official3": KW_POLAND, "pl-press": KW_PL, "pl-press2": KW_PL,
            "int-press": KW_EN, "int-press2": KW_EN, "int-law": KW_EN}

def gnews_url(sites, kw, lang):
    q = "(" + " OR ".join("site:" + s for s in sites) + ")" + (" " + kw if kw else "") + " when:7d"
    loc = "hl=pl&gl=PL&ceid=PL:pl" if lang == "pl" else "hl=en-GB&gl=GB&ceid=GB:en"
    return "https://news.google.com/rss/search?q=" + quote(q) + "&" + loc

def build_groups():
    groups = {}
    for name, site, tier, lang, g in SOURCES:
        if g in ("rss", "fallback"):
            continue
        groups.setdefault(g, {"sites": [], "lang": lang})["sites"].append(site)
    return {g: gnews_url(v["sites"], GROUP_KW.get(g, ""), v["lang"]) for g, v in groups.items()}

def host(url):
    return urlsplit(url or "").netloc.lower().removeprefix("www.")

def lookup(url):
    """Registry entry for a URL (longest matching site wins), else None."""
    h, p = host(url), (urlsplit(url or "").path.lower() or "/")
    best = None
    for name, site, tier, lang, g in SOURCES:
        s_host, _, s_path = site.partition("/")
        if (h == s_host or h.endswith("." + s_host)) and p.startswith("/" + s_path if s_path else "/"):
            if best is None or len(site) > len(best[1]):
                best = (name, site, tier, lang, g)
    return best

# v7: strip tracking parameters / fragments from article links (other parameters are kept,
# because some publishers use ?id= to identify the article, and Google News redirects need theirs)
TRACKING = re.compile(r"^(utm_.*|fbclid|gclid|mc_cid|mc_eid|igshid|ref|ref_src|cmpid|at_.*|wt_.*)$", re.I)
def clean_link(url):
    try:
        p = urlsplit((url or "").strip())
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not TRACKING.match(k)]
        return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q), ""))
    except Exception:
        return (url or "").strip()

# ── 1. SCOPE GUARDRAIL ──────────────────────────────────────────────────────
POLAND = re.compile(r"\bpoland\b|\bpolish\b|\bpolsk|\bpge\b|tauron|\benea\b|energa|orlen|ze pak|\bpse\b|\bure\b|baltica|bałtyk|baltyk|lubiatowo|warsaw|warszaw", re.I)
POWER = re.compile(r"power|energy|electric|grid|wind|solar|\bpv\b|batter|storage|bess|gas|ccgt|ocgt|nuclear|smr|coal|chp|heat|hydrogen|capacity|tariff|"
                   r"energ|prąd|elektrown|elektrociepł|wiatr|fotowolt|magazyn|jądrow|atomow|węgl|ciepł|ciepl|gazow|sieci|aukcj|rynek mocy|rynku mocy|taryf|\boze\b", re.I)
COS = {"PGE": r"\bpge\b", "ENEA": r"\benea\b", "ENERGA": r"energa", "ORLEN": r"orlen", "TAURON": r"tauron",
       "ZEPAK": r"ze pak", "PEJ": r"\bpej\b|polskie elektrownie j", "POLENERGIA": r"polenergia",
       "AXPO": r"\baxpo\b", "ENGIE": r"\bengie\b", "FORTUM": r"fortum", "EQUINOR": r"equinor",
       "ORSTED": r"ørsted|orsted", "RWE": r"\brwe\b", "TOTAL": r"totalenergies", "EDPR": r"\bedpr?\b",
       "IBERDROLA": r"iberdrola"}
INVESTORS = (r"pge|enea|energa|orlen|tauron|pej|pse|ure|ze pak|polenergia|synthos|osge|equinor|"
             r"ørsted|orsted|rwe|engie|axpo|fortum|edpr?|westinghouse|bechtel|ge vernova|"
             r"iberdrola|greenvolt|r\.power|dri|dtek|actis|northland|ocean winds|statkraft|qair|"
             r"enefit|cez|čez|eon|e\.on|vattenfall|totalenergies|baltic power|bałtyk|electrum|photon energy")
IN_SCOPE = re.compile(
    r"energ|electric|power (plant|station|market|grid|sector|price|generation|purchase)|\bgrid\b|"
    r"\bwind\b|offshore|onshore|solar|\bpv\b|photovolt|batter|storage|\bbess\b|"
    r"nuclear|\bsmr\b|reactor|\bgas\b|ccgt|ocgt|peaker|gas engine|\bcoal\b|lignite|"
    r"\bheat|\bchp\b|cogenerat|district heating|hydrogen|biomethane|"
    r"capacity market|capacity auction|\bcfd\b|contract for difference|renewabl|\bppa\b|tariff|"
    r"interconnect|transmission|substation|turbine|repower|"
    r"prąd|elektrown|elektrociepł|wiatr|fotowolt|magazyn|jądrow|atomow|reaktor|węgl|"
    r"ciepł|ciepl|kogenera|gazow|wodór|wodor|biometan|sieci|przesył|dystrybuc|"
    r"rynek mocy|rynku mocy|aukcj|kontrakt(y|ów)? różnicow|taryf|\boze\b|odnawialn|"
    r"\b(" + INVESTORS + r")\b", re.I)
OFF_SCOPE = re.compile(
    r"\bwwii\b|world war|second world war|nazi|holocaust|reparation|\bvictims\b|"
    r"\bhistory of\b|warsaw uprising|powstani[ae] warszawsk|\b19(39|44|45)\b|"                                  # v7: history
    r"\[opinion\]|\bopinion\b|\bop-ed\b|\bcomment(ary)?:|"
    r"\bnato\b|military|\barmy\b|apache|missile|fighter jet|\btanks?\b|security forum|ready to fight|"
    r"\bdefen[cs]e\b|defen[cs]e (minister|spending|industry)|"                                                     # v7: defence
    r"coalition partner|quits .*government|\belection\b|presidential (race|campaign)|"
    r"political party|\bpis\b|konfederacj|\blewica\b|\bpsl\b|polska 2050|"                                       # v7: party politics
    r"migra|refugee|football|\bsport|olympic|museum|exhibition|\bfilm\b|church|abortion|"
    r"\bcrime\b|police|murder|court rul|"
    r"data cent(er|re)s?|centr(um|a|ów|ach) danych|"                                                              # v7: data centres
    r"\blng terminal|\bfsru\b|terminal lng|lng terminal project brings|cycle path|"                              # v7: LNG terminal
    r"sabotage|swedish cable|"
    r"fuel price|petrol|diesel|oil price|crude|"
    r"wojsk|armi[ai]\b|zbrojeni|rakiet|wybor(y|ach|ów)\b|koalicj|felieton|komentarz:|opinia:|"
    r"ceny paliw|stacj[ae] paliw|benzyn|olej napędow|ropa naftowa|ropy naftowej|"
    r"sponsor|konkurs|wolontariat|kariera|rekrutacj|stypendi|piknik|festiwal|maraton", re.I)

def in_scope(title):
    t = title or ""
    return bool(IN_SCOPE.search(t)) and not OFF_SCOPE.search(t)

# ── 2. SOURCE PRIORITY ──────────────────────────────────────────────────────
PAYWALL_PATH = re.compile(r"wysokienapiecie\.pl/ole/", re.I)

def is_paid(item_or_url):
    if isinstance(item_or_url, dict):
        if item_or_url.get("publisher") in PAID:
            return True
        url = item_or_url.get("source_url") or item_or_url["links"][0]["url"]
    else:
        url = item_or_url
        r = lookup(url)
        if r and r[0] in PAID:
            return True
    return bool(PAYWALL_PATH.search(url or ""))

def source_rank(item):
    """1 official · 2 free specialist · 3 free general · 4 paid specialist · 5 paid general"""
    tier = item.get("tier")
    if not tier:
        r = lookup(item.get("source_url") or item["links"][0]["url"])
        tier = r[2] if r else "general"
    if tier == "official":
        return 1
    base = 2 if tier == "specialist" else 3
    return base + 2 if is_paid(item) else base

# ── 3. TRANSLATION (optional, Azure AI Translator) ──────────────────────────
TR_KEY, TR_REGION = os.environ.get("AZURE_TRANSLATOR_KEY", ""), os.environ.get("AZURE_TRANSLATOR_REGION", "")

def translate(texts):
    if not TR_KEY or not texts:
        return None
    try:
        req = urllib.request.Request(
            "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0&from=pl&to=en",
            data=json.dumps([{"Text": t} for t in texts]).encode(),
            headers={"Ocp-Apim-Subscription-Key": TR_KEY, "Ocp-Apim-Subscription-Region": TR_REGION,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return [x["translations"][0]["text"] for x in json.loads(r.read())]
    except Exception as e:
        print(f"  TRANSLATE failed: {e}")
        return None

# ── 4. DUPLICATE DETECTION ──────────────────────────────────────────────────
SYN = [(r"\bstations?\b", "plant"), (r"\bplants?\b", "plant"), (r"\bnpp\b", "nuclear plant"),
       (r"\b(signs|signed|signing|inks|inked)\b", "sign"), (r"\b(deal|pact|accord|agreement)s?\b", "agreement"),
       (r"\b(sells?|sold|selling|offloads?|divests?|weighs sale|considers sale|eyes sale)\b", "sell"),
       (r"\b(buys?|bought|acquires?|acquired|snaps up)\b", "buy"),
       (r"\b(orders|ordered|ordering)\b", "order"), (r"\b(places|placed|placing)\b", "place"),
       (r"\b(announces|announced|unveils)\b", "announce"), (r"\b(plans|planned|planning)\b", "plan"),
       (r"\b(operations|business|assets|portfolio)\b", "assets"),
       (r"\bjoint venture\b|\bshareholders?['’]? agreement\b|\bjv\b", "jointventure"),
       (r"\bpolska grupa energetyczna\b", "pge"), (r"\bpolskie elektrownie j[aą]drowe\b", "pej"),
       (r"\b(polish|poland['’]s|poland)\b", "")]
STOP = set("the and for with from into over under that this these those has have had its their about after before "
           "will would could more than also new on of in to a an by at as is are was were be been it".split())
GENERIC = {"poland", "polish", "energy", "power", "group", "first", "new", "baltic", "europe", "offshore", "wind",
           "solar", "battery", "storage", "nuclear", "project", "projects", "plant", "farm", "auction", "contract",
           "report", "reportedly"}
FIG = re.compile(r"\d+(?:[.,]\d+)?\s?(?:mw|mwh|gw|gwh|twh|bn|billion|million|zł|pln|eur|€|\$)", re.I)

def canon(url):
    p = urlsplit(url or "")
    return (p.netloc.lower().removeprefix("www.") + p.path.rstrip("/")).lower()

def words(t):
    x = " " + (t or "").lower() + " "
    for rx, rep in SYN:
        x = re.sub(rx, rep, x)
    return {w for w in re.sub(r"[^a-z0-9ąćęłńóśźż ]", " ", x).split() if len(w) > 2 and w not in STOP}

def jacc(a, b):
    i = len(a & b)
    return i / max(1, len(a | b)), i

def lead_name(title):
    for w in re.findall(r"[A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż.\-]{2,}", " ".join((title or "").split()[:2])):
        if w.lower() not in GENERIC:
            return w.lower()
    return ""

def days(a, b):
    return abs((date.fromisoformat(a["date"]) - date.fromisoformat(b["date"])).days)

def same_event(a, b):
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
    ra, rb = source_rank(a), source_rank(b)
    return ra < rb if ra != rb else a["date"] <= b["date"]

def merge(keep, drop):
    seen = {canon(l["url"]) for l in keep["links"]}
    for l in drop["links"]:
        if canon(l["url"]) not in seen:
            label = f'{drop["publisher"]} — also reported' + (" · subscription required" if is_paid(drop) else "")
            keep["links"].append({**l, "label": label})
            seen.add(canon(l["url"]))
    keep["links"] = keep["links"][:1] + sorted(keep["links"][1:], key=lambda l: "subscription" in l.get("label", ""))
    keep["date"] = min(keep["date"], drop["date"])
    # v7: if the dropped copy was already enriched but the kept one is not, keep the enrichment
    for k in ENRICH_KEYS:
        if k not in keep and k in drop:
            keep[k] = drop[k]
    return keep

def dedupe(items):
    out = []
    for s in sorted(items, key=lambda s: (s["date"], s["id"])):          # v7: id tie-break → identical order every run
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

# ── 5. TEXT CLEAN-UP ────────────────────────────────────────────────────────
BOILER = re.compile(r"(the post .{3,300}? appeared first on .{2,80}?\.?\s*$)|"
                    r"(\[\s*(…|\.\.\.|&hellip;)\s*\])|(\b(read more|continue reading|czytaj (więcej|dalej))\b.*$)", re.I)

def clean(t):
    t = html.unescape(html.unescape(re.sub(r"<[^>]+>", " ", t or "")))
    return re.sub(r"\s+", " ", t).strip()

def tidy(desc, max_words=60):
    d = BOILER.sub("", clean(desc)).strip(" .…")
    if not d:
        return ""
    sents = re.split(r"(?<=[.!?])\s+(?=[A-ZĄĆĘŁŃÓŚŹŻ0-9\"„])", d + ".")
    out, n = [], 0
    for s in sents:
        w = len(s.split())
        if out and n + w > max_words:
            break
        out.append(s); n += w
    text = " ".join(out).strip()
    if len(text.split()) > max_words:
        text = " ".join(text.split()[:max_words]).rstrip(",;:") + "…"
    return re.sub(r"\.\.$", ".", text)

def change_line(title, summary):
    first = re.split(r"(?<=[.!?])\s+", summary or "")[0] if summary else ""
    return first if first and first.endswith((".", "!", "?")) and 6 <= len(first.split()) <= 35 else title

def parse_date(s):
    try:
        return parsedate_to_datetime(s).astimezone(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return s[:10] if re.match(r"\d{4}-\d{2}-\d{2}", s or "") else None

# ── FETCH ───────────────────────────────────────────────────────────────────
def fetch(name, url, google=False, polish_feed=False):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        root = ET.fromstring(r.read())
    out, dropped = [], 0
    for it in root.iter("item"):
        title = clean(it.findtext("title"))
        link = clean_link(it.findtext("link") or "")                    # v7: tracking parameters removed
        desc = clean(it.findtext("description"))
        date_ = parse_date(it.findtext("pubDate") or "")
        publisher, source_url, tier, lang = name, "", None, "pl" if polish_feed else "en"
        if google:
            src = it.find("source")
            src_name = clean(src.text) if src is not None else ""
            source_url = src.get("url", "") if src is not None else ""
            if src_name:
                title = re.sub(r"\s+-\s+" + re.escape(src_name) + r"\s*$", "", title)
            reg = lookup(source_url)
            if not reg:                                   # result from a site we do not monitor
                continue
            publisher, _, tier, lang, _ = reg
            desc = "" if desc.startswith(title) else desc
        else:
            reg = lookup(link)
            if reg:
                tier = reg[2]
        if not (title and link and date_) or date_ < "2024-01-01":
            continue
        hay = f"{title} {desc}"
        local = lang == "pl"                              # Polish sources are about Poland by definition
        if not ((local or POLAND.search(hay)) and (POWER.search(hay) or IN_SCOPE.search(title))):
            continue
        if not in_scope(title):
            dropped += 1
            print(f"  OUT-OF-SCOPE ({publisher}): {title[:90]}")
            continue
        cos = [k for k, rx in COS.items() if re.search(rx, hay, re.I)] or ["ALL"]
        item = {
            "id": "live-" + hashlib.md5(canon(link).encode()).hexdigest()[:10],
            "date": date_, "title": title,                # exact publisher headline (never rewritten)
            "summary": tidy(desc),
            "links": [{"label": f"{publisher} — article", "url": link,
                       "type": "official" if tier == "official" else "press"}],
            "source_url": source_url, "tier": tier,
            "cos": cos, "publisher": publisher, "lang": lang, "region": "National",
        }
        if is_paid(item):
            item["links"][0]["label"] += " · subscription required"
        out.append(item)
    return out, dropped

def add_translation(item, old):
    if item["lang"] != "pl":
        item["change"] = change_line(item["title"], item["summary"])
        return item
    prev = old.get(item["id"])
    if prev and prev.get("translated"):
        for k in ("title", "title_pl", "summary", "change", "translated"):
            if k in prev:
                item[k] = prev[k]
        return item
    res = translate([item["title"], item["summary"]] if item["summary"] else [item["title"]])
    if res:
        item["title_pl"], item["title"] = item["title"], res[0]       # Polish headline kept in title_pl
        if item["summary"]:
            item["summary_pl"], item["summary"] = item["summary"], res[1]
        item["translated"] = True
    item["change"] = change_line(item["title"], item["summary"])
    return item

def keep_enrichment(item, old):
    """v7: carry over everything enrich_signals.py wrote for this item on an earlier run."""
    prev = old.get(item["id"])
    if not prev:
        return item
    for k in ENRICH_KEYS:
        if k in prev:
            item[k] = prev[k]
    if prev.get("change_src") and prev.get("change"):               # article-based "what changed" wins over the RSS line
        item["change"] = prev["change"]
    return item

def headline_companies(s):
    """v7: companies named in the HEADLINE only (dashboard v81+ uses this to tell policy news from company news)."""
    h = f'{s.get("title", "")} {s.get("title_pl", "")}'
    return [k for k, rx in COS.items() if re.search(rx, h, re.I)]

def main():
    old = {}
    if OUT.exists():
        try:
            old = {s["id"]: s for s in json.loads(OUT.read_text(encoding="utf-8")).get("signals", [])}
        except Exception:
            old = {}
    store, found = dict(old), {}
    print("Translation:", "Azure AI Translator" if TR_KEY else "OFF (no AZURE_TRANSLATOR_KEY) - Polish items hidden on dashboard")
    jobs = [(n, u, False, n in POLISH_FEEDS) for n, u in FEEDS.items()] + \
           [(f"Google News · {g}", u, True, False) for g, u in build_groups().items()]
    for name, url, google, pl in jobs:
        try:
            items, dropped = fetch(name, url, google, pl)
            for s in items:
                s = add_translation(s, old)
                store[s["id"]] = keep_enrichment(s, old)             # v7: enrichment survives the refresh
                found[s["publisher"]] = found.get(s["publisher"], 0) + 1
            print(f"OK   {name}: {len(items)} items ({dropped} out of scope)")
        except Exception as e:
            print(f"SKIP {name}: {e}")
        if google:
            time.sleep(2)                                  # be polite to Google News
    kept = [s for s in store.values() if in_scope(s.get("title_pl") or s.get("title", ""))]
    for s in kept:
        s["summary"] = tidy(s.get("summary", "")) if s.get("summary") else ""
        if not s.get("change_src") and (not s.get("change") or s["change"].endswith("…")):   # v7: never touch an enriched line
            s["change"] = change_line(s["title"], s["summary"])
    print(f"Scope clean-up: removed {len(store) - len(kept)} off-scope items")
    before = len(kept)
    kept = dedupe(kept)
    print(f"Duplicate clean-up: merged {before - len(kept)} duplicate items")
    for s in kept:
        s["cos_headline"] = headline_companies(s)             # v7
    names = sorted({n for n, *_ in SOURCES})
    print(f"Coverage: {len(names)} sources monitored · {len(found)} returned in-scope items this run")
    for n in names:
        print(f"  {'✓' if n in found else '·'} {n}: {found.get(n, 0)}")
    sigs = sorted(kept, key=lambda s: (s["date"], s["id"]), reverse=True)[:600]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                          "sources_monitored": len(names), "signals": sigs}, ensure_ascii=False, indent=1)
    tmp = OUT.with_suffix(".json.tmp")                         # v7: write-then-rename, so a crash never leaves a half-written file
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(OUT)
    enriched = sum(1 for s in sigs if s.get("change_src"))
    print(f"Wrote {len(sigs)} live items to {OUT} ({enriched} already enriched from the source article)")

if __name__ == "__main__":
    main()
