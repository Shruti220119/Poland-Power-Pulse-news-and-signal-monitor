"""Poland Power Pulse - live feed fetcher v6 (stdlib only, no pip installs).
Monitors ALL dashboard sources every run (the GitHub workflow runs it every 30 minutes):
  A. 16 direct RSS feeds (publishers that offer an RSS feed)
  B. Google News "site:" searches for every other publisher in the dashboard (company,
     government, regulator, press, law-firm sites), grouped into a few batched queries
Keeps Poland power-sector items, writes data/signals.json.
A failing feed or query is skipped, so one broken source never fails the workflow.

Guardrails
  1. Scope      - headline must be inside the monitoring scope (IN_SCOPE / OFF_SCOPE).
  2. Duplicates - the same event reported by several sources is stored ONCE (same_event()).
  3. Priority   - kept version: 1 official · 2 free specialist · 3 free general ·
                  4 subscription specialist · 5 subscription general, then earliest report.
  4. Coverage   - the log lists every monitored publisher with items found this run.
  v6 additions: ~35 extra sources (policy, permitting, grid, associations, trade press, company newsrooms);
  policy / regulation terms in the scope filter; `title_clear` (a descriptive headline for vague titles,
  exact publisher headline kept in `title`); `cos_headline` (companies named in the headline only, so
  national policy items are not tagged to a company); `generated` timestamp for the dashboard clock.
Optional translation of Polish items: GitHub secrets AZURE_TRANSLATOR_KEY / AZURE_TRANSLATOR_REGION."""
import json, os, re, time, hashlib, html, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone, date
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, quote

OUT = Path(__file__).resolve().parent.parent / "data" / "signals.json"
UA = {"User-Agent": "Mozilla/5.0 (PolandPowerPulse feed bot)"}

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
    # -- v6: policy, permitting, grid and market bodies (Poland) --
    ("Government Legislation Centre (RCL)", "legislacja.gov.pl", "official", "pl", "pl-gov2"),
    ("NIK - Supreme Audit Office", "nik.gov.pl", "official", "pl", "pl-gov2"),
    ("NFOŚiGW", "nfosigw.gov.pl", "official", "pl", "pl-gov2"),
    ("Maritime Office Gdynia (offshore permits)", "umgdy.gov.pl", "official", "pl", "pl-gov2"),
    ("Zarządca Rozliczeń", "zrsa.pl", "official", "pl", "pl-gov2"),
    ("ARE - Energy Market Agency", "are.waw.pl", "official", "pl", "pl-gov2"),
    ("GAZ-SYSTEM", "gaz-system.pl", "official", "pl", "pl-gov2"),
    ("Renewables contact point (gov.pl)", "gov.pl/web/kontaktoze", "official", "pl", "pl-gov2"),
    # -- v6: company newsrooms, contractors and exchange filings --
    ("TAURON Dystrybucja", "tauron-dystrybucja.pl", "official", "pl", "pl-official3"),
    ("ZE PAK", "zepak.com.pl", "official", "pl", "pl-official3"),
    ("Polimex Mostostal", "polimex-mostostal.pl", "official", "pl", "pl-official3"),
    ("Budimex", "budimex.pl", "official", "pl", "pl-official3"),
    ("LG Energy Solution Wrocław", "lgensol.pl", "official", "pl", "pl-official3"),
    ("GPW - exchange filings", "gpw.pl", "official", "pl", "pl-official3"),
    ("Iberdrola", "iberdrola.com", "official", "en", "int-official4"),
    ("OSGE (Orlen Synthos Green Energy)", "osge.com", "official", "en", "int-official4"),
    ("EDP Renewables investors", "edpr-investors.com", "official", "en", "int-official4"),
    ("European Commission (Europa)", "commission.europa.eu", "official", "en", "int-official4"),
    # -- v6: associations, think tanks and Polish trade press --
    ("PSEW", "psew.pl", "specialist", "pl", "pl-press4"),
    ("PIGEOR", "pigeor.pl", "specialist", "pl", "pl-press4"),
    ("Polish PV Association (PSF)", "stowarzyszeniepv.pl", "specialist", "pl", "pl-press4"),
    ("Instrat", "instrat.pl", "specialist", "pl", "pl-press4"),
    ("Instytut Reform", "ireform.eu", "specialist", "pl", "pl-press4"),
    ("WiseEuropa", "wise-europa.eu", "specialist", "pl", "pl-press4"),
    ("IGCP - district heating", "igcp.org.pl", "specialist", "pl", "pl-press4"),
    ("Globenergia.pl", "globenergia.pl", "specialist", "pl", "pl-press3"),
    ("Teraz Środowisko", "teraz-srodowisko.pl", "specialist", "pl", "pl-press3"),
    ("BiznesAlert.pl", "biznesalert.pl", "specialist", "pl", "pl-press3"),
    ("ISBnews", "isbnews.pl", "specialist", "pl", "pl-press3"),
    ("Stockwatch.pl", "stockwatch.pl", "general", "pl", "pl-press3"),
    ("Biznes24", "biznes24.pl", "general", "pl", "pl-press3"),
    # -- v6: international trade press --
    ("Nuclear Engineering International", "neimagazine.com", "specialist", "en", "int-press3"),
    ("reNEWS", "renews.biz", "specialist", "en", "int-press3"),
    ("PV Tech", "pv-tech.org", "specialist", "en", "int-press3"),
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
PAID = {"Renewables Now", "PAP Biznes", "inspiratia", "Reuters", "MarketScreener", "Riviera", "NucNet",
        "Nuclear Engineering International", "reNEWS"}   # v6: partly paywalled

# ── B. GOOGLE NEWS GROUPS (one batched query per group, last 7 days) ────────
KW_PL = '(energia OR energetyka OR elektrownia OR "magazyn energii" OR wiatrowa OR fotowoltaika OR jądrowa OR ciepłownia OR "rynek mocy" OR OZE)'
KW_POLAND = "(Poland OR Polish OR Polska)"
KW_EN = "(Poland OR Polish) (energy OR power OR wind OR solar OR battery OR storage OR nuclear OR gas OR coal OR grid OR heat)"
GROUP_KW = {"pl-official": "", "pl-official2": "", "pl-official3": "", "pl-gov": KW_PL, "pl-gov2": KW_PL,
            "int-official4": KW_POLAND, "pl-press3": KW_PL, "pl-press4": KW_PL, "int-press3": KW_EN, "int-official": KW_POLAND,
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
POLICY_TERMS = (r"energy law|prawo energetyczne|\bres act\b|ustaw\w* o odnawialnych|nowelizacj\w* ustawy|projekt ustawy|"
                r"\bnecp\b|\bkpeik\b|\bpep ?2040\b|\bets ?2?\b|state aid|pomoc publiczn|capacity mechanism|market design|"
                r"market reform|reforma rynku|price freeze|zamrożeni\w* cen|derogation|support scheme|system wsparcia|"
                r"deregulac|konsultacj\w* (społeczn|publiczn)|public consultation|draft (law|bill|act|amendment)")
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
    + POLICY_TERMS + r"|\b(" + INVESTORS + r")\b", re.I)
OFF_SCOPE = re.compile(
    r"\bwwii\b|world war|second world war|nazi|holocaust|reparation|\bvictims\b|"
    r"\[opinion\]|\bopinion\b|\bop-ed\b|\bcomment(ary)?:|"
    r"\bnato\b|military|\barmy\b|apache|missile|fighter jet|\btanks?\b|security forum|ready to fight|"
    r"defen[cs]e (minister|spending|industry)|"
    r"coalition partner|quits .*government|\belection\b|presidential (race|campaign)|"
    r"migra|refugee|football|\bsport|olympic|museum|exhibition|\bfilm\b|church|abortion|"
    r"\bcrime\b|police|murder|court rul|"
    r"data cent(er|re) capacity|lng terminal project brings|cycle path|"
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
    if VAGUE.search(a["title"].strip()) and VAGUE.search(b["title"].strip()):
        return ""      # v6: numbered notices / periodic reports ("Informacja nr 10/2026" vs "nr 11/2026") are different items
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

VAGUE = re.compile(r"^(informacja nr|komunikat\b|karta aktualizacji|raport (bieżący|okresowy)|periodic reports?|"
                   r"consolidated report|current report|(q[1-4]|h[12])\b.{0,40}(results|report)|dispatch from|"
                   r"podsumowanie tygodnia|weekly summary|energy transition in poland|rrf impact|"
                   r"komunikat prasowy|press release$|ogłoszenie\b)", re.I)
def is_vague(*titles):
    return any(t and (VAGUE.search(t.strip()) or len(t.split()) <= 3) for t in titles)
def clear_title(title, title_pl, summary, max_words=20):
    """Descriptive headline built from the article summary when the publisher headline is vague.
    The exact publisher headline always stays in `title`. Returns "" if no usable summary."""
    if not is_vague(title, title_pl) or not summary:
        return ""
    first = re.split(r"(?<=[.!?])\s+", summary.strip())[0].rstrip(".")
    if len(first.split()) > max_words:                     # cut at a natural break before the limit
        for sep in (", according", ", while", ", as ", "; ", " - ", " – ", ", "):
            head = first.split(sep)[0]
            if 6 <= len(head.split()) <= max_words:
                first = head
                break
        else:
            first = " ".join(first.split()[:max_words]).rstrip(",;:") + "…"
    if len(first.split()) < 5:
        return ""
    return first if first.lower() != (title or "").lower() else ""
def scope_text(title, desc=""):
    """Vague headlines (e.g. 'Informacja nr 10/2026') are scope-checked on headline + summary."""
    return f"{title} {desc}" if is_vague(title) and desc else title
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
        link = (it.findtext("link") or "").strip()
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
        if not in_scope(scope_text(title, desc)):
            dropped += 1
            print(f"  OUT-OF-SCOPE ({publisher}): {title[:90]}")
            continue
        cos = [k for k, rx in COS.items() if re.search(rx, hay, re.I)] or ["ALL"]
        item = {
            "id": "live-" + hashlib.md5(canon(link).encode()).hexdigest()[:10],
            "date": date_, "title": title,
            "summary": tidy(desc),
            "links": [{"label": f"{publisher} — article", "url": link,
                       "type": "official" if tier == "official" else "press"}],
            "source_url": source_url, "tier": tier,
            "cos": cos, "publisher": publisher, "lang": lang, "region": "National",
            "cos_headline": [k for k, rx in COS.items() if re.search(rx, title, re.I)] or ["ALL"],
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
        item["title_pl"], item["title"] = item["title"], res[0]
        if item["summary"]:
            item["summary_pl"], item["summary"] = item["summary"], res[1]
        item["translated"] = True
    item["change"] = change_line(item["title"], item["summary"])
    return item

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
                store[s["id"]] = add_translation(s, old)
                found[s["publisher"]] = found.get(s["publisher"], 0) + 1
            print(f"OK   {name}: {len(items)} items ({dropped} out of scope)")
        except Exception as e:
            print(f"SKIP {name}: {e}")
        if google:
            time.sleep(2)                                  # be polite to Google News
    kept = [s for s in store.values() if in_scope(scope_text(s.get("title_pl") or s.get("title", ""), s.get("summary", "")))]
    for s in kept:
        s["summary"] = tidy(s.get("summary", "")) if s.get("summary") else ""
        if not s.get("change") or s["change"].endswith("…"):
            s["change"] = change_line(s["title"], s["summary"])
    for s in kept:                                         # v6: descriptive headline for vague titles
        s["title_clear"] = clear_title(s.get("title", ""), s.get("title_pl", ""), s.get("summary", ""))
        if not s["title_clear"]:
            s.pop("title_clear", None)
        if "cos_headline" not in s:                       # items stored by an older version
            s["cos_headline"] = [k for k, rx in COS.items() if re.search(rx, s.get("title_pl") or s.get("title", ""), re.I)] or ["ALL"]
    print(f"Scope clean-up: removed {len(store) - len(kept)} off-scope items")
    before = len(kept)
    kept = dedupe(kept)
    print(f"Duplicate clean-up: merged {before - len(kept)} duplicate items")
    names = sorted({n for n, *_ in SOURCES})
    print(f"Coverage: {len(names)} sources monitored · {len(found)} returned in-scope items this run")
    for n in names:
        print(f"  {'✓' if n in found else '·'} {n}: {found.get(n, 0)}")
    sigs = sorted(kept, key=lambda s: s["date"], reverse=True)[:600]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    OUT.write_text(json.dumps({"updated": now_iso, "generated": now_iso,
                               "sources_monitored": len(names), "signals": sigs},
                              ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Wrote {len(sigs)} live items to {OUT}")

if __name__ == "__main__":
    main()
