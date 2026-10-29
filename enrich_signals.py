"""Poland Power Pulse - article reader  (scripts/enrich_signals.py, v94-compatible rebuild)

Runs AFTER feed_fetcher.py and BEFORE the commit step. For every live item in data/signals.json it
opens the ORIGINAL article (not the RSS teaser) and writes the fields dashboard v94 reads:

    source_text    first paragraphs of the article (the "lead"; Technology / Asset / Company / Event are read from it)
    summary        the lead facts (2-3 sentences) - the RSS teaser is kept in summary_rss
    change         "What changed" = the most informative sentence of the lead      (change_src = true)
    capacity_text  capacity AS THE SOURCE STATES IT, e.g. "400 MW / 800 MWh";
                   several projects -> "A 200 MW / 800 MWh + B 100 MW / 400 MWh = 300 MW / 1,200 MWh"
    capacity_src   [{label,url,type}] when the figure comes from ANOTHER source for the same news
    delay_text     "Delayed from <old date> to <new date>" - only when the article gives the dates
    delay_src      [] (own article)

Rules: nothing is inferred. No figure / no date in the text -> the field is left out (the dashboard then
shows nothing, or its own headline fallback). Polish articles are translated with the existing Azure secrets
(AZURE_TRANSLATOR_KEY / AZURE_TRANSLATOR_REGION); without a key a Polish item only gets capacity.
Results are cached in data/source_cache.json, so each article is read once, not every 30 minutes.
Standard library only. A slow site can never overrun the workflow (time budget, default 7 minutes).

Environment:  ENRICH_BUDGET_S (default 420) - total time budget
              ENRICH_WEB_LOOKUP (default 1) - look for a missing capacity in other reports of the same news; 0 = off
"""
import json, os, re, sys, time, html, base64, urllib.request, urllib.error
from datetime import datetime, timezone, date
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, quote
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent.parent
SIGNALS = ROOT / "data" / "signals.json"
CACHE = ROOT / "data" / "source_cache.json"
BUDGET = float(os.environ.get("ENRICH_BUDGET_S", "420"))
WEB_LOOKUP = os.environ.get("ENRICH_WEB_LOOKUP", "1") != "0"
TR_KEY, TR_REGION = os.environ.get("AZURE_TRANSLATOR_KEY", ""), os.environ.get("AZURE_TRANSLATOR_REGION", "")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-GB,en;q=0.9,pl;q=0.8", "Accept-Encoding": "identity"}
MAX_TRIES, MAX_BYTES, LEAD_PARAS = 3, 1_500_000, 5
T0 = time.time()

# ══ 1. HTTP ═════════════════════════════════════════════════════════════════
def http_get(url, timeout=15):
    """Returns (final_url, text). Raises on network / HTTP errors."""
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read(MAX_BYTES)
        ctype = r.headers.get("Content-Type", "")
        final = r.geturl()
    m = re.search(r"charset=([\w\-]+)", ctype, re.I) or re.search(rb"<meta[^>]+charset=[\"']?([\w\-]+)", raw[:4000], re.I)
    enc = (m.group(1).decode() if isinstance(m.group(1), bytes) else m.group(1)) if m else "utf-8"
    try:
        return final, raw.decode(enc, "replace")
    except LookupError:
        return final, raw.decode("utf-8", "replace")

def resolve_google(url):
    """Best effort: Google News links -> publisher URL. Returns '' when it cannot be resolved."""
    if "news.google.com" not in url:
        return url
    try:                                           # old-style ids carry the URL in base64
        tail = urlsplit(url).path.rstrip("/").split("/")[-1]
        raw = base64.urlsafe_b64decode(tail + "=" * (-len(tail) % 4))
        m = re.search(rb"https?://[^\x00-\x20\x7f-\xff\"<>]+", raw)
        if m:
            return m.group(0).decode()
    except Exception:
        pass
    try:                                           # interstitial page sometimes exposes the target
        final, page = http_get(url)
        if "news.google.com" not in urlsplit(final).netloc:
            return final
        m = re.search(r'data-n-au="(https?://[^"]+)"', page)
        if m:
            return html.unescape(m.group(1))
    except Exception:
        pass
    return ""

# ══ 2. ARTICLE TEXT ═════════════════════════════════════════════════════════
SKIP_TAGS = {"script", "style", "nav", "footer", "aside", "form", "noscript", "header", "figure", "figcaption",
             "svg", "button", "iframe", "select", "template"}
VOID = {"br", "img", "input", "meta", "link", "hr", "source", "wbr", "area", "base", "col", "embed", "param", "track"}
SKIP_CLASS = re.compile(r"(^|[\s_-])(nav|menu|footer|sidebar|share|sharing|social|newsletter|subscribe|subscription|cookie|"
                        r"comment|comments|related|promo|advert|ad|ads|breadcrumb|widget|toolbar|tags|author|byline|"
                        r"paywall|banner|popup|modal)([\s_-]|$)", re.I)

class Extract(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.buf, self.in_p = [], [], False
        self.art_depth, self.art, self.all, self.meta = 0, [], [], {}
        self.title = ""
        self._in_title = False

    def skipping(self):
        return any(s for _, s, _ in self.stack)

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "meta":
            k = (a.get("property") or a.get("name") or "").lower()
            if k in ("og:description", "description", "og:title", "article:published_time") and a.get("content"):
                self.meta.setdefault(k, a["content"])
            return
        if tag == "title":
            self._in_title = True
        if tag in VOID:
            return
        cls = (a.get("class") or "") + " " + (a.get("id") or "")
        skip = tag in SKIP_TAGS or bool(SKIP_CLASS.search(cls)) or a.get("aria-hidden") == "true"
        is_art = tag == "article"
        self.stack.append((tag, skip, is_art))
        if is_art:
            self.art_depth += 1
        if tag == "p" and not self.skipping():
            self.in_p, self.buf = True, []

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
        if tag in VOID:
            return
        if tag == "p" and self.in_p:
            t = re.sub(r"\s+", " ", "".join(self.buf)).strip()
            self.in_p = False
            if t:
                self.all.append(t)
                if self.art_depth:
                    self.art.append(t)
        for i in range(len(self.stack) - 1, -1, -1):          # tolerate malformed HTML
            if self.stack[i][0] == tag:
                for _, _, was_art in self.stack[i:]:
                    if was_art:
                        self.art_depth -= 1
                del self.stack[i:]
                break

    def handle_data(self, data):
        if self._in_title:
            self.title += data
        if self.in_p and not self.skipping():
            self.buf.append(data)

BOILER = re.compile(
    r"cookie|subscribe|subscription|newsletter|sign up|sign in|log in|login|register to|all rights reserved|follow us|"
    r"share this|read more|read also|related (articles?|stories)|advertisement|copyright|©|"
    r"the post .{3,300}? appeared first on|^(photo|image|source|tags?|credit|fot\.?|zdjęcie|foto)\s*[:\-–]|"
    r"^(also read|see also|czytaj (też|także|więcej)|zobacz (też|także))|click here|privacy policy|terms of use|"
    r"this article is (for|only available)|already a subscriber|zaloguj|dostęp dla|premium", re.I)
PAYWALL = re.compile(r"subscribe to (read|continue)|subscribers only|log in to read|for subscribers|zaloguj się|"
                     r"dostęp dla (prenumeratorów|subskrybentów)|treść dostępna po|wykup dostęp", re.I)
DATELINE = re.compile(r"^[A-ZĄĆĘŁŃÓŚŹŻ][A-ZĄĆĘŁŃÓŚŹŻ .,'-]{2,30}(\([^)]{2,30}\))?\s*[-–—:]\s+")

def is_quote(p):
    return bool(re.match(r"^[“\"„‘']", p)) and bool(re.search(r"[”\"”’']\s*$", p))

def article_paragraphs(page):
    ex = Extract()
    try:
        ex.feed(page)
    except Exception:
        pass
    paras = ex.art if sum(len(p) > 60 for p in ex.art) >= 2 else ex.all
    out, seen = [], set()
    for p in paras:
        p = html.unescape(p)
        p = DATELINE.sub("", p).strip()
        if len(p) < 45 or len(p.split()) < 7 or BOILER.search(p) or is_quote(p) or p.lower() in seen:
            continue
        seen.add(p.lower())
        out.append(p)
    paywalled = bool(PAYWALL.search(page)) and sum(len(p) for p in out) < 600
    desc = html.unescape(ex.meta.get("og:description") or ex.meta.get("description") or "").strip()
    if len(out) < 2 and desc and len(desc.split()) >= 8 and not BOILER.search(desc):
        out.insert(0, desc)
    return out, paywalled

# ══ 3. SENTENCES, WORDS ═════════════════════════════════════════════════════
ABBR = re.compile(r"(\b(approx|Mr|Mrs|Ms|Dr|St|No|vs|etc|Inc|Ltd|Co|Corp|Sp|ul|np|tzw|ok|mln|mld|tys|godz|nr|e\.g|i\.e|U\.S|z o\.o|S\.A)\.)$", re.I)

def split_sentences(text):
    parts = re.split(r"(?<=[.!?])\s+(?=[A-ZĄĆĘŁŃÓŚŹŻ0-9\"“„‘(])", text.strip())
    out = []
    for p in parts:
        if out and ABBR.search(out[-1]):
            out[-1] += " " + p
        else:
            out.append(p)
    return [s.strip() for s in out if s.strip()]

STOP = set("the and for with from into over under that this these those has have had its their about after before will would "
           "could more than also new on of in to a an by at as is are was were be been it said says".split())
GENERIC = {"poland", "polish", "energy", "power", "group", "project", "projects", "plant", "farm", "company"}

def words(t):
    return {w for w in re.sub(r"[^\w ]", " ", (t or "").lower()).split() if len(w) > 2 and w not in STOP}

def jac(a, b):
    a, b = words(a), words(b)
    return len(a & b) / max(1, len(a | b))

# ══ 4. WHAT CHANGED ═════════════════════════════════════════════════════════
EVENT = re.compile(r"\b(sign(s|ed)|begin|began|begun|start(s|ed)|commenc\w+|award(s|ed)|select(s|ed)|chose|chosen|complet\w+|"
                   r"acquir\w+|buy|bought|sells?|sold|launch\w*|approv\w+|announc\w+|arriv\w+|install\w+|secur\w+|clos(es|ed)|"
                   r"receiv\w+|grant(s|ed)|delay\w*|postpon\w+|reach\w+|appoint\w+|agree\w*|partner\w*|invest\w+|"
                   r"inaugurat\w+|energi[sz]ed|connect\w+|order\w*|cancel\w+|raise[sd]?|unveil\w+|"
                   r"podpis\w+|rozpocz\w+|wybra\w+|zakończ\w+|przej\w+|sprzed\w+|ogłos\w+|uruchom\w+|zatwierdz\w+|opóźni\w+)\b", re.I)
WEAK = re.compile(r"^(the (project|facility|plant|site|company|deal|sale|move|contract|agreement|investment|unit|farm|park)|"
                  r"it|this|that|these|they|its)\b", re.I)
UNITRX = re.compile(r"\d\s?(GW|MW|MWh|GWh|PLN|EUR|€|m|bn)\b", re.I)

def endDot(s):
    s = s.strip()
    return s if re.search(r"[.!?]$", s) else s + "."

def pick_change(title, paras):
    cands = []
    for pi, p in enumerate(paras[:LEAD_PARAS]):
        for si, s in enumerate(split_sentences(p)):
            n = len(s.split())
            if not 7 <= n <= 45 or jac(s, title) >= 0.8:
                continue
            score = 0.0
            score += 3 if EVENT.search(s) else 0
            score += 1 if UNITRX.search(s) else 0
            score += 1 if re.search(r"\b[A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż]{2,}\b", s[1:]) else 0
            score -= 2 if WEAK.match(s) else 0
            score -= 2 if re.match(r"^(according to|in an interview|speaking|commenting)\b", s, re.I) else 0
            score -= 0.6 * (pi * 2 + si)
            cands.append((score, s))
    if not cands:
        return ""
    return endDot(max(cands, key=lambda x: x[0])[1])

def lead_summary(change, paras, max_words=75):
    out, n = [], 0
    for p in paras[:3]:
        for s in split_sentences(p):
            if jac(s, change) >= 0.8:
                continue
            w = len(s.split())
            if out and n + w > max_words:
                return " ".join(out)
            out.append(endDot(s)); n += w
            if len(out) >= 3:
                return " ".join(out)
    return " ".join(out)

# ══ 5. CAPACITY ═════════════════════════════════════════════════════════════
NUM = r"(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{1,3}(?:[ \u00a0]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)"
UNIT = r"(?:GWh|MWh|GWp|MWp|GWe|MWe|MWt|MWac|GW|MW)"
CAP = re.compile(r"(?P<mul>\d{1,2}\s?[x×]\s?)?(?P<pre>~|up to |about |approx\.? |around |over |more than |at least )?"
                 rf"(?P<n1>{NUM})[\s\u00a0-]?(?P<u1>{UNIT})\b(?:\s?/\s?(?P<n2>{NUM})[\s\u00a0-]?(?P<u2>GWh|MWh)\b)?", re.I)
BAD_BEFORE = re.compile(r"(pln|eur|€|\$|zł|usd|gbp)\s?$|(/|per |za )\s?$", re.I)
SKIP_SENT = re.compile(r"capacity (market|auction)|rynek mocy|rynku mocy|aukcj|total installed|installed (wind|solar|renewable|power)? ?capacity "
                       r"(in|of) (poland|the country)|moc zainstalowana|(poland|polish) (now )?(has|had)\b|as of \d{1,2} (jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)|"
                       r"portfolio of|pipeline of|currently operates|already (has|operates)|operates (more than|over)|"
                       r"\b(in total|a total of) .{0,40}(portfolio|operat)", re.I)
TOTAL_CUE = re.compile(r"\b(total|combined|together|in total|a total of|łącznie|razem|sumarycznie|aggregate)\b", re.I)
MONTHS = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
          "styczeń", "luty", "marzec", "kwiecień", "maj", "czerwiec", "lipiec", "sierpień", "wrzesień", "październik", "listopad", "grudzień"}
NAME_STOP = {"The", "This", "That", "These", "In", "On", "At", "Poland", "Polish", "PGE", "ORLEN", "Orlen", "TAURON", "Tauron", "Enea",
             "Energa", "Equinor", "Ørsted", "Orsted", "RWE", "Baltic", "Group", "Project", "Company", "Capacity", "Battery", "BESS", "Wind",
             "Solar", "Energy", "Power", "Offshore", "Onshore", "Storage", "Nuclear", "Gas", "Plant", "Farm", "Park", "Total", "Combined",
             "Both", "Each", "Two", "Three", "Four", "With", "And", "For", "Of", "To", "By", "From", "Its", "Their", "Located", "Near",
             "Fluence", "Siemens", "Vestas", "Hitachi", "Ocean", "Winds", "Greenvolt", "Iberdrola", "EDPR", "PPC", "ZE", "PAK", "Elektrotim"}
NAME_RX = re.compile(r"[A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,}(?: [A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,})?")

def to_float(s, pl=False):
    s = s.replace("\u00a0", " ")
    if re.fullmatch(r"\d{1,3}(,\d{3})+(\.\d+)?", s):
        return float(s.replace(",", ""))
    if re.fullmatch(r"\d{1,3}( \d{3})+([.,]\d+)?", s):
        return float(s.replace(" ", "").replace(",", "."))
    return float(s.replace(",", ".") if re.fullmatch(r"\d+,\d{1,2}", s) else s)

def fnum(x):
    return f"{int(round(x)):,}" if abs(x - round(x)) < 1e-9 else f"{x:,.2f}".rstrip("0").rstrip(".")

def unit_norm(u):
    return {"mw": "MW", "gw": "GW", "mwh": "MWh", "gwh": "GWh", "mwp": "MWp", "gwp": "GWp", "mwe": "MWe", "gwe": "GWe",
            "mwt": "MWt", "mwac": "MWac"}[u.lower()]

def kind(u):
    return "energy" if u.endswith("h") else "power"

def hit_text(m):
    pre = (m.group("pre") or "").strip().lower()
    pre = {"about": "~", "approx": "~", "approx.": "~", "around": "~", "more than": "over"}.get(pre, pre)
    n1, u1 = m.group("n1"), unit_norm(m.group("u1"))
    n1 = fnum(to_float(n1))
    s = (pre + (" " if pre and pre != "~" else "") if pre else "")
    if m.group("mul"):
        k = int(re.match(r"\d+", m.group("mul")).group())
        s += f"{k} × {n1} {u1} = {fnum(to_float(m.group('n1')) * k)} {u1}"
    else:
        s += f"{n1} {u1}"
    if m.group("n2"):
        s += f" / {fnum(to_float(m.group('n2')))} {unit_norm(m.group('u2'))}"
    return s

def hit_values(m):
    """-> {unit: value} in the units written, multiplied if '2 × 668 MWe'."""
    mul = int(re.match(r"\d+", m.group("mul")).group()) if m.group("mul") else 1
    d = {unit_norm(m.group("u1")): to_float(m.group("n1")) * mul}
    if m.group("n2"):
        d[unit_norm(m.group("u2"))] = to_float(m.group("n2"))
    return d

def asset_name(sent, m):
    before, after = sent[max(0, m.start() - 45):m.start()], sent[m.end():m.end() + 40]
    cand = []
    mb = re.search(r"([A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,}(?: [A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,})?)\s*[(:–—-]\s*$", before)
    if mb:
        cand.append(mb.group(1))
    ma = re.match(r"^\)?\s*(?:[a-ząćęłńóśźż]+ ){0,2}?([A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,}(?: [A-ZĄĆĘŁŃÓŚŹŻ][\wąćęłńóśźż\-]{2,})?)", after)
    if ma:
        cand.append(ma.group(1))
    for c in cand:
        first = c.split()[0]
        if first not in NAME_STOP and first.lower() not in MONTHS:
            return c
    return ""

def find_hits(sentences):
    hits = []
    for si, s in enumerate(sentences):
        if SKIP_SENT.search(s):
            continue
        for m in CAP.finditer(s):
            if BAD_BEFORE.search(s[max(0, m.start() - 6):m.start()]) or re.match(r"\s?(per|/)\s?\w", s[m.end():m.end() + 6]):
                continue
            hits.append({"si": si, "txt": hit_text(m), "vals": hit_values(m), "name": asset_name(s, m),
                         "total": bool(TOTAL_CUE.search(s[max(0, m.start() - 40):m.end() + 10]))})
    return hits

def sum_hits(hs):
    units = [tuple(sorted(h["vals"])) for h in hs]
    if len(set(units)) != 1:
        return ""
    tot = {u: sum(h["vals"][u] for h in hs) for u in hs[0]["vals"]}
    return " / ".join(f"{fnum(v)} {u}" for u, v in sorted(tot.items(), key=lambda x: kind(x[0]) == "energy"))

def capacity_from(title, paras):
    """-> (text, status) ; status in ok | none | review"""
    sentences = [title] + [s for p in paras[:LEAD_PARAS] for s in split_sentences(p)]
    hits = find_hits(sentences)
    if not hits:
        return "", "none"
    uniq = []
    for h in hits:                                              # same figure repeated -> one
        if not any(h["txt"] == u["txt"] and (h["name"] == u["name"] or not h["name"] or not u["name"]) for u in uniq):
            uniq.append(h)
    for h in list(uniq):                                        # "800 MWh" is contained in "400 MW / 800 MWh"
        if len(h["vals"]) == 1 and any(len(o["vals"]) == 2 and o is not h and
                                       all(abs(o["vals"].get(u, -1) - v) < 1e-9 for u, v in h["vals"].items()) for o in uniq):
            uniq.remove(h)
    if len(uniq) == 1:
        return uniq[0]["txt"], "ok"
    totals = [h for h in uniq if h["total"]]
    named = [h for h in uniq if h["name"] and not h["total"]]
    if totals and len({h["name"] for h in named}) >= 2:
        parts = " + ".join(f'{h["name"]} {h["txt"]}' for h in named)
        return f'{parts} = {totals[0]["txt"]}', "ok"
    if totals:
        return totals[0]["txt"], "ok"
    names = [h["name"] for h in uniq]
    if len(uniq) == 2 and not any(names) and uniq[0]["si"] == uniq[1]["si"] \
            and {kind(u) for h in uniq for u in h["vals"]} == {"power", "energy"} and all(len(h["vals"]) == 1 for h in uniq):
        p = next(h for h in uniq if kind(next(iter(h["vals"]))) == "power")
        e = next(h for h in uniq if kind(next(iter(h["vals"]))) == "energy")
        return f'{p["txt"]} / {e["txt"]}', "ok"
    if all(names) and len(set(names)) == len(names) and len(uniq) >= 2:
        parts = " + ".join(f'{h["name"]} {h["txt"]}' for h in uniq)
        t = sum_hits(uniq)
        return (f"{parts} = {t}" if t else parts), "ok"
    return "", "review"                                         # several figures, no way to tell which asset -> leave to the analyst

# ══ 6. DELAY ════════════════════════════════════════════════════════════════
MON = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
DATE = (rf"(?:Q[1-4]\s?(?:of\s)?20\d\d|(?:H[12]|first half|second half)\s(?:of\s)?20\d\d|\d{{1,2}}\s{MON}\.?\s20\d\d|{MON}\.?\s(?:\d{{1,2}},\s)?20\d\d|"
        rf"(?:end|beginning|start|middle|mid)(?:-|\sof\s)(?:the\s)?(?:year\s)?20\d\d|(?:the\s)?(?:end|beginning|start)\sof\s20\d\d|20\d\d)")
DELAY_CUE = re.compile(r"\bdelay\w*|\bpostpon\w+|pushed back|\bslipp?\w*|behind schedule|later than (planned|expected|scheduled)|"
                       r"\breschedul\w+|\bpostponement|opóźni\w+|przesun\w+|przełoż\w+|\bnow (expected|scheduled|planned|due)", re.I)
def G(n):
    return "(?P<" + n + ">" + DATE + ")"

def clean_date(d):
    return re.sub(r"\s+", " ", re.sub(r"^the\s+", "", d.strip(), flags=re.I)).strip(" .,")

def delay_from(title, paras):
    text_sents = [title] + [s for p in paras[:8] for s in split_sentences(p)]
    for i, s in enumerate(text_sents):
        if not DELAY_CUE.search(s):
            continue
        ctx = " ".join(text_sents[max(0, i - 1):i + 2])
        pats = [
            rf"from\s+{G('old')}\s+(?:to|until|till|back to)\s+{G('new')}",
            rf"(?:originally|initially|previously)\s+(?:planned|scheduled|expected|due|slated|set)(?:\s+(?:for|by|in|to))?\s+{G('old')}.{{0,100}}?(?:now|but|will|is expected|to be)\D{{0,60}}?{G('new')}",
            rf"{G('new')}\s*,?\s+instead of\s+(?:the\s+)?(?:original(?:ly)?\s+)?(?:planned\s+)?{G('old')}",
            rf"(?:postpon\w+|delay\w*|pushed back|slipp\w+|moved|rescheduled|przesun\w+|przełoż\w+)[^.]{{0,60}}?(?:to|until|till|do)\s+{G('new')}[^.]{{0,80}}?(?:from|instead of|originally|z)\s+{G('old')}",
            rf"(?:scheduled|planned|expected|due)\s+(?:for|by|in)\s+{G('old')}[^.]{{0,100}}?(?:postpon\w+|delay\w*|pushed back|slipp\w+|moved|rescheduled)[^.]{{0,40}}?(?:to|until|till)\s+{G('new')}",
        ]
        for p in pats:
            m = re.search(p, ctx, re.I)
            if m and clean_date(m.group("old")) != clean_date(m.group("new")):
                return f'Delayed from {clean_date(m.group("old"))} to {clean_date(m.group("new"))}'
        m = re.search(rf"(?:postpon\w+|delay\w*|pushed back|slipp\w+|moved|rescheduled|now (?:expected|scheduled|planned|due))[^.]{{0,60}}?(?:to|until|till|for|in|by)\s+{G('new')}", s, re.I)
        if m:
            return f'Now expected {clean_date(m.group("new"))} (original date not stated in the article)'
    return ""

# ══ 7. TRANSLATION ══════════════════════════════════════════════════════════
def translate(texts):
    if not TR_KEY or not texts:
        return None
    try:
        req = urllib.request.Request(
            "https://api.cognitive.microsofttranslator.com/translate?api-version=3.0&from=pl&to=en",
            data=json.dumps([{"Text": t} for t in texts]).encode(),
            headers={"Ocp-Apim-Subscription-Key": TR_KEY, "Ocp-Apim-Subscription-Region": TR_REGION, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return [x["translations"][0]["text"] for x in json.loads(r.read())]
    except Exception as e:
        print(f"  TRANSLATE failed: {e}")
        return None

# ══ 8. OTHER REPORTS OF THE SAME NEWS (for a missing capacity) ═════════════
def host(u):
    return urlsplit(u or "").netloc.lower().removeprefix("www.")

def same_event(a_title, a_date, b_title, b_date, thr):
    try:
        if abs((date.fromisoformat(a_date) - date.fromisoformat(b_date)).days) > 10:
            return False
    except Exception:
        return False
    A, B = words(a_title) - GENERIC, words(b_title) - GENERIC
    return len(A & B) >= 2 and len(A & B) / max(1, len(A | B)) >= thr

def web_capacity(item):
    """Google News RSS search for another report of the same news; capacity must be in that report's headline/teaser."""
    q = " ".join(sorted(words(item["title"]) - GENERIC)[:7]) + " MW"
    url = "https://news.google.com/rss/search?q=" + quote(q) + "&hl=en-GB&gl=GB&ceid=GB:en"
    try:
        final, xml = http_get(url, timeout=15)
        root = ET.fromstring(xml)
    except Exception as e:
        print(f"  web lookup failed: {e}")
        return "", []
    for it in root.iter("item"):
        t = html.unescape(it.findtext("title") or "")
        src = it.find("source")
        pub = html.unescape(src.text) if src is not None and src.text else ""
        t = re.sub(r"\s+-\s+" + re.escape(pub) + r"\s*$", "", t) if pub else t
        if pub and pub.lower() == (item.get("publisher") or "").lower():
            continue
        if not same_event(item["title"], item["date"], t, item["date"], 0.5):
            continue
        cap, st = capacity_from(t, [])
        if st == "ok" and cap:
            return cap, [{"label": pub or "Other report", "url": it.findtext("link") or "", "type": "press"}]
    return "", []

# ══ 9. MAIN ═════════════════════════════════════════════════════════════════
def load_json(p, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

def write_json(p, obj):
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(p)

def read_article(item):
    """-> dict of fields, or {'_fail': reason}"""
    url = (item.get("links") or [{}])[0].get("url", "")
    if not url:
        return {"_fail": "no link"}
    real = resolve_google(url)
    if not real:
        return {"_fail": "Google News redirect not resolvable"}
    try:
        final, page = http_get(real)
    except Exception as e:
        return {"_fail": f"fetch error: {str(e)[:60]}"}
    paras, paywalled = article_paragraphs(page)
    if paywalled or not paras:
        return {"_fail": "paywalled or no readable text"}
    title = item.get("title_pl") or item.get("title", "")
    f = {"paras": paras[:8], "url_read": final}
    pl = item.get("lang") == "pl"
    cap, cst = capacity_from(title, paras)
    f["cap"], f["cap_status"] = cap, cst
    if pl:                                                    # Polish article
        tr = translate(paras[:LEAD_PARAS])
        if tr:
            f["paras_en"] = tr
            f["delay"] = delay_from(item.get("title", ""), tr)
        else:
            f["delay"] = ""
    else:
        f["delay"] = delay_from(title, paras)
    return f

def apply(item, f):
    paras = f.get("paras_en") or (f["paras"] if item.get("lang") != "pl" else None)
    if item.get("lang") == "pl" and not paras:                # untranslated Polish: keep the original, no English text fields
        item["source_text_pl"] = " ".join(f["paras"][:LEAD_PARAS])[:1100]
    else:
        title = item.get("title", "")
        change = pick_change(title, paras)
        if "summary_rss" not in item:
            item["summary_rss"] = item.get("summary", "")
        item["source_text"] = " ".join(paras[:LEAD_PARAS])[:1100]
        if change:
            item["change"], item["change_src"] = change, True
            item["summary"] = lead_summary(change, paras) or item.get("summary", "")
    for k, v in (("capacity_text", f.get("cap")), ("delay_text", f.get("delay"))):
        if v:
            item[k] = v
        else:
            item.pop(k, None)
    item.setdefault("capacity_src", []) if f.get("cap") else item.pop("capacity_src", None)
    item.setdefault("delay_src", []) if f.get("delay") else item.pop("delay_src", None)
    item["enriched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

def main():
    data = load_json(SIGNALS, None)
    if not data or not data.get("signals"):
        print("Article reader: data/signals.json is empty or missing - nothing to do")
        return
    items = data["signals"]
    cache = load_json(CACHE, {"version": 1, "items": {}})
    cache.setdefault("items", {})
    st = {"read": 0, "cached": 0, "skip": 0, "budget": 0}
    todo = []
    for it in items:                                          # cached results are re-applied first (cheap, no network)
        c = cache["items"].get(it["id"])
        url = (it.get("links") or [{}])[0].get("url", "")
        if c and c.get("ok") and c.get("url") == url:
            apply(it, c["fields"])
            if c["fields"].get("cap_src"):
                it["capacity_text"], it["capacity_src"] = c["fields"]["cap"], c["fields"]["cap_src"]
            st["cached"] += 1
        elif c and not c.get("ok") and c.get("tries", 0) >= MAX_TRIES and c.get("url") == url and \
                time.time() - c.get("ts", 0) < 24 * 3600:                # gave up after 3 tries - try again after a day
            st["skip"] += 1
        else:
            todo.append(it)
    todo.sort(key=lambda s: s["date"], reverse=True)
    for n_done, it in enumerate(todo):
        if time.time() - T0 > BUDGET:
            print(f"Time budget reached - {len(todo) - n_done} items left for the next run")
            break
        url = (it.get("links") or [{}])[0].get("url", "")
        f = read_article(it)
        prev = cache["items"].get(it["id"], {})
        if "_fail" in f:
            cache["items"][it["id"]] = {"ok": False, "url": url, "tries": (prev.get("tries", 0) % MAX_TRIES) + 1 if prev.get("url") == url else 1,
                                        "ts": time.time(), "reason": f["_fail"]}
            print(f"  SKIP {it.get('publisher', '')}: {it['title'][:60]} - {f['_fail']}")
            st["skip"] += 1
        else:
            apply(it, f)
            cache["items"][it["id"]] = {"ok": True, "url": url, "fields": f}
            st["read"] += 1
            print(f"  READ {it.get('publisher', '')}: {it['title'][:60]}")
        time.sleep(0.4)
    # missing capacity: look at other reports of the same news
    looked = 0
    for it in items:
        if it.get("capacity_text") or not it.get("enriched_at") and not it.get("source_text"):
            continue
        c = cache["items"].get(it["id"], {})
        if c.get("fields", {}).get("cap_status") == "review" or c.get("fields", {}).get("cap_looked"):
            continue
        found = False
        for o in items:                                       # 1) other feed items on the same story
            if o is it or not o.get("capacity_text") or o.get("capacity_src"):
                continue
            if same_event(it["title"], it["date"], o["title"], o["date"], 0.4) and host(o["links"][0]["url"]) != host(it["links"][0]["url"]):
                it["capacity_text"] = o["capacity_text"]
                it["capacity_src"] = [{"label": o.get("publisher", "Other report"), "url": o["links"][0]["url"],
                                       "type": o["links"][0].get("type", "press")}]
                found = True
                break
        if not found and WEB_LOOKUP and looked < 8 and time.time() - T0 < BUDGET and \
                re.search(r"batter|storage|bess|wind|solar|pv|ccgt|ocgt|gas|nuclear|smr|chp|plant|farm", it["title"], re.I):
            looked += 1                                       # 2) web search (capped)
            cap, src = web_capacity(it)
            if cap:
                it["capacity_text"], it["capacity_src"] = cap, src
                found = True
        if c.get("ok"):
            c["fields"]["cap_looked"] = True
            if found:
                c["fields"]["cap"], c["fields"]["cap_src"] = it["capacity_text"], it["capacity_src"]
    keep = {s["id"] for s in items}
    cache["items"] = {k: v for k, v in cache["items"].items() if k in keep}
    cache["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_json(SIGNALS, data)
    write_json(CACHE, cache)
    caps = [s for s in items if s.get("capacity_text")]
    dls = [s for s in items if s.get("delay_text")]
    rev = [s for s in items if cache["items"].get(s["id"], {}).get("fields", {}).get("cap_status") == "review" and not s.get("capacity_text")]
    print(f"Article reader: {st['read']} read, {st['cached']} from cache, {st['skip']} skipped (not readable), of {len(items)} items")
    print(f"Capacity: {len(caps)} items" + (f" · {len(rev)} with several unlabeled figures left blank for review" if rev else ""))
    for s in caps[:12]:
        print(f"  {s['title'][:55]:55} -> {s['capacity_text']}" + (" (other source)" if s.get("capacity_src") else ""))
    print(f"Delay timeline: {len(dls)} items")
    for s in dls:
        print(f"  {s['title'][:55]:55} -> {s['delay_text']}")

if __name__ == "__main__":
    main()
