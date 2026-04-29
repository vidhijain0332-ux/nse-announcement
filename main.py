import requests
import json
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# ── env vars ──────────────────────────────────────────────
BOT_TOKEN          = os.environ["TELEGRAM_BOT_TOKEN"]
CHANNEL_RESULTS    = os.environ["TELEGRAM_CHANNEL_RESULTS"]
CHANNEL_INVESTORS  = os.environ["TELEGRAM_CHANNEL_INVESTORS"]
CHANNEL_ACQMERGER  = os.environ["TELEGRAM_CHANNEL_ACQMERGER"]
CHANNEL_DEMERGER   = os.environ["TELEGRAM_CHANNEL_DEMERGER"]
CHANNEL_MGMT       = os.environ["TELEGRAM_CHANNEL_MGMT"]
CHANNEL_OTHERS     = os.environ["TELEGRAM_CHANNEL_OTHERS"]
SHEET_ID           = os.environ["GOOGLE_SHEET_ID"]

# ── sheet tab names ────────────────────────────────────────
SHEET_RESULTS    = "Results"
SHEET_INVESTORS  = "Investors Meet"
SHEET_ACQMERGER  = "Acquisition & Merger"
SHEET_DEMERGER   = "Demerger"
SHEET_MGMT       = "Change in Management"
SHEET_OTHERS     = "Others"

# ══════════════════════════════════════════════════════════
# EXCLUSION FILTERS
# Announcements whose title matches ANY of these phrases
# are dropped entirely before classification.
# This removes newspaper publications, routine filings, etc.
# ══════════════════════════════════════════════════════════
GLOBAL_EXCLUSIONS = [
    # newspaper / advertisement filings
    "copy of newspaper",
    "newspaper publication",
    "newspaper advertisement",
    "publication of advertisement",
    "newspaper clipping",
    "advertisement in newspaper",
    "published in newspaper",
    "notice published in",
    "extract of newspaper",
    "newspaper cutting",
    # routine boilerplate
    "corrigendum",
    "erratum",
    "loss of share certificate",
    "duplicate share certificate",
    "sub-division of shares",           # unless you want splits — remove if needed
    "consolidation of shares",
    "transmission of shares",
    "intimation of record date",         # pure record-date intimation (no result context)
    "change in registrar",
    "appointment of registrar",
]

# Per-category exclusions: if a title matches these, it is NOT
# classified into that specific category (but may still match others).
CATEGORY_EXCLUSIONS = {
    "acqmerger": [
        # Financial results filings — not M&A deals
        "financial results", "quarterly results", "annual results",
        "unaudited results", "audited results", "half year results",
        "half yearly results", "standalone results", "consolidated results",
        # Routine corporate actions that are not deals
        "newspaper", "advertisement",
        "dividend", "record date", "book closure",
        "agm", "annual general meeting",
        "egm", "extraordinary general meeting",
        "postal ballot", "voting result",
        # Pure fundraising (not an acquisition)
        "rights issue", "public issue", "ipo", "fpo",
        "ncd", "non-convertible debenture", "commercial paper",
    ],
    "demerger": [
        # Keep demerger exclusions minimal — only things that are
        # definitively NOT demerger-related
        "newspaper", "advertisement",
        "financial results", "quarterly results", "annual results",
        "dividend",
    ],
    "results": [
        # board meeting guard handled separately in classify()
    ],
    "investors": [
        "newspaper", "advertisement",
        # Only exclude if the announcement is PURELY about results
        # with zero investor-meeting context. Since classify() already
        # requires an investor keyword to match, these exclusions only
        # fire when BOTH an investor keyword AND a results phrase exist —
        # which is exactly a cross-post scenario we DO want.
        # So keep investors exclusion empty here and let cross-post logic handle it.
    ],
    "mgmt": [
        "newspaper", "advertisement",
    ],
}

# For "board meeting" to count as a Results trigger, at least one of
# these must ALSO appear in the text (prevents false positives).
RESULTS_BOARD_MEETING_REQUIRED = [
    "financial results", "quarterly results", "q1", "q2", "q3", "q4",
    "annual results", "half year", "half yearly", "audited", "unaudited",
]

# ══════════════════════════════════════════════════════════
# CATEGORY RULES
# Order matters for display only — classification is independent.
# ══════════════════════════════════════════════════════════
RULES = {
    "results": {
        "keywords": [
            "financial results", "quarterly results", "half yearly results",
            "half year results", "annual results", "unaudited results",
            "audited results", "unaudited financial", "audited financial",
            "q1 results", "q2 results", "q3 results", "q4 results",
            "standalone results", "consolidated results",
            # board meeting is handled specially — see classify()
            "board meeting",
        ],
        "sheet":   SHEET_RESULTS,
        "channel": None,
        "emoji":   "📊",
        "label":   "Financial Results",
        "priority": 4,   # lower number = shown first in cross-post label
    },
    "investors": {
        "keywords": [
            "investor meet", "investors meet", "analyst meet", "concall",
            "con call", "conference call", "earnings call", "q&a", "q & a",
            "investor day", "investor presentation", "analyst day",
            "road show", "roadshow", "interaction with", "transcript",
            "recording", "webinar", "investor briefing", "management meet",
            "non-deal roadshow", "ndr",
            "jefferies", "clsa", "citi", "citigroup", "bofa",
            "bank of america", "goldman sachs", "goldman", "jp morgan",
            "jpmorgan", "morgan stanley", "bandhan small cap",
            "hdfc mutual fund", "motilal oswal",
        ],
        "sheet":   SHEET_INVESTORS,
        "channel": None,
        "emoji":   "📞",
        "label":   "Investors Meet / Concall",
        "priority": 3,
    },
    "acqmerger": {
        # ── STRICT ACQ/MERGER KEYWORDS ────────────────────
        # Rule: words that specifically and primarily indicate a
        # corporate acquisition or merger event.
        # Removed: "preferential allotment" (too common for fundraising),
        # "loi" (3-letter match too risky), "spa" (same reason).
        # "scheme of arrangement" is handled in classify() below —
        # it only tags acqmerger when paired with an acq/merger keyword.
        "keywords": [
            "acquisition",
            "acquire",
            "acquired",
            "acquiring",
            "acquirer",
            "takeover",
            "open offer",
            "merger",
            "amalgamation",
            "amalgamate",
            "amalgamated",
            "slump sale",
            "business transfer agreement",
            "business acquisition",
            "strategic acquisition",
            "strategic investment",
            "share purchase agreement",
            "binding term sheet",
            "definitive agreement",
            "letter of intent",
            "due diligence",
            "delisting",
            "substantial acquisition",
            "change in control",
            "promoter acquisition",
            "creeping acquisition",
        ],
        "sheet":   SHEET_ACQMERGER,
        "channel": None,
        "emoji":   "🤝",
        "label":   "Acquisition / Merger",
        "priority": 2,
    },
    "demerger": {
        # ── STRICT DEMERGER KEYWORDS ──────────────────────
        # Rule: ONLY words that cannot appear in a non-demerger
        # announcement. Generic legal terms (scheme of arrangement,
        # NCLT, appointed date, transfer of undertaking) are
        # intentionally excluded — they fire on mergers, dividends,
        # capital reductions, AGMs, etc. and flood the sheet.
        # The word "demerger" / "demerge" / "demerged" appearing
        # in the title IS sufficient signal — NSE titles are concise.
        "keywords": [
            "demerger",
            "demerge",
            "demerged",
            "demerging",
            "spin-off",
            "spinoff",
            "spin off",
            "hive off",
            "hive-off",
            "hiving off",
            "carve-out",
            "carve out",
            "carved out",
            "scheme of demerger",       # explicitly says demerger
            "demerger ratio",           # only used in demerger filings
            "demerger consideration",   # only used in demerger filings
            "composite scheme of demerger",
        ],
        "sheet":   SHEET_DEMERGER,
        "channel": None,
        "emoji":   "🔀",
        "label":   "Demerger",
        "priority": 1,
    },
    "mgmt": {
        "keywords": [
            # director / KMP changes
            "change in directorate",
            "change in director",
            "change in management",
            "appointment of director",
            "resignation of director",
            "cessation of director",
            "re-appointment of director",
            "reappointment of director",
            "appointment of managing director",
            "appointment of md",
            "appointment of ceo",
            "resignation of ceo",
            "appointment of cfo",
            "resignation of cfo",
            "appointment of coo",
            "appointment of cs",         # company secretary
            "appointment of company secretary",
            "resignation of company secretary",
            "change in key managerial",
            "kmp",
            "whole time director",
            "executive director",
            "independent director",
            "woman director",
            "additional director",
            "director retirement",
            "change in chairman",
            "appointment of chairman",
            "change in board",
            "board reconstitution",
            "cessation of md",
            "cessation of ceo",
            "cessation of cfo",
            "change in chief executive",
            "change in chief financial",
            "change in managing director",
            "promoter reclassification",   # ownership/control shift
        ],
        "sheet":   SHEET_MGMT,
        "channel": None,
        "emoji":   "👔",
        "label":   "Change in Management",
        "priority": 5,
    },
}

RULES["results"]["channel"]   = CHANNEL_RESULTS
RULES["investors"]["channel"] = CHANNEL_INVESTORS
RULES["acqmerger"]["channel"] = CHANNEL_ACQMERGER
RULES["demerger"]["channel"]  = CHANNEL_DEMERGER
RULES["mgmt"]["channel"]      = CHANNEL_MGMT

# ── cross-post pairs ──────────────────────────────────────
CROSS_POST_PAIRS = [
    ("acqmerger", "investors"),
    ("demerger",  "investors"),
]

# ── investors meet sub-categories ─────────────────────────
INVESTOR_SUBCATEGORIES = [
    ("Transcript",             ["transcript"]),
    ("Recording",              ["recording", "webcast", "webinar"]),
    ("Concall",                ["concall", "con call", "conference call", "earnings call"]),
    ("Analyst / Broker Meet",  ["analyst meet", "broker meet", "jefferies", "clsa", "citi",
                                 "citigroup", "bofa", "bank of america", "goldman sachs",
                                 "goldman", "jp morgan", "jpmorgan", "morgan stanley",
                                 "bandhan small cap", "hdfc mutual fund", "motilal oswal"]),
    ("Institutional Meet",     ["institutional", "fund manager", "ndr", "non-deal roadshow",
                                 "investor briefing", "management meet", "management interaction"]),
    ("Investor / Analyst Day", ["investor day", "analyst day", "investor presentation",
                                 "investor meet", "investors meet", "interaction with"]),
    ("Roadshow",               ["road show", "roadshow"]),
    ("Q&A Session",            ["q&a", "q & a"]),
]

KNOWN_INVESTORS = [
    "Jefferies", "CLSA", "Citi", "Citigroup", "BofA", "Bank of America",
    "Goldman Sachs", "JP Morgan", "JPMorgan", "Morgan Stanley",
    "Bandhan Small Cap", "HDFC Mutual Fund", "Motilal Oswal",
    "Nomura", "UBS", "Macquarie", "Deutsche Bank", "Bernstein",
    "HSBC", "Kotak", "Axis Capital", "ICICI Securities", "Edelweiss",
    "Nuvama", "Emkay", "Ambit", "Systematix", "Prabhudas Lilladher",
    "Sharekhan", "Angel One", "Nirmal Bang",
]

NSE_DATE_FORMATS = [
    "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
]

# ─────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────

def load_seen():
    try:
        with open("seen_ids.json") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_seen(seen):
    with open("seen_ids.json", "w") as f:
        json.dump(list(seen), f)

def make_uid(ann):
    return (ann.get("an_dt", "") + "|" +
            ann.get("symbol", "") + "|" +
            ann.get("desc", "")[:80])

def parse_nse_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in NSE_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def is_excluded_globally(title: str) -> bool:
    """Return True if the announcement should be dropped entirely."""
    t = title.lower()
    return any(ex in t for ex in GLOBAL_EXCLUSIONS)

def is_first_disclosure(title: str) -> bool:
    """
    Heuristic: return True if this looks like an original disclosure
    rather than a follow-up / update.
    We prefer announcements that do NOT contain these follow-up markers.
    """
    t = title.lower()
    follow_up_markers = [
        "update on", "further update", "outcome of",
        "corrigendum", "addendum", "revised",
        "clarification on", "reply to", "response to",
        "reminder", "completion of", "receipt of approval",
        "receipt of no objection", "final approval",
    ]
    return not any(m in t for m in follow_up_markers)

def classify(title: str, body: str) -> list:
    """
    Returns list of matched category keys.

    Logic:
    - Each category matches independently via its keyword list.
    - Per-category exclusions prevent cross-contamination.
    - Special guard for "board meeting": only a Results hit when
      accompanied by a results-context word.
    - "merger" substring guard: "demerger" contains "merger" so we
      check that a demerger word is NOT present before tagging acqmerger
      on a merger-only match.
    - If a title has both a genuine acq keyword AND results context,
      it cross-posts to both (e.g. "acquisition update in Q1 results").
    """
    text = (title + " " + body).lower()
    matched = []

    # Pre-compute whether text contains a demerger-specific word
    # (used to guard against "merger" substring in "demerger")
    has_demerger_word = any(kw in text for kw in [
        "demerger", "demerge", "demerged", "demerging",
        "spin-off", "spinoff", "spin off",
        "hive off", "hive-off", "hiving off",
        "carve-out", "carve out", "carved out",
        "scheme of demerger", "demerger ratio", "demerger consideration",
    ])

    for cat, rule in RULES.items():
        # Must match at least one keyword
        if not any(kw in text for kw in rule["keywords"]):
            continue

        # Per-category exclusions
        excl = CATEGORY_EXCLUSIONS.get(cat, [])
        if any(ex in text for ex in excl):
            # Special case: acqmerger exclusion has "financial results" etc.
            # But if there is ALSO a genuine non-merger acq keyword present
            # (acquisition, takeover, open offer etc.), still allow acqmerger.
            if cat == "acqmerger":
                strong_acq_kws = [
                    "acquisition", "acquire", "acquired", "acquiring", "acquirer",
                    "takeover", "open offer", "slump sale", "delisting",
                    "substantial acquisition", "change in control",
                    "promoter acquisition", "creeping acquisition",
                    "share purchase agreement", "binding term sheet",
                    "definitive agreement", "letter of intent",
                ]
                if any(kw in text for kw in strong_acq_kws):
                    pass  # override the exclusion — genuine acquisition
                else:
                    continue
            else:
                continue

        # ── "merger" substring guard ──────────────────────
        # "demerger" contains "merger". Without this guard, a title like
        # "Demerger of XYZ division" would also fire acqmerger.
        if cat == "acqmerger" and has_demerger_word:
            # Only flag acqmerger if there is a non-merger acq keyword too
            non_merger_acq_kws = [
                "acquisition", "acquire", "acquired", "acquiring", "acquirer",
                "takeover", "open offer", "slump sale", "delisting",
                "substantial acquisition", "change in control",
                "promoter acquisition", "creeping acquisition",
                "share purchase agreement", "binding term sheet",
                "definitive agreement", "letter of intent", "due diligence",
                "amalgamation", "amalgamate", "amalgamated",
            ]
            if not any(kw in text for kw in non_merger_acq_kws):
                continue  # pure demerger — don't also tag as acqmerger

        # ── board meeting guard ───────────────────────────
        if cat == "results":
            other_results_kw = [kw for kw in rule["keywords"] if kw != "board meeting"]
            has_other = any(kw in text for kw in other_results_kw)
            has_bm    = "board meeting" in text
            if has_bm and not has_other:
                if not any(r in text for r in RESULTS_BOARD_MEETING_REQUIRED):
                    continue

        matched.append(cat)

    return matched

def detect_cross_post(matched: list) -> bool:
    return any(a in matched and b in matched for a, b in CROSS_POST_PAIRS)

def extract_topic(title: str, body: str) -> str | None:
    title = (title or "").strip()
    body  = (body  or "").strip()
    if body and body.lower() not in ("nan", "none", "") and body != title:
        return f"{title} | {body}" if title else body
    return title or None

def detect_investor_subcategory(text: str) -> str:
    t = text.lower()
    for label, kws in INVESTOR_SUBCATEGORIES:
        if any(kw in t for kw in kws):
            return label
    return "Investors Meet"

def extract_investor_name(title: str, body: str) -> str:
    text = (title + " " + body).lower()
    found = [name for name in KNOWN_INVESTORS if name.lower() in text]
    return ", ".join(found) if found else ""

def build_category_label(matched_cats: list, title: str, body: str) -> str:
    # Sort by priority so higher-priority categories appear first
    sorted_cats = sorted(matched_cats, key=lambda c: RULES[c]["priority"])
    text = (title + " " + body).lower()
    labels = []
    for cat in sorted_cats:
        if cat == "investors":
            labels.append(detect_investor_subcategory(text))
        else:
            labels.append(RULES[cat]["label"])
    return " + ".join(labels)

def build_nse_link(ann: dict) -> str:
    attachment = ann.get("attchmntFile", "")
    symbol     = ann.get("symbol", "")
    if attachment:
        if attachment.startswith("http"):
            return attachment
        return f"https://www.nseindia.com{attachment}"
    if symbol:
        return (f"https://www.nseindia.com/get-quotes/equity?"
                f"symbol={symbol}#corporate-announcements")
    return "https://www.nseindia.com/companies-listing/corporate-filings-announcements"

def build_screener_link(ann: dict) -> str:
    symbol = ann.get("symbol", "")
    if symbol:
        return f"https://www.screener.in/company/{symbol}/announcements/"
    return "https://www.screener.in"

def format_message(ann: dict, matched_cats: list, topic, category_label: str,
                   nse_link: str, screener_link: str, investor_name: str,
                   is_first: bool) -> str:
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    emojis   = " ".join(RULES[c]["emoji"] for c in sorted(matched_cats,
                         key=lambda c: RULES[c]["priority"]))
    first_tag = "🔔 *FIRST DISCLOSURE*\n" if is_first else ""

    msg = (
        f"{first_tag}"
        f"{emojis} *{category_label}*\n\n"
        f"🏢 *{company}* (`{symbol}`)\n"
        f"📋 {title}\n"
    )
    if investor_name:
        msg += f"👤 *Investor:* {investor_name}\n"
    if topic and topic != title:
        msg += f"📝 {topic}\n"
    msg += (
        f"📅 {date_str}\n"
        f"🔗 [NSE Circular]({nse_link})\n"
        f"📈 [Screener]({screener_link})"
    )
    return msg

def format_others_message(ann: dict, nse_link: str, screener_link: str) -> str:
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    return (
        f"📌 *Other Announcement*\n\n"
        f"🏢 *{company}* (`{symbol}`)\n"
        f"📋 {title}\n"
        f"📅 {date_str}\n"
        f"🔗 [NSE Circular]({nse_link})\n"
        f"📈 [Screener]({screener_link})"
    )

def send_to_channel(channel_id: str, msg: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    channel_id,
        "text":       msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=15)
    if not r.ok:
        print(f"  Telegram error [{channel_id}]: {r.text}")
    time.sleep(0.4)

def append_to_sheet(ws_map, sheet_name: str, ann: dict, category_label: str,
                    topic, nse_link: str, screener_link: str,
                    investor_name: str, is_first: bool):
    ws = ws_map.get(sheet_name)
    if not ws:
        return
    company   = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol    = ann.get("symbol", "")
    title     = ann.get("desc", "")
    date_str  = ann.get("an_dt", "")
    now       = datetime.now().strftime("%Y-%m-%d %H:%M")
    full_topic = topic if topic else title
    first_flag = "YES" if is_first else ""

    if sheet_name == SHEET_INVESTORS:
        ws.append_row([now, company, symbol, category_label, title,
                       full_topic, investor_name, date_str,
                       first_flag, nse_link, screener_link])
    elif sheet_name == SHEET_OTHERS:
        ws.append_row([now, company, symbol, title,
                       date_str, nse_link, screener_link])
    else:
        ws.append_row([now, company, symbol, category_label, title,
                       full_topic, date_str, first_flag, nse_link, screener_link])

# ─────────────────────────────────────────────────────────
# NSE FETCH
# ─────────────────────────────────────────────────────────

def fetch_nse_page(session, headers: dict, from_date: str, to_date: str, page: int):
    url = (
        f"https://www.nseindia.com/api/corporate-announcements"
        f"?index=equities"
        f"&from_date={from_date}&to_date={to_date}"
        f"&page={page}"
    )
    resp = session.get(url, headers=headers, timeout=20)
    if not resp.ok:
        print(f"  Page {page} HTTP {resp.status_code} — stopping")
        return [], 0
    result = resp.json()
    if isinstance(result, list):
        return result, len(result)
    if isinstance(result, dict):
        data  = result.get("data", [])
        total = result.get("total", len(data))
        return data, total
    return [], 0

def fetch_nse() -> list:
    """
    Fetch all announcements from the last 24 hours across all pages.
    Returns deduplicated list sorted with FIRST disclosures at the top.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":           "application/json, text/plain, */*",
        "Referer":          "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=15)
    time.sleep(2)

    now       = datetime.now()
    yesterday = now - timedelta(hours=24)
    from_date = yesterday.strftime("%d-%m-%Y")
    to_date   = now.strftime("%d-%m-%Y")
    print(f"  Date range: {from_date} → {to_date}")

    all_data = []
    page     = 1

    while True:
        print(f"  Fetching page {page}…")
        data, total = fetch_nse_page(session, headers, from_date, to_date, page)
        if not data:
            break
        all_data.extend(data)
        print(f"  Page {page}: {len(data)} records | total so far: {len(all_data)}")
        if len(all_data) >= total > 0 or len(data) < 10:
            break
        page += 1
        time.sleep(1)

    # Deduplicate within batch
    seen_uids = set()
    unique = []
    for ann in all_data:
        uid = make_uid(ann)
        if uid not in seen_uids:
            seen_uids.add(uid)
            unique.append(ann)

    # Sort: first disclosures first, then by date descending
    def sort_key(ann):
        first = 0 if is_first_disclosure(ann.get("desc", "")) else 1
        dt    = parse_nse_date(ann.get("an_dt", "")) or datetime.min
        return (first, -dt.timestamp())

    unique.sort(key=sort_key)
    print(f"  Total unique after dedup + sort: {len(unique)}")
    return unique

# ─────────────────────────────────────────────────────────
# GOOGLE SHEETS SETUP
# ─────────────────────────────────────────────────────────

def setup_sheets() -> dict:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    wb     = client.open_by_key(SHEET_ID)

    std_header = [
        "Logged At", "Company", "Symbol", "Category",
        "Title", "Full Subject / Topic",
        "NSE Date", "First Disclosure?", "NSE Circular Link", "Screener Link",
    ]
    inv_header = [
        "Logged At", "Company", "Symbol", "Category",
        "Title", "Full Subject / Topic", "Investor Name",
        "NSE Date", "First Disclosure?", "NSE Circular Link", "Screener Link",
    ]
    oth_header = [
        "Logged At", "Company", "Symbol",
        "Title", "NSE Date", "NSE Circular Link", "Screener Link",
    ]

    tab_headers = {
        SHEET_RESULTS:   std_header,
        SHEET_INVESTORS: inv_header,
        SHEET_ACQMERGER: std_header,
        SHEET_DEMERGER:  std_header,
        SHEET_MGMT:      std_header,
        SHEET_OTHERS:    oth_header,
    }

    existing = {ws.title.strip().lower(): ws for ws in wb.worksheets()}
    print(f"  Existing tabs: {list(existing.keys())}")
    ws_map = {}

    for tab, header in tab_headers.items():
        key = tab.strip().lower()
        if key in existing:
            ws = existing[key]
            if ws.title != tab:
                ws.update_title(tab)
                print(f"  Renamed → '{tab}'")
            else:
                print(f"  Found '{tab}'")
        else:
            ws = wb.add_worksheet(title=tab, rows=2000, cols=len(header) + 2)
            ws.append_row(header)
            col_letter = chr(ord("A") + len(header) - 1)
            ws.format(f"A1:{col_letter}1", {"textFormat": {"bold": True}})
            print(f"  Created '{tab}'")
        ws_map[tab] = ws

    return ws_map

# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("NSE Announcement Bot — starting run")
    print("=" * 55)

    print("\n[1] Fetching announcements (last 24 h, all pages)…")
    announcements = fetch_nse()
    print(f"    Fetched: {len(announcements)} unique records")

    seen     = load_seen()
    ws_map   = setup_sheets()
    new_seen = set()

    counts = {k: 0 for k in list(RULES.keys()) + ["others", "excluded"]}
    processed = 0

    print("\n[2] Processing…")
    for ann in announcements:
        uid = make_uid(ann)
        if uid in seen:
            continue

        title = ann.get("desc", "")
        body  = (ann.get("attchmntText") or ann.get("subject") or "")
        new_seen.add(uid)

        # ── global exclusion (newspapers, boilerplate) ────
        if is_excluded_globally(title):
            counts["excluded"] += 1
            print(f"  [EXCLUDED] {ann.get('symbol','?')} — {title[:60]}")
            continue

        matched  = classify(title, body)
        is_first = is_first_disclosure(title)

        # ── OTHERS: did not match any category ────────────
        if not matched:
            counts["others"] += 1
            nse_link      = build_nse_link(ann)
            screener_link = build_screener_link(ann)
            msg = format_others_message(ann, nse_link, screener_link)
            send_to_channel(CHANNEL_OTHERS, msg)
            append_to_sheet(ws_map, SHEET_OTHERS, ann, "", None,
                            nse_link, screener_link, "", False)
            print(f"  [OTHERS] {ann.get('symbol','?')} — {title[:60]}")
            processed += 1
            continue

        # ── normal categorised announcement ──────────────
        topic          = extract_topic(title, body)
        is_cross       = detect_cross_post(matched)
        category_label = build_category_label(matched, title, body)
        nse_link       = build_nse_link(ann)
        screener_link  = build_screener_link(ann)
        investor_name  = extract_investor_name(title, body) if "investors" in matched else ""
        msg            = format_message(ann, matched, topic, category_label,
                                        nse_link, screener_link, investor_name, is_first)

        channels_to_notify = list(dict.fromkeys(RULES[c]["channel"] for c in matched))
        sheets_to_write    = list(dict.fromkeys(RULES[c]["sheet"]   for c in matched))

        for channel_id in channels_to_notify:
            send_to_channel(channel_id, msg)

        for sheet_name in sheets_to_write:
            append_to_sheet(ws_map, sheet_name, ann, category_label, topic,
                            nse_link, screener_link, investor_name, is_first)

        for c in matched:
            counts[c] += 1

        cross_note = " [CROSS]" if is_cross else ""
        first_note = " ⭐FIRST" if is_first else ""
        print(f"  {cross_note}{first_note}[{category_label}] — {ann.get('symbol','?')}")
        processed += 1

    seen |= new_seen
    save_seen(seen)

    print("\n[3] Summary")
    print(f"    Processed  : {processed}")
    print(f"    Excluded   : {counts['excluded']} (newspapers / boilerplate)")
    print(f"    Others     : {counts['others']}")
    for cat, rule in RULES.items():
        print(f"    {rule['label']:<28}: {counts[cat]}")
    print(f"    New IDs recorded: {len(new_seen)}")
    print("=" * 55)

if __name__ == "__main__":
    main()
