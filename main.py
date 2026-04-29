import requests
import json
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
from collections import defaultdict

# ── env vars ──────────────────────────────────────────────
BOT_TOKEN          = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHANNEL_RESULTS    = os.environ.get("TELEGRAM_CHANNEL_RESULTS", "")
CHANNEL_INVESTORS  = os.environ.get("TELEGRAM_CHANNEL_INVESTORS", "")
CHANNEL_ACQMERGER  = os.environ.get("TELEGRAM_CHANNEL_ACQMERGER", "")
CHANNEL_DEMERGER   = os.environ.get("TELEGRAM_CHANNEL_DEMERGER", "")
CHANNEL_MGMT       = os.environ.get("TELEGRAM_CHANNEL_MGMT", "")
CHANNEL_OTHERS     = os.environ.get("TELEGRAM_CHANNEL_OTHERS", "")
SHEET_ID           = os.environ.get("GOOGLE_SHEET_ID", "")

# ── warn loudly about any missing secrets at startup ──────
_REQUIRED = {
    "TELEGRAM_BOT_TOKEN":        BOT_TOKEN,
    "TELEGRAM_CHANNEL_RESULTS":  CHANNEL_RESULTS,
    "TELEGRAM_CHANNEL_INVESTORS":CHANNEL_INVESTORS,
    "TELEGRAM_CHANNEL_ACQMERGER":CHANNEL_ACQMERGER,
    "TELEGRAM_CHANNEL_DEMERGER": CHANNEL_DEMERGER,
    "TELEGRAM_CHANNEL_MGMT":     CHANNEL_MGMT,
    "TELEGRAM_CHANNEL_OTHERS":   CHANNEL_OTHERS,
    "GOOGLE_SHEET_ID":           SHEET_ID,
}
for _k, _v in _REQUIRED.items():
    if not _v:
        print(f"  ⚠️  WARNING: secret '{_k}' is not set — related features will be skipped")

# ── sheet tab names ────────────────────────────────────────
SHEET_RESULTS    = "Results"
SHEET_INVESTORS  = "Investors Meet"
SHEET_ACQMERGER  = "Acquisition & Merger"
SHEET_DEMERGER   = "Demerger"
SHEET_MGMT       = "Change in Management"
SHEET_OTHERS     = "Others"

# ══════════════════════════════════════════════════════════
# EXCLUSION FILTERS
# ══════════════════════════════════════════════════════════
GLOBAL_EXCLUSIONS = [
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
    "corrigendum",
    "erratum",
    "loss of share certificate",
    "duplicate share certificate",
    "sub-division of shares",
    "consolidation of shares",
    "transmission of shares",
    "intimation of record date",
    "change in registrar",
    "appointment of registrar",
]

CATEGORY_EXCLUSIONS = {
    "acqmerger": [
        "financial results", "quarterly results", "annual results",
        "unaudited results", "audited results", "half year results",
        "half yearly results", "standalone results", "consolidated results",
        "newspaper", "advertisement",
        "dividend", "record date", "book closure",
        "agm", "annual general meeting",
        "egm", "extraordinary general meeting",
        "postal ballot", "voting result",
        "rights issue", "public issue", "ipo", "fpo",
        "ncd", "non-convertible debenture", "commercial paper",
    ],
    "demerger": [
        "newspaper", "advertisement",
        "financial results", "quarterly results", "annual results",
        "dividend",
    ],
    "results": [],
    "investors": [],
    "mgmt": [
        "newspaper", "advertisement",
    ],
}

RESULTS_BOARD_MEETING_REQUIRED = [
    "financial results", "quarterly results", "q1", "q2", "q3", "q4",
    "annual results", "half year", "half yearly", "audited", "unaudited",
]

# ══════════════════════════════════════════════════════════
# CATEGORY RULES
# ══════════════════════════════════════════════════════════
RULES = {
    "results": {
        "keywords": [
            "financial results", "quarterly results", "half yearly results",
            "half year results", "annual results", "unaudited results",
            "audited results", "unaudited financial", "audited financial",
            "q1 results", "q2 results", "q3 results", "q4 results",
            "standalone results", "consolidated results",
            "board meeting",
        ],
        "sheet":   SHEET_RESULTS,
        "channel": None,
        "emoji":   "📊",
        "label":   "Financial Results",
        "priority": 4,
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
            "scheme of demerger",
            "demerger ratio",
            "demerger consideration",
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
            "appointment of cs",
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
            "promoter reclassification",
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

# ── acquisition & merger sub-categories ───────────────────
ACQ_SUBCATEGORIES = [
    ("Open Offer / Takeover",      ["open offer", "takeover", "substantial acquisition",
                                     "creeping acquisition"]),
    ("Merger / Amalgamation",      ["merger", "amalgamation", "amalgamate", "amalgamated"]),
    ("Acquisition",                ["acquisition", "acquire", "acquired", "acquiring",
                                     "acquirer", "business acquisition", "strategic acquisition",
                                     "promoter acquisition"]),
    ("Slump Sale / Business Transfer", ["slump sale", "business transfer agreement"]),
    ("Strategic Investment",       ["strategic investment"]),
    ("Share Purchase Agreement",   ["share purchase agreement", "binding term sheet",
                                     "definitive agreement"]),
    ("Letter of Intent / Due Diligence", ["letter of intent", "due diligence"]),
    ("Change in Control",          ["change in control"]),
    ("Delisting",                  ["delisting"]),
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
    t = title.lower()
    return any(ex in t for ex in GLOBAL_EXCLUSIONS)

def is_first_disclosure(title: str) -> bool:
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
    text = (title + " " + body).lower()
    matched = []

    has_demerger_word = any(kw in text for kw in [
        "demerger", "demerge", "demerged", "demerging",
        "spin-off", "spinoff", "spin off",
        "hive off", "hive-off", "hiving off",
        "carve-out", "carve out", "carved out",
        "scheme of demerger", "demerger ratio", "demerger consideration",
    ])

    for cat, rule in RULES.items():
        if not any(kw in text for kw in rule["keywords"]):
            continue

        excl = CATEGORY_EXCLUSIONS.get(cat, [])
        if any(ex in text for ex in excl):
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
                    pass
                else:
                    continue
            else:
                continue

        if cat == "acqmerger" and has_demerger_word:
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
                continue

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

def detect_acq_subcategory(text: str) -> str:
    t = text.lower()
    for label, kws in ACQ_SUBCATEGORIES:
        if any(kw in t for kw in kws):
            return label
    return "Acquisition / Merger"

def extract_investor_name(title: str, body: str) -> str:
    text = (title + " " + body).lower()
    found = [name for name in KNOWN_INVESTORS if name.lower() in text]
    return ", ".join(found) if found else ""

def build_category_label(matched_cats: list, title: str, body: str) -> str:
    sorted_cats = sorted(matched_cats, key=lambda c: RULES[c]["priority"])
    text = (title + " " + body).lower()
    labels = []
    for cat in sorted_cats:
        if cat == "investors":
            labels.append(detect_investor_subcategory(text))
        elif cat == "acqmerger":
            labels.append(detect_acq_subcategory(text))
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

# ─────────────────────────────────────────────────────────
# TELEGRAM — with proper retry_after handling
# ─────────────────────────────────────────────────────────

def send_to_channel(channel_id: str, msg: str, max_retries: int = 5):
    """Send a message to a Telegram channel with proper 429 backoff."""
    if not channel_id:
        print("  [SKIP] Channel ID is empty — skipping send")
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    channel_id,
        "text":       msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.ok:
                time.sleep(0.35)  # polite base delay
                return
            # Rate limited — respect retry_after exactly
            if r.status_code == 429:
                try:
                    retry_after = r.json().get("parameters", {}).get("retry_after", 30)
                except Exception:
                    retry_after = 30
                print(f"  [TG] 429 rate limit — waiting {retry_after}s (attempt {attempt}/{max_retries})")
                time.sleep(retry_after + 1)
                continue
            # Other errors
            print(f"  [TG] Error {r.status_code}: {r.text[:200]}")
            if attempt < max_retries:
                time.sleep(5 * attempt)
        except requests.exceptions.Timeout:
            print(f"  [TG] Timeout on attempt {attempt}/{max_retries}")
            if attempt < max_retries:
                time.sleep(10)
        except Exception as e:
            print(f"  [TG] Unexpected error: {e}")
            if attempt < max_retries:
                time.sleep(5)
    print(f"  [TG] ❌ Failed after {max_retries} attempts for channel {channel_id}")

# ─────────────────────────────────────────────────────────
# GOOGLE SHEETS — bulk batch writer
# ─────────────────────────────────────────────────────────

def build_row(sheet_name: str, ann: dict, category_label: str,
              topic, nse_link: str, screener_link: str,
              investor_name: str, is_first: bool) -> list:
    """Build a row list for a given sheet without writing anything."""
    company    = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol     = ann.get("symbol", "")
    title      = ann.get("desc", "")
    date_str   = ann.get("an_dt", "")
    now        = datetime.now().strftime("%Y-%m-%d %H:%M")
    full_topic = topic if topic else title
    first_flag = "YES" if is_first else ""

    if sheet_name == SHEET_INVESTORS:
        return [now, company, symbol, category_label, title,
                full_topic, investor_name, date_str,
                first_flag, nse_link, screener_link]
    elif sheet_name == SHEET_OTHERS:
        return [now, company, symbol, title,
                full_topic, date_str, nse_link, screener_link]
    else:
        return [now, company, symbol, category_label, title,
                full_topic, date_str, first_flag, nse_link, screener_link]


def flush_all_sheets(ws_map: dict, pending_rows: dict):
    """
    Write all accumulated rows to Google Sheets in one go.
    Uses batch append per sheet to minimise API round-trips.
    """
    total_written = 0
    for sheet_name, rows in pending_rows.items():
        if not rows:
            continue
        ws = ws_map.get(sheet_name)
        if not ws:
            print(f"  [SHEET] WARNING: '{sheet_name}' not found in ws_map — skipping {len(rows)} rows")
            continue

        print(f"  [SHEET] Writing {len(rows)} row(s) to '{sheet_name}'…")
        # Write in chunks of 50 to stay comfortably under Sheets API limits
        chunk_size = 50
        success = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            for attempt in range(1, 5):
                try:
                    ws.append_rows(chunk, value_input_option="USER_ENTERED")
                    success += len(chunk)
                    time.sleep(1.0)  # quota buffer between chunks
                    break
                except gspread.exceptions.APIError as e:
                    code = e.response.status_code if hasattr(e, "response") else 0
                    if code == 429 or code >= 500:
                        wait = 20 * attempt
                        print(f"    [SHEET] API {code} — retrying in {wait}s (attempt {attempt}/4)")
                        time.sleep(wait)
                    else:
                        print(f"    [SHEET] APIError {code}: {e}")
                        break
                except Exception as e:
                    print(f"    [SHEET] Unexpected error: {e}")
                    break

        if success == len(rows):
            print(f"  [SHEET] ✅ '{sheet_name}': all {success} row(s) written")
        else:
            print(f"  [SHEET] ⚠️  '{sheet_name}': {success}/{len(rows)} row(s) written")
        total_written += success

    print(f"  [SHEET] Total rows written across all sheets: {total_written}")


# ─────────────────────────────────────────────────────────
# AUTO-CLEANUP: delete rows older than 10 days
# ─────────────────────────────────────────────────────────

CLEANUP_DAYS = 10

def cleanup_old_rows(ws_map: dict):
    cutoff = datetime.now() - timedelta(days=CLEANUP_DAYS)
    cutoff_str = cutoff.strftime("%Y-%m-%d")
    print(f"\n[CLEANUP] Deleting rows logged before {cutoff_str} (>{CLEANUP_DAYS} days old)…")

    for sheet_name, ws in ws_map.items():
        try:
            all_rows = ws.get_all_values()
        except Exception as e:
            print(f"  [CLEANUP] Could not read '{sheet_name}': {e}")
            continue

        if len(all_rows) <= 1:
            print(f"  [CLEANUP] '{sheet_name}': empty — skipping")
            continue

        to_delete = []
        for i, row in enumerate(all_rows[1:], start=2):
            if not row or not row[0].strip():
                continue
            try:
                logged_at = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M")
                if logged_at < cutoff:
                    to_delete.append(i)
            except ValueError:
                continue

        if not to_delete:
            print(f"  [CLEANUP] '{sheet_name}': nothing to delete")
            continue

        for row_idx in reversed(to_delete):
            try:
                ws.delete_rows(row_idx)
                time.sleep(0.25)
            except Exception as e:
                print(f"  [CLEANUP] Error on row {row_idx} in '{sheet_name}': {e}")

        print(f"  [CLEANUP] '{sheet_name}': removed {len(to_delete)} old row(s)")

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

    seen_uids = set()
    unique = []
    for ann in all_data:
        uid = make_uid(ann)
        if uid not in seen_uids:
            seen_uids.add(uid)
            unique.append(ann)

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
    creds_raw = os.environ.get("GOOGLE_CREDS_JSON", "")
    if not creds_raw:
        print("  [SHEET] ERROR: GOOGLE_CREDS_JSON not set — sheet writes disabled")
        return {}
    if not SHEET_ID:
        print("  [SHEET] ERROR: GOOGLE_SHEET_ID not set — sheet writes disabled")
        return {}
    try:
        creds_dict = json.loads(creds_raw)
    except json.JSONDecodeError as e:
        print(f"  [SHEET] ERROR: Invalid GOOGLE_CREDS_JSON — {e}")
        return {}
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
        "Title", "Full Subject / Topic", "NSE Date", "NSE Circular Link", "Screener Link",
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

    # ── PHASE 1: Fetch all announcements ──────────────────
    print("\n[1] Fetching announcements (last 24 h, all pages)…")
    announcements = fetch_nse()
    print(f"    Fetched: {len(announcements)} unique records")

    seen = load_seen()
    print(f"    Seen IDs loaded: {len(seen)}")

    # ── Connect to sheets ─────────────────────────────────
    ws_map = setup_sheets()
    if ws_map:
        print(f"  [SHEET] Connected — tabs: {list(ws_map.keys())}")
    else:
        print("  [SHEET] NO sheet connection — all writes skipped")

    # ── 10-day auto-cleanup ───────────────────────────────
    if ws_map:
        cleanup_old_rows(ws_map)
    else:
        print("  [CLEANUP] Skipped — no sheet connection")

    # ── PHASE 2: Classify everything, build rows & messages ──
    print("\n[2] Classifying all announcements…")

    # pending_rows[sheet_name] = list of rows to write
    pending_rows: dict[str, list] = defaultdict(list)
    # pending_messages = list of (channel_id, message_text, label_for_log)
    pending_messages: list[tuple] = []

    new_seen = set()
    counts   = {k: 0 for k in list(RULES.keys()) + ["others", "excluded"]}
    processed = 0

    for ann in announcements:
        uid = make_uid(ann)
        if uid in seen:
            continue

        title = ann.get("desc", "")
        body  = (ann.get("attchmntText") or ann.get("subject") or "")
        new_seen.add(uid)

        if is_excluded_globally(title):
            counts["excluded"] += 1
            print(f"  [EXCLUDED] {ann.get('symbol','?')} — {title[:60]}")
            continue

        matched  = classify(title, body)
        is_first = is_first_disclosure(title)

        if not matched:
            counts["others"] += 1
            nse_link      = build_nse_link(ann)
            screener_link = build_screener_link(ann)

            # Queue sheet row
            row = build_row(SHEET_OTHERS, ann, "", None, nse_link, screener_link, "", False)
            pending_rows[SHEET_OTHERS].append(row)

            # Queue Telegram message
            msg = format_others_message(ann, nse_link, screener_link)
            pending_messages.append((CHANNEL_OTHERS, msg,
                                     f"[OTHERS] {ann.get('symbol','?')} — {title[:50]}"))

            print(f"  [QUEUED-OTHERS] {ann.get('symbol','?')} — {title[:60]}")
            processed += 1
            continue

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

        # Queue Telegram messages
        for channel_id in channels_to_notify:
            pending_messages.append((channel_id, msg, None))

        # Queue sheet rows
        for sheet_name in sheets_to_write:
            row = build_row(sheet_name, ann, category_label, topic,
                            nse_link, screener_link, investor_name, is_first)
            pending_rows[sheet_name].append(row)

        for c in matched:
            counts[c] += 1

        cross_note = " [CROSS]" if is_cross else ""
        first_note = " ⭐FIRST" if is_first else ""
        print(f"  [QUEUED]{cross_note}{first_note}[{category_label}] — {ann.get('symbol','?')}")
        processed += 1

    print(f"\n  Classification complete: {processed} to process, {counts['excluded']} excluded")

    # ── PHASE 3: Write ALL sheets FIRST (before any Telegram) ──
    print("\n[3] Writing all data to Google Sheets…")
    if ws_map and pending_rows:
        flush_all_sheets(ws_map, pending_rows)
    elif not ws_map:
        print("  [SHEET] Skipped — no sheet connection")
    else:
        print("  [SHEET] Nothing new to write")

    # Save seen IDs right after sheets are written
    seen |= new_seen
    save_seen(seen)
    print(f"  Saved {len(new_seen)} new IDs to seen_ids.json")

    # ── PHASE 4: Send Telegram messages ───────────────────
    print(f"\n[4] Sending {len(pending_messages)} Telegram message(s)…")
    tg_sent    = 0
    tg_skipped = 0
    for i, (channel_id, msg, log_label) in enumerate(pending_messages, 1):
        if log_label:
            print(f"  {log_label}")
        send_to_channel(channel_id, msg)
        tg_sent += 1
        if i % 20 == 0:
            print(f"  … {i}/{len(pending_messages)} messages sent")

    print(f"  Telegram: {tg_sent} sent, {tg_skipped} skipped")

    # ── Summary ───────────────────────────────────────────
    print("\n[5] Summary")
    print(f"    Processed  : {processed}")
    print(f"    Excluded   : {counts['excluded']} (newspapers / boilerplate)")
    print(f"    Others     : {counts['others']}")
    for cat, rule in RULES.items():
        print(f"    {rule['label']:<28}: {counts[cat]}")
    print(f"    New IDs recorded: {len(new_seen)}")
    for sheet_name, rows in pending_rows.items():
        print(f"    Rows → '{sheet_name}': {len(rows)}")
    print("=" * 55)

if __name__ == "__main__":
    main()
