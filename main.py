import requests
import json
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# ── env vars ──────────────────────────────────────────────
BOT_TOKEN         = os.environ["TELEGRAM_BOT_TOKEN"]
CHANNEL_RESULTS   = os.environ["TELEGRAM_CHANNEL_RESULTS"]
CHANNEL_INVESTORS = os.environ["TELEGRAM_CHANNEL_INVESTORS"]
CHANNEL_ACQMERGER = os.environ["TELEGRAM_CHANNEL_ACQMERGER"]
CHANNEL_DEMERGER  = os.environ["TELEGRAM_CHANNEL_DEMERGER"]
SHEET_ID          = os.environ["GOOGLE_SHEET_ID"]

# ── sheet tab names ────────────────────────────────────────
SHEET_RESULTS    = "Results"
SHEET_INVESTORS  = "Investors Meet"
SHEET_ACQMERGER  = "Acquisition & Merger"
SHEET_DEMERGER   = "Demerger"

# ── category rules ────────────────────────────────────────
RULES = {
    "results": {
        "keywords": [
            "financial results", "quarterly results", "half yearly results",
            "half year results", "annual results", "unaudited results",
            "audited results", "unaudited financial", "audited financial",
            "q1 results", "q2 results", "q3 results", "q4 results",
            "standalone results", "consolidated results", "board meeting"
        ],
        "sheet":   SHEET_RESULTS,
        "channel": None,
        "emoji":   "📊",
        "label":   "Financial Results"
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
            "hdfc mutual fund", "motilal oswal"
        ],
        "sheet":   SHEET_INVESTORS,
        "channel": None,
        "emoji":   "📞",
        "label":   "Investors Meet / Concall"
    },
    "acqmerger": {
        "keywords": [
            "acquisition", "acquire", "takeover", "merger", "amalgamation",
            "scheme of arrangement", "slump sale", "business transfer",
            "strategic investment", "open offer", "delisting",
            "share purchase agreement", "spa", "binding term sheet",
            "letter of intent", "loi", "due diligence"
        ],
        "sheet":   SHEET_ACQMERGER,
        "channel": None,
        "emoji":   "🤝",
        "label":   "Acquisition / Merger"
    },
    "demerger": {
        "keywords": [
            "demerger", "demerge", "spin-off", "spinoff",
            "hive off", "hive-off", "carved out", "carve-out",
            "separate listing", "composite scheme"
        ],
        "sheet":   SHEET_DEMERGER,
        "channel": None,
        "emoji":   "🔀",
        "label":   "Demerger"
    },
}

RULES["results"]["channel"]   = CHANNEL_RESULTS
RULES["investors"]["channel"] = CHANNEL_INVESTORS
RULES["acqmerger"]["channel"] = CHANNEL_ACQMERGER
RULES["demerger"]["channel"]  = CHANNEL_DEMERGER

CROSS_POST_PAIRS = [
    ("acqmerger", "investors"),
    ("demerger",  "investors"),
]

# ── investors meet sub-category detection ─────────────────
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

# ── known investor names to extract ───────────────────────
KNOWN_INVESTORS = [
    "Jefferies", "CLSA", "Citi", "Citigroup", "BofA", "Bank of America",
    "Goldman Sachs", "JP Morgan", "JPMorgan", "Morgan Stanley",
    "Bandhan Small Cap", "HDFC Mutual Fund", "Motilal Oswal",
    "Nomura", "UBS", "Macquarie", "Deutsche Bank", "Bernstein",
    "HSBC", "Kotak", "Axis Capital", "ICICI Securities", "Edelweiss",
    "Nuvama", "Emkay", "Ambit", "Systematix", "Prabhudas Lilladher",
    "Sharekhan", "Angel One", "Nirmal Bang",
]

# ── NSE date formats ───────────────────────────────────────
NSE_DATE_FORMATS = [
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y %H:%M",
    "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y",
]

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
    """Try all known NSE date formats. Returns datetime or None."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in NSE_DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def is_within_24h(ann):
    """
    Return True if announcement is within last 24 hours.
    If date cannot be parsed, INCLUDE it (don't drop it).
    """
    dt = parse_nse_date(ann.get("an_dt", ""))
    if dt is None:
        return True   # unknown date → include to be safe
    return dt >= datetime.now() - timedelta(hours=24)

def classify(title, body):
    text = (title + " " + body).lower()
    return [cat for cat, rule in RULES.items()
            if any(kw in text for kw in rule["keywords"])]

def detect_cross_post(matched):
    return any(a in matched and b in matched for a, b in CROSS_POST_PAIRS)

def extract_topic(title, body):
    """Return full subject/topic — NO truncation at all."""
    title = (title or "").strip()
    body  = (body  or "").strip()
    if body and body.lower() not in ("nan", "none", "") and body != title:
        return f"{title} | {body}" if title else body
    return title or None

def detect_investor_subcategory(text):
    t = text.lower()
    for label, kws in INVESTOR_SUBCATEGORIES:
        if any(kw in t for kw in kws):
            return label
    return "Investors Meet"

def extract_investor_name(title, body):
    """Scan for known institution names and return them comma-separated."""
    text = (title + " " + body).lower()
    found = [name for name in KNOWN_INVESTORS if name.lower() in text]
    return ", ".join(found) if found else ""

def build_category_label(matched_cats, title, body):
    labels = []
    text   = (title + " " + body).lower()
    for cat in matched_cats:
        if cat == "investors":
            labels.append(detect_investor_subcategory(text))
        else:
            labels.append(RULES[cat]["label"])
    return " + ".join(labels)

def build_nse_link(ann):
    """Direct PDF/circular link from attchmntFile, else symbol page."""
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

def build_screener_link(ann):
    symbol = ann.get("symbol", "")
    if symbol:
        return f"https://www.screener.in/company/{symbol}/announcements/"
    return "https://www.screener.in"

def format_message(ann, matched_cats, topic, category_label,
                   nse_link, screener_link, investor_name):
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    emojis   = " ".join(RULES[c]["emoji"] for c in matched_cats)

    msg = (
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

def send_to_channel(channel_id, msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    channel_id,
        "text":       msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=15)
    if not r.ok:
        print(f"  Telegram error [{channel_id}]: {r.text}")
    time.sleep(0.4)

def append_to_sheet(ws_map, sheet_name, ann, category_label, topic,
                    nse_link, screener_link, investor_name):
    ws = ws_map.get(sheet_name)
    if not ws:
        return
    company    = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol     = ann.get("symbol", "")
    title      = ann.get("desc", "")
    date_str   = ann.get("an_dt", "")
    now        = datetime.now().strftime("%Y-%m-%d %H:%M")
    full_topic = topic if topic else title

    if sheet_name == SHEET_INVESTORS:
        ws.append_row([now, company, symbol, category_label, title,
                       full_topic, investor_name, date_str, nse_link, screener_link])
    else:
        ws.append_row([now, company, symbol, category_label, title,
                       full_topic, date_str, nse_link, screener_link])

def fetch_nse_page(session, headers, from_date, to_date, page):
    """Fetch one page of NSE announcements."""
    url = (
        f"https://www.nseindia.com/api/corporate-announcements"
        f"?index=equities"
        f"&from_date={from_date}&to_date={to_date}"
        f"&page={page}"
    )
    resp = session.get(url, headers=headers, timeout=20)
    if not resp.ok:
        print(f"  Page {page} HTTP {resp.status_code} — stopping pagination")
        return [], 0
    result = resp.json()
    if isinstance(result, list):
        return result, len(result)
    if isinstance(result, dict):
        data  = result.get("data", [])
        total = result.get("total", len(data))
        return data, total
    return [], 0

def fetch_nse():
    """
    Fetch ALL announcements from the last 24 hours by paginating
    through the NSE corporate announcements API.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept":          "application/json, text/plain, */*",
        "Referer":         "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=15)
    time.sleep(2)

    # Date range: last 24 hours
    now       = datetime.now()
    yesterday = now - timedelta(hours=24)
    from_date = yesterday.strftime("%d-%m-%Y")
    to_date   = now.strftime("%d-%m-%Y")

    print(f"  Fetching announcements from {from_date} to {to_date}")

    all_announcements = []
    page = 1

    while True:
        print(f"  Fetching page {page}...")
        data, total = fetch_nse_page(session, headers, from_date, to_date, page)

        if not data:
            print(f"  No data on page {page} — done paginating")
            break

        all_announcements.extend(data)
        print(f"  Page {page}: got {len(data)} records (total so far: {len(all_announcements)})")

        # Stop if we have fetched all available records
        if len(all_announcements) >= total and total > 0:
            print(f"  Fetched all {total} available records")
            break

        # Stop if page returned fewer than expected (last page)
        if len(data) < 10:
            break

        page += 1
        time.sleep(1)   # be polite between page requests

    # Deduplicate by uid within the fetched batch
    seen_in_batch = set()
    unique = []
    for ann in all_announcements:
        uid = make_uid(ann)
        if uid not in seen_in_batch:
            seen_in_batch.add(uid)
            unique.append(ann)

    return unique

def setup_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    wb     = client.open_by_key(SHEET_ID)
    ws_map = {}

    std_header = ["Logged At", "Company", "Symbol", "Category",
                  "Title", "Full Subject / Topic",
                  "NSE Date", "NSE Circular Link", "Screener Link"]

    inv_header = ["Logged At", "Company", "Symbol", "Category",
                  "Title", "Full Subject / Topic", "Investor Name",
                  "NSE Date", "NSE Circular Link", "Screener Link"]

    existing = {ws.title.strip().lower(): ws for ws in wb.worksheets()}
    print(f"  Existing tabs: {list(existing.keys())}")

    for tab in [SHEET_RESULTS, SHEET_INVESTORS, SHEET_ACQMERGER, SHEET_DEMERGER]:
        key    = tab.strip().lower()
        header = inv_header if tab == SHEET_INVESTORS else std_header

        if key in existing:
            ws = existing[key]
            if ws.title != tab:
                ws.update_title(tab)
                print(f"  Renamed tab '{ws.title}' -> '{tab}'")
            else:
                print(f"  Found tab '{tab}'")
        else:
            ws = wb.add_worksheet(title=tab, rows=2000, cols=12)
            ws.append_row(header)
            ws.format("A1:J1", {"textFormat": {"bold": True}})
            print(f"  Created tab '{tab}'")
        ws_map[tab] = ws
    return ws_map

# ── main ──────────────────────────────────────────────────

def main():
    print("Fetching NSE announcements (last 24 hours, all pages)...")
    announcements = fetch_nse()
    print(f"  Total unique announcements fetched: {len(announcements)}")

    seen     = load_seen()
    ws_map   = setup_sheets()
    new_seen = set()
    processed = 0

    for ann in announcements:
        uid = make_uid(ann)
        if uid in seen:
            continue   # already sent in a previous run

        title = ann.get("desc", "")
        body  = (ann.get("attchmntText") or ann.get("subject") or "")

        matched = classify(title, body)
        new_seen.add(uid)   # always mark seen

        if not matched:
            continue

        topic          = extract_topic(title, body)
        is_cross       = detect_cross_post(matched)
        category_label = build_category_label(matched, title, body)
        nse_link       = build_nse_link(ann)
        screener_link  = build_screener_link(ann)
        investor_name  = extract_investor_name(title, body) if "investors" in matched else ""
        msg            = format_message(ann, matched, topic, category_label,
                                        nse_link, screener_link, investor_name)

        channels_to_notify = list(dict.fromkeys(RULES[c]["channel"] for c in matched))
        sheets_to_write    = list(dict.fromkeys(RULES[c]["sheet"]   for c in matched))

        for channel_id in channels_to_notify:
            send_to_channel(channel_id, msg)

        for sheet_name in sheets_to_write:
            append_to_sheet(ws_map, sheet_name, ann, category_label, topic,
                            nse_link, screener_link, investor_name)

        cross_note = " [CROSS-POST]" if is_cross else ""
        inv_note   = f" | {investor_name}" if investor_name else ""
        print(f"  {cross_note}[{category_label}]{inv_note} — {ann.get('symbol','?')}")
        processed += 1

    seen |= new_seen
    save_seen(seen)
    print(f"\nDone. {processed} new announcements sent. {len(new_seen)} total IDs recorded this run.")

if __name__ == "__main__":
    main()
