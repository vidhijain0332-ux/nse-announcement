import requests
import json
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

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
            # meeting / event types
            "investor meet", "investors meet", "analyst meet", "concall",
            "con call", "conference call", "earnings call", "q&a", "q & a",
            "investor day", "investor presentation", "analyst day",
            "road show", "roadshow", "interaction with", "transcript",
            "recording", "webinar", "investor briefing", "management meet",
            "non-deal roadshow", "ndr",
            # institutional / brokerage names
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

# Assign channels
RULES["results"]["channel"]   = CHANNEL_RESULTS
RULES["investors"]["channel"] = CHANNEL_INVESTORS
RULES["acqmerger"]["channel"] = CHANNEL_ACQMERGER
RULES["demerger"]["channel"]  = CHANNEL_DEMERGER

# ── cross-post pairs ──────────────────────────────────────
CROSS_POST_PAIRS = [
    ("acqmerger", "investors"),
    ("demerger",  "investors"),
]

# ── investors meet sub-category detection ─────────────────
INVESTOR_SUBCATEGORIES = [
    ("Transcript",              ["transcript"]),
    ("Recording",               ["recording", "webcast", "webinar"]),
    ("Concall",                 ["concall", "con call", "conference call", "earnings call"]),
    ("Analyst / Broker Meet",   ["analyst meet", "broker meet", "jefferies", "clsa", "citi",
                                  "citigroup", "bofa", "bank of america", "goldman sachs",
                                  "goldman", "jp morgan", "jpmorgan", "morgan stanley",
                                  "bandhan small cap", "hdfc mutual fund", "motilal oswal"]),
    ("Institutional Meet",      ["institutional", "fund manager", "ndr", "non-deal roadshow",
                                  "investor briefing", "management meet", "management interaction"]),
    ("Investor / Analyst Day",  ["investor day", "analyst day", "investor presentation",
                                  "investor meet", "investors meet", "interaction with"]),
    ("Roadshow",                ["road show", "roadshow"]),
    ("Q&A Session",             ["q&a", "q & a"]),
]

def detect_investor_subcategory(text: str) -> str:
    t = text.lower()
    for label, kws in INVESTOR_SUBCATEGORIES:
        if any(kw in t for kw in kws):
            return label
    return "Investors Meet"

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

def classify(title, body):
    text = (title + " " + body).lower()
    return [cat for cat, rule in RULES.items()
            if any(kw in text for kw in rule["keywords"])]

def detect_cross_post(matched):
    return any(a in matched and b in matched for a, b in CROSS_POST_PAIRS)

def extract_topic(title, body):
    """Return full subject/topic — NO truncation."""
    title = (title or "").strip()
    body  = (body  or "").strip()
    if body and body.lower() not in ("nan", "none", "") and body != title:
        return f"{title} | {body}" if title else body
    return title or None

def build_category_label(matched_cats, title, body):
    """Build category label; investors gets specific sub-category."""
    labels = []
    text   = (title + " " + body).lower()
    for cat in matched_cats:
        if cat == "investors":
            labels.append(detect_investor_subcategory(text))
        else:
            labels.append(RULES[cat]["label"])
    return " + ".join(labels)

def format_message(ann, matched_cats, topic, category_label):
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    link     = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    emojis   = " ".join(RULES[c]["emoji"] for c in matched_cats)

    msg = (
        f"{emojis} *{category_label}*\n\n"
        f"🏢 *{company}* (`{symbol}`)\n"
        f"📋 {title}\n"
    )
    if topic and topic != title:
        msg += f"📝 {topic}\n"
    msg += f"📅 {date_str}\n🔗 [View on NSE]({link})"
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

def append_to_sheet(ws_map, sheet_name, ann, category_label, topic):
    ws = ws_map.get(sheet_name)
    if not ws:
        return
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    now      = datetime.now().strftime("%Y-%m-%d %H:%M")
    link     = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    # Full subject — no truncation at all
    full_topic = topic if topic else title
    ws.append_row([now, company, symbol, category_label, title, full_topic, date_str, link])

def fetch_nse():
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
    url  = "https://www.nseindia.com/api/corporate-announcements?index=equities"
    resp = session.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    result = resp.json()
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("data", [])
    return []

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
    header = ["Logged At", "Company", "Symbol", "Category",
              "Title", "Full Subject / Topic", "NSE Date", "Link"]

    existing = {ws.title.strip().lower(): ws for ws in wb.worksheets()}
    print(f"  Existing tabs: {list(existing.keys())}")

    for tab in [SHEET_RESULTS, SHEET_INVESTORS, SHEET_ACQMERGER, SHEET_DEMERGER]:
        key = tab.strip().lower()
        if key in existing:
            ws = existing[key]
            if ws.title != tab:
                ws.update_title(tab)
                print(f"  Renamed tab '{ws.title}' -> '{tab}'")
            else:
                print(f"  Found tab '{tab}'")
        else:
            ws = wb.add_worksheet(title=tab, rows=2000, cols=10)
            ws.append_row(header)
            ws.format("A1:H1", {"textFormat": {"bold": True}})
            print(f"  Created tab '{tab}'")
        ws_map[tab] = ws
    return ws_map

# ── main ──────────────────────────────────────────────────

def main():
    print("Fetching NSE announcements...")
    announcements = fetch_nse()
    print(f"  Got {len(announcements)} records")

    seen     = load_seen()
    ws_map   = setup_sheets()
    new_seen = set()

    for ann in announcements:
        uid = make_uid(ann)
        if uid in seen:
            continue

        title = ann.get("desc", "")
        body  = (ann.get("attchmntText") or ann.get("subject") or "")

        matched = classify(title, body)
        new_seen.add(uid)

        if not matched:
            continue

        topic          = extract_topic(title, body)
        is_cross       = detect_cross_post(matched)
        category_label = build_category_label(matched, title, body)
        msg            = format_message(ann, matched, topic, category_label)

        channels_to_notify = list(dict.fromkeys(RULES[c]["channel"] for c in matched))
        sheets_to_write    = list(dict.fromkeys(RULES[c]["sheet"]   for c in matched))

        for channel_id in channels_to_notify:
            send_to_channel(channel_id, msg)

        for sheet_name in sheets_to_write:
            append_to_sheet(ws_map, sheet_name, ann, category_label, topic)

        cross_note = " [CROSS-POST]" if is_cross else ""
        print(f"  {cross_note}[{category_label}] {ann.get('symbol', '?')}")

    seen |= new_seen
    save_seen(seen)
    print(f"Done. {len(new_seen)} new IDs recorded.")

if __name__ == "__main__":
    main()
