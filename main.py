import requests
import json
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ── env vars ──────────────────────────────────────────────
BOT_TOKEN         = os.environ["TELEGRAM_BOT_TOKEN"]
CHANNEL_RESULTS   = os.environ["TELEGRAM_CHANNEL_RESULTS"]    # @channel or -100xxxxxxxxxx
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
    "demerger": {
        "keywords": ["demerger", "demerge", "spin-off", "spinoff", "hive off", "hive-off"],
        "sheet":    SHEET_DEMERGER,
        "channel":  None,
        "emoji":    "🔀",
        "label":    "Demerger"
    },
    "acqmerger": {
        "keywords": [
            "acquisition", "acquire", "takeover", "merger", "amalgamation",
            "scheme of arrangement", "slump sale", "business transfer",
            "strategic investment", "open offer", "delisting"
        ],
        "sheet":    SHEET_ACQMERGER,
        "channel":  None,
        "emoji":    "🤝",
        "label":    "Acquisition / Merger"
    },
    "investors": {
        "keywords": [
            "investor meet", "investors meet", "analyst meet", "concall",
            "conference call", "earnings call", "q&a", "q & a",
            "investor day", "investor presentation", "analyst day",
            "road show", "roadshow", "interaction with"
        ],
        "sheet":    SHEET_INVESTORS,
        "channel":  None,
        "emoji":    "📞",
        "label":    "Investors Meet / Concall"
    },
    "results": {
        "keywords": [
            "financial results", "quarterly results", "q1 results", "q2 results",
            "q3 results", "q4 results", "annual results", "board meeting",
            "unaudited results", "audited results", "half year results",
            "standalone results", "consolidated results"
        ],
        "sheet":    SHEET_RESULTS,
        "channel":  None,
        "emoji":    "📊",
        "label":    "Financial Results"
    }
}

# Assign channels after env vars are loaded
RULES["demerger"]["channel"]  = CHANNEL_DEMERGER
RULES["acqmerger"]["channel"] = CHANNEL_ACQMERGER
RULES["investors"]["channel"] = CHANNEL_INVESTORS
RULES["results"]["channel"]   = CHANNEL_RESULTS

# ── cross-post pairs ──────────────────────────────────────
# If an announcement matches BOTH categories → goes to BOTH sheets + BOTH channels
CROSS_POST_PAIRS = [
    ("acqmerger", "investors"),   # e.g. concall to discuss proposed acquisition
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

def classify(title, body):
    text = (title + " " + body).lower()
    return [cat for cat, rule in RULES.items()
            if any(kw in text for kw in rule["keywords"])]

def detect_cross_post(matched):
    return any(a in matched and b in matched for a, b in CROSS_POST_PAIRS)

def extract_topic(body):
    body = (body or "").strip()
    if not body or body.lower() in ("nan", "none", ""):
        return None
    return body[:200] + ("…" if len(body) > 200 else "")

def format_message(ann, matched_cats, topic):
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    link     = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    labels   = " + ".join(RULES[c]["label"] for c in matched_cats)
    emojis   = " ".join(RULES[c]["emoji"] for c in matched_cats)

    msg = (
        f"{emojis} *{labels}*\n\n"
        f"🏢 *{company}* (`{symbol}`)\n"
        f"📋 {title}\n"
    )
    if topic:
        msg += f"📝 _{topic}_\n"
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

def append_to_sheet(ws_map, sheet_name, ann, matched_cats, topic):
    ws = ws_map.get(sheet_name)
    if not ws:
        return
    company  = ann.get("sm_name") or ann.get("symbol", "Unknown")
    symbol   = ann.get("symbol", "")
    title    = ann.get("desc", "")
    date_str = ann.get("an_dt", "")
    labels   = " + ".join(RULES[c]["label"] for c in matched_cats)
    now      = datetime.now().strftime("%Y-%m-%d %H:%M")
    link     = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
    ws.append_row([now, company, symbol, labels, title, topic or "", date_str, link])

def fetch_nse():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=15)
    time.sleep(2)
    url  = "https://www.nseindia.com/api/corporate-announcements?index=equities"
    resp = session.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    result = resp.json()
    # NSE API returns either a list directly, or {"data": [...]}
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
              "Title", "Summary / Topic", "NSE Date", "Link"]

    for tab in [SHEET_RESULTS, SHEET_INVESTORS, SHEET_ACQMERGER, SHEET_DEMERGER]:
        try:
            ws = wb.worksheet(tab)
        except gspread.exceptions.WorksheetNotFound:
            ws = wb.add_worksheet(title=tab, rows=1000, cols=10)
            ws.append_row(header)
            ws.format("A1:H1", {"textFormat": {"bold": True}})
        ws_map[tab] = ws
    return ws_map

# ── main ──────────────────────────────────────────────────

def main():
    print("Fetching NSE announcements…")
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
        body  = (ann.get("attchmntText") or
                 ann.get("subject") or "")

        matched = classify(title, body)
        new_seen.add(uid)   # mark seen regardless of category match

        if not matched:
            continue

        topic    = extract_topic(body)
        is_cross = detect_cross_post(matched)
        msg      = format_message(ann, matched, topic)

        # Collect unique channels and sheets (preserve order, dedupe)
        channels_to_notify = list(dict.fromkeys(RULES[c]["channel"] for c in matched))
        sheets_to_write    = list(dict.fromkeys(RULES[c]["sheet"]   for c in matched))

        for channel_id in channels_to_notify:
            send_to_channel(channel_id, msg)

        for sheet_name in sheets_to_write:
            append_to_sheet(ws_map, sheet_name, ann, matched, topic)

        tag        = " + ".join(RULES[c]["label"] for c in matched)
        cross_note = " [CROSS-POST]" if is_cross else ""
        print(f" {cross_note}[{tag}] {ann.get('symbol','?')}")

    seen |= new_seen
    save_seen(seen)
    print(f"Done. {len(new_seen)} new IDs recorded.")

if __name__ == "__main__":
    main()
