"""
local_agent.py — Hapana Local Enrichment Agent
Runs on your Mac at all times via LaunchAgent.

Responsibilities:
  1. Sends a heartbeat to Firestore every 30s so Cloud Run knows your machine is online.
  2. Watches Firestore stage4_queue for companies that couldn't be scraped by Cloud Run.
  3. Scrapes them using your local Chrome (Stage 4) and writes results back to Firestore.
  4. Cloud Run picks up results and completes enrichment.

Setup:
  1. Install LaunchAgent (one-time):
       cp agents/com.hapana.enrichment.plist ~/Library/LaunchAgents/
       launchctl load ~/Library/LaunchAgents/com.hapana.enrichment.plist

  2. Make sure Chrome is running with remote debugging:
       /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \\
         --remote-debugging-port=9222 --no-first-run &

  Or just run manually for testing:
       python local_agent.py
"""
import os
import sys
import time
import logging
import threading
import datetime

# Load .env if present
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

FIRESTORE_PROJECT  = os.environ.get("FIRESTORE_PROJECT", "verdant-wave-440404-g9")
CDP_URL            = os.environ.get("LOCAL_CDP_URL", "http://localhost:9222")
HEARTBEAT_INTERVAL = 30    # seconds between heartbeats
QUEUE_POLL_INTERVAL = 10   # seconds between queue checks
BATCH_SIZE         = 5     # companies processed per queue poll

_BOT_PHRASES = [
    "attention required", "cloudflare", "you have been blocked",
    "access denied", "ddos protection", "checking your browser",
]


def _is_bot_wall(text: str) -> bool:
    t = text.lower()[:3000]
    return sum(1 for p in _BOT_PHRASES if p in t) >= 2


def get_db():
    import google.cloud.firestore as firestore
    return firestore.Client(project=FIRESTORE_PROJECT)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

def send_heartbeat(db):
    import google.cloud.firestore as firestore
    db.collection("machine_heartbeat").document("local").set({
        "online":    True,
        "last_seen": firestore.SERVER_TIMESTAMP,
        "hostname":  os.uname().nodename,
    })
    log.info("♥ Heartbeat sent")


def mark_offline(db):
    import google.cloud.firestore as firestore
    try:
        db.collection("machine_heartbeat").document("local").set({
            "online":    False,
            "last_seen": firestore.SERVER_TIMESTAMP,
        })
        log.info("Marked offline in Firestore")
    except Exception:
        pass


def heartbeat_loop(db):
    while True:
        try:
            send_heartbeat(db)
        except Exception as e:
            log.error(f"Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


# ── Stage 4 scraping with local Chrome ───────────────────────────────────────

def _scrape_local_chrome(url: str) -> str:
    """Connect to already-running Chrome via CDP and scrape the page."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            ctx = browser.contexts[0] if browser.contexts else browser.new_context()
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=25_000)
            except PWTimeout:
                pass
            page.wait_for_timeout(2000)
            content = page.inner_text("body")
            page.close()
            if content and not _is_bot_wall(content):
                return content[:5000]
    except Exception as e:
        log.warning(f"Local Chrome scrape failed for {url}: {e}")
    return ""


# ── Queue processor ───────────────────────────────────────────────────────────

def process_queue(db):
    import google.cloud.firestore as firestore

    pending = (
        db.collection("stage4_queue")
        .where("status", "==", "pending")
        .limit(BATCH_SIZE)
        .stream()
    )

    processed = 0
    for doc in pending:
        data    = doc.to_dict()
        url     = data.get("url", "")
        name    = data.get("company_name", "—")
        cid     = data.get("company_id", "")

        if not url:
            doc.reference.update({"status": "skipped"})
            continue

        log.info(f"→ Processing queue item: {name} ({url})")
        doc.reference.update({"status": "processing"})

        content = _scrape_local_chrome(url)
        status  = "done" if content else "failed"

        doc.reference.update({
            "status":       status,
            "content":      content,
            "content_len":  len(content),
            "processed_at": firestore.SERVER_TIMESTAMP,
        })

        if content:
            log.info(f"✓ {name} — {len(content)} chars scraped, written back")
        else:
            log.warning(f"✗ {name} — Stage 4 also returned no content")

        processed += 1

    if processed:
        log.info(f"Queue batch done — {processed} item(s) processed")


def queue_loop(db):
    while True:
        try:
            process_queue(db)
        except Exception as e:
            log.error(f"Queue loop error: {e}")
        time.sleep(QUEUE_POLL_INTERVAL)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("Hapana Local Enrichment Agent starting")
    log.info(f"Firestore project : {FIRESTORE_PROJECT}")
    log.info(f"Chrome CDP URL    : {CDP_URL}")
    log.info(f"Heartbeat every   : {HEARTBEAT_INTERVAL}s")
    log.info(f"Queue poll every  : {QUEUE_POLL_INTERVAL}s")
    log.info("=" * 60)

    db = get_db()

    # Immediate heartbeat on startup
    try:
        send_heartbeat(db)
    except Exception as e:
        log.error(f"Initial heartbeat failed — check GCP credentials: {e}")
        sys.exit(1)

    # Run both loops in background threads
    t_hb = threading.Thread(target=heartbeat_loop, args=(db,), daemon=True, name="heartbeat")
    t_q  = threading.Thread(target=queue_loop,     args=(db,), daemon=True, name="queue")
    t_hb.start()
    t_q.start()

    log.info("Agent running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        log.info("Shutting down — marking offline...")
        mark_offline(db)
        log.info("Done.")
