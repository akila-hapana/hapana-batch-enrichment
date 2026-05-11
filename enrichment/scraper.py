"""
enrichment/scraper.py
Pre-tier scraping layer — runs once per company before any AI tier.
Four stages, all free. Each stage only runs if the previous returned thin content.

Stage 1 — requests + BeautifulSoup   (~1s, handles ~60% of sites)
Stage 2 — Jina Reader r.jina.ai      (~3s, handles JS-rendered SPAs)
Stage 3 — Playwright headless Chrome (~5s, handles 95% of remaining)
Stage 4 — Local Chrome via CDP       (~4s, user's real browser, virtually undetectable)
           Only runs if local machine is online (Firestore heartbeat < 2 min old).
           If offline → company queued in Firestore; local_agent.py processes on wakeup.
"""
import os
import re
import logging
import requests
from bs4 import BeautifulSoup
from enrichment import BROWSER_HEADERS

log = logging.getLogger(__name__)

FIRESTORE_PROJECT = os.environ.get("FIRESTORE_PROJECT", "verdant-wave-440404-g9")
CDP_URL           = os.environ.get("LOCAL_CDP_URL", "http://localhost:9222")
JINA_BASE         = "https://r.jina.ai/"
STATIC_TIMEOUT    = 8
JINA_TIMEOUT      = 15
PW_TIMEOUT_MS     = 20_000   # ms for Playwright page load
MIN_CHARS         = 300      # below this we try next stage
HEARTBEAT_MAX_AGE = 120      # seconds before machine considered offline

_BOT_PHRASES = [
    "attention required", "cloudflare", "you have been blocked",
    "access denied", "ddos protection", "checking your browser",
    "enable javascript and cookies", "just a moment",
]


# ── helpers ──────────────────────────────────────────────────────────────────

def _is_bot_wall(text: str) -> bool:
    t = text.lower()[:3000]
    return sum(1 for p in _BOT_PHRASES if p in t) >= 2


def _sufficient(text: str) -> bool:
    return bool(text) and len(text.strip()) >= MIN_CHARS and not _is_bot_wall(text)


def _build_url(company: dict) -> str:
    w = (company.get("website") or "").strip()
    d = (company.get("domain") or "").strip()
    if w.startswith("http"):
        return w
    if w:
        return "https://" + w
    if d:
        return "https://" + d
    return ""


def _clean_soup(soup) -> str:
    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()
    parts = []
    if soup.title and soup.title.string:
        parts.append(f"Title: {soup.title.string.strip()}")
    for m in soup.find_all("meta"):
        if m.get("name", "").lower() in ("description", "keywords"):
            parts.append(f"Meta: {m.get('content','')}")
    for h in soup.find_all(["h1", "h2", "h3"])[:8]:
        t = h.get_text(strip=True)
        if t:
            parts.append(f"H: {t}")
    nav = soup.find("nav")
    if nav:
        parts.append(f"Nav: {nav.get_text(' ', strip=True)[:300]}")
    main = soup.find("main") or soup.find(id=re.compile(r"main|content", re.I)) or soup.body
    if main:
        parts.append(f"Body: {main.get_text(' ', strip=True)[:2500]}")
    footer = soup.find("footer")
    if footer:
        parts.append(f"Footer: {footer.get_text(' ', strip=True)[:400]}")
    return "\n".join(parts)


# ── Stage 1: Static scrape ───────────────────────────────────────────────────

def _stage1(url: str) -> str:
    try:
        r = requests.get(url, timeout=STATIC_TIMEOUT, headers=BROWSER_HEADERS,
                         allow_redirects=True)
        if r.status_code >= 400 or _is_bot_wall(r.text):
            return ""
        soup = BeautifulSoup(r.text, "lxml")
        return _clean_soup(soup)
    except Exception as e:
        log.debug(f"Stage 1 failed {url}: {e}")
        return ""


# ── Stage 2: Jina Reader ─────────────────────────────────────────────────────

def _stage2_jina(url: str) -> str:
    try:
        r = requests.get(
            f"{JINA_BASE}{url}",
            timeout=JINA_TIMEOUT,
            headers={**BROWSER_HEADERS, "Accept": "text/plain"},
        )
        if r.status_code == 200:
            text = r.text.strip()
            # Jina wraps with metadata — strip it
            if "Title:" in text or len(text) > 100:
                return text[:5000]
    except Exception as e:
        log.debug(f"Stage 2 Jina failed {url}: {e}")
    return ""


# ── Stage 3: Playwright headless ─────────────────────────────────────────────

def _stage3_playwright(url: str) -> str:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            page = browser.new_page()
            page.set_extra_http_headers({
                k: v for k, v in BROWSER_HEADERS.items()
                if k not in ("Sec-Fetch-Dest", "Sec-Fetch-Mode",
                             "Sec-Fetch-Site", "Sec-Fetch-User")
            })
            try:
                page.goto(url, wait_until="networkidle", timeout=PW_TIMEOUT_MS)
            except PWTimeout:
                pass  # page may still have content
            page.wait_for_timeout(2000)
            content = page.inner_text("body")
            # Also grab a screenshot for debugging if content is thin
            screenshot = None
            if not _sufficient(content):
                screenshot = page.screenshot(type="png")
            browser.close()
            if not _is_bot_wall(content):
                return content[:5000]
    except ImportError:
        log.warning("playwright not installed — skipping Stage 3")
    except Exception as e:
        log.warning(f"Stage 3 Playwright failed {url}: {e}")
    return ""


# ── Stage 4: Local Chrome via CDP ────────────────────────────────────────────

def _local_machine_online() -> bool:
    """Check Firestore heartbeat — True if machine seen within HEARTBEAT_MAX_AGE seconds."""
    try:
        import datetime
        import google.cloud.firestore as firestore
        db = firestore.Client(project=FIRESTORE_PROJECT)
        doc = db.collection("machine_heartbeat").document("local").get()
        if not doc.exists:
            return False
        last_seen = doc.to_dict().get("last_seen")
        if not last_seen:
            return False
        now = datetime.datetime.now(datetime.timezone.utc)
        age = now.timestamp() - last_seen.timestamp()
        return age < HEARTBEAT_MAX_AGE
    except Exception as e:
        log.debug(f"Heartbeat check failed: {e}")
        return False


def _stage4_local_chrome(url: str) -> str:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            ctx = browser.contexts[0] if browser.contexts else browser.new_context()
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=PW_TIMEOUT_MS)
            except PWTimeout:
                pass
            page.wait_for_timeout(2000)
            content = page.inner_text("body")
            page.close()
            if not _is_bot_wall(content):
                return content[:5000]
    except Exception as e:
        log.warning(f"Stage 4 local Chrome failed {url}: {e}")
    return ""


def _queue_for_stage4(url: str, company: dict):
    """Write to Firestore waiting list — local_agent.py processes when machine wakes."""
    try:
        import google.cloud.firestore as firestore
        db = firestore.Client(project=FIRESTORE_PROJECT)
        db.collection("stage4_queue").add({
            "url":          url,
            "company_id":   company.get("id", ""),
            "company_name": company.get("name", ""),
            "domain":       company.get("domain", ""),
            "status":       "pending",
            "queued_at":    firestore.SERVER_TIMESTAMP,
        })
        log.info(f"Queued for Stage 4: {company.get('name')} ({url})")
    except Exception as e:
        log.warning(f"Failed to queue for Stage 4: {e}")


# ── Public API ────────────────────────────────────────────────────────────────

def scrape(company: dict) -> dict:
    """
    Scrape company website through all available stages.
    Returns:
        {
            "content": str,   # best text found (may be empty)
            "stage":   int,   # stage that produced content (0=none)
            "queued":  bool,  # True if added to Stage 4 waiting list
        }
    """
    url = _build_url(company)
    name = company.get("name", "")

    if not url:
        return {"content": "", "stage": 0, "queued": False}

    # Stage 1 — Static
    log.info(f"[Scraper S1] {name} — {url}")
    content = _stage1(url)
    if _sufficient(content):
        log.info(f"[Scraper S1 ✓] {len(content)} chars")
        return {"content": content, "stage": 1, "queued": False}

    # Stage 2 — Jina Reader
    log.info(f"[Scraper S2] Escalating to Jina Reader")
    c2 = _stage2_jina(url)
    if _sufficient(c2):
        log.info(f"[Scraper S2 ✓] {len(c2)} chars")
        return {"content": c2, "stage": 2, "queued": False}
    content = c2 if len(c2) > len(content) else content

    # Stage 3 — Playwright headless
    log.info(f"[Scraper S3] Escalating to Playwright headless")
    c3 = _stage3_playwright(url)
    if _sufficient(c3):
        log.info(f"[Scraper S3 ✓] {len(c3)} chars")
        return {"content": c3, "stage": 3, "queued": False}
    content = c3 if len(c3) > len(content) else content

    # Stage 4 — Local Chrome via CDP
    if _local_machine_online():
        log.info(f"[Scraper S4] Escalating to local Chrome")
        c4 = _stage4_local_chrome(url)
        if _sufficient(c4):
            log.info(f"[Scraper S4 ✓] {len(c4)} chars")
            return {"content": c4, "stage": 4, "queued": False}
        content = c4 if len(c4) > len(content) else content
        return {"content": content, "stage": 4, "queued": False}
    else:
        log.info(f"[Scraper S4] Local machine offline — queuing {name}")
        _queue_for_stage4(url, company)
        return {"content": content, "stage": 3, "queued": True}

    return {"content": content, "stage": 0, "queued": False}
