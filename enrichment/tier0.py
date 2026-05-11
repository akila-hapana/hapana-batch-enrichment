"""
Tier 0 — Data Collection (free except Google Maps ~$0.049/call).
Runs before any AI tier. T1/T2/T3 only read from the context this returns.

Steps:
  1. Domain validation + DuckDuckGo correction
  2. 4-stage website scraping  (static → Jina → Playwright → local Chrome)
  3. Location extraction       (follow /locations nav link, postcode/address count)
  4. Google Maps Places API    (~$0.049/company, billed to GCP account)
  5. Apollo                    (industry + employee count, free tier)

Returns a context dict. _skip=True means stop — classify as Other/blank, no AI spend.
"""
import os
import re
import logging
import requests
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
from urllib.parse import urlparse
from enrichment import BROWSER_HEADERS

log = logging.getLogger(__name__)

FIRESTORE_PROJECT    = os.environ.get("FIRESTORE_PROJECT", "verdant-wave-440404-g9")
GOOGLE_MAPS_KEY      = os.environ.get("GOOGLE_PLACES_API_KEY", "")
APOLLO_KEY           = os.environ.get("APOLLO_API_KEY", "")
CDP_URL              = os.environ.get("LOCAL_CDP_URL", "http://localhost:9222")
JINA_BASE            = "https://r.jina.ai/"

REACH_TIMEOUT        = 5
SEARCH_TIMEOUT       = 6
STATIC_TIMEOUT       = 8
JINA_TIMEOUT         = 15
PW_TIMEOUT_MS        = 20_000
MIN_CHARS            = 300
HEARTBEAT_MAX_AGE    = 120      # seconds
SIMILARITY_THRESHOLD = 0.40
_MAPS_COST           = 0.049    # Find Place + Text Search

NAV_LOCATION_HINTS = [
    "location", "locations", "studio", "studios", "find us",
    "our clubs", "our gyms", "find a gym", "find a studio", "branches",
]

_BOT_PHRASES = [
    "attention required", "cloudflare", "you have been blocked",
    "access denied", "enable javascript", "ddos protection",
    "checking your browser", "please wait", "just a moment",
]


# ── Shared helpers ────────────────────────────────────────────────────────────

def _is_bot_wall(text: str) -> bool:
    t = text.lower()[:3000]
    return sum(1 for p in _BOT_PHRASES if p in t) >= 2


def _is_binary(text: str) -> bool:
    """Reject binary/corrupted responses — non-printable char ratio > 15%."""
    if not text:
        return False
    sample = text[:2000]
    non_printable = sum(1 for c in sample if ord(c) < 32 and c not in "\t\n\r")
    return (non_printable / len(sample)) > 0.15


def _sufficient(text: str) -> bool:
    return (bool(text) and len(text.strip()) >= MIN_CHARS
            and not _is_bot_wall(text) and not _is_binary(text))


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


def _name_similarity(a: str, b: str) -> float:
    a = re.sub(r"[^\w\s]", "", a.lower().strip())
    b = re.sub(r"[^\w\s]", "", b.lower().strip())
    return SequenceMatcher(None, a, b).ratio()


def _clean_soup(soup) -> str:
    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()
    parts = []
    if soup.title and soup.title.string:
        parts.append(f"Title: {soup.title.string.strip()}")
    for m in soup.find_all("meta"):
        if m.get("name", "").lower() in ("description", "keywords"):
            parts.append(f"Meta: {m.get('content', '')}")
    for h in soup.find_all(["h1", "h2", "h3"])[:8]:
        t = h.get_text(strip=True)
        if t:
            parts.append(f"H: {t}")
    nav = soup.find("nav")
    if nav:
        parts.append(f"Nav: {nav.get_text(' ', strip=True)[:300]}")
    main = (soup.find("main")
            or soup.find(id=re.compile(r"main|content", re.I))
            or soup.body)
    if main:
        parts.append(f"Body: {main.get_text(' ', strip=True)[:2500]}")
    footer = soup.find("footer")
    if footer:
        parts.append(f"Footer: {footer.get_text(' ', strip=True)[:400]}")
    return "\n".join(parts)


# ── 1. Domain validation ──────────────────────────────────────────────────────

def _is_reachable(url: str) -> tuple[bool, bool]:
    """Returns (reachable, bot_blocked). 403 = real site, just blocking bots."""
    try:
        r = requests.head(url, timeout=REACH_TIMEOUT, allow_redirects=True,
                          headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code < 400 or r.status_code == 403:
            return True, r.status_code == 403
        return False, False
    except Exception:
        try:
            r = requests.get(url, timeout=REACH_TIMEOUT, allow_redirects=True,
                             headers={"User-Agent": "Mozilla/5.0"}, stream=True)
            chunk = next(r.iter_content(4096), b"").decode("utf-8", errors="ignore")
            r.close()
            blocked = _is_bot_wall(chunk) or r.status_code == 403
            ok = r.status_code < 400 or blocked
            return ok, blocked
        except Exception:
            return False, False


def _ddg_search(name: str) -> tuple[str | None, str | None]:
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": name, "format": "json", "no_html": 1, "skip_disambig": 1},
            timeout=SEARCH_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        data = r.json()
        if data.get("AbstractURL"):
            parsed = urlparse(data["AbstractURL"])
            return parsed.netloc.replace("www.", ""), data.get("Heading", "")
        for topic in data.get("RelatedTopics", [])[:2]:
            if isinstance(topic, dict) and topic.get("FirstURL"):
                parsed = urlparse(topic["FirstURL"])
                return parsed.netloc.replace("www.", ""), topic.get("Text", "")
    except Exception:
        pass
    return None, None


# ── 2. 4-Stage scraping ───────────────────────────────────────────────────────

def _stage1(url: str) -> tuple[str, object]:
    """Returns (text, soup). soup used downstream for nav-link extraction."""
    try:
        r = requests.get(url, timeout=STATIC_TIMEOUT, headers=BROWSER_HEADERS,
                         allow_redirects=True)
        if r.status_code >= 400 or _is_bot_wall(r.text):
            return "", None
        soup = BeautifulSoup(r.text, "lxml")
        return _clean_soup(soup), soup
    except Exception as e:
        log.debug(f"Stage 1 failed {url}: {e}")
        return "", None


def _stage2_jina(url: str) -> str:
    try:
        r = requests.get(
            f"{JINA_BASE}{url}",
            timeout=JINA_TIMEOUT,
            headers={**BROWSER_HEADERS, "Accept": "text/plain"},
        )
        if r.status_code == 200:
            text = r.text.strip()
            if "Title:" in text or len(text) > 100:
                return text[:5000]
    except Exception as e:
        log.debug(f"Stage 2 Jina failed {url}: {e}")
    return ""


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
                pass
            page.wait_for_timeout(2000)
            content = page.inner_text("body")
            browser.close()
            if not _is_bot_wall(content):
                return content[:5000]
    except ImportError:
        log.warning("playwright not installed — skipping Stage 3")
    except Exception as e:
        log.warning(f"Stage 3 Playwright failed {url}: {e}")
    return ""


def _local_machine_online() -> bool:
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
        return (now.timestamp() - last_seen.timestamp()) < HEARTBEAT_MAX_AGE
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
    except Exception as e:
        log.warning(f"Failed to queue for Stage 4: {e}")


def _run_scraper(url: str, company: dict) -> tuple[str, int, bool, object]:
    """
    Returns (content, stage, queued, soup).
    soup is from Stage 1 (may be None) — used for location nav-link extraction
    regardless of which stage produced the final content.
    """
    name = company.get("name", "")

    s1_content, soup = _stage1(url)
    if _sufficient(s1_content):
        log.info(f"[S1 ✓] {name} — {len(s1_content)} chars")
        return s1_content, 1, False, soup

    c2 = _stage2_jina(url)
    if _sufficient(c2):
        log.info(f"[S2 ✓] {name} — {len(c2)} chars")
        return c2, 2, False, soup
    best = c2 if len(c2) > len(s1_content) else s1_content

    c3 = _stage3_playwright(url)
    if _sufficient(c3):
        log.info(f"[S3 ✓] {name} — {len(c3)} chars")
        return c3, 3, False, soup
    best = c3 if len(c3) > len(best) else best

    if _local_machine_online():
        c4 = _stage4_local_chrome(url)
        if _sufficient(c4):
            log.info(f"[S4 ✓] {name} — {len(c4)} chars")
            return c4, 4, False, soup
        best = c4 if len(c4) > len(best) else best
        return best, 4, False, soup
    else:
        log.info(f"[S4] Local machine offline — queuing {name}")
        _queue_for_stage4(url, company)
        return best, 3, True, soup


# ── 3. Location extraction ────────────────────────────────────────────────────

def _count_in_text(text: str) -> int:
    postcodes = set(re.findall(r'\b\d{4,5}\b', text))
    streets = set(re.findall(
        r'\b\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s+'
        r'(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Way|Blvd|Lane)\b',
        text,
    ))
    return max(len(postcodes), len(streets))


def _extract_locations(url: str, scraped_text: str, soup) -> tuple[int | None, str]:
    """
    Returns (location_count, locations_snippet).

    Priority:
    1. Follow /locations nav link from Stage 1 soup → most accurate
    2. Count postcodes/addresses in main scraped_text → fallback
    """
    locations_snippet = ""

    # Strategy 1: /locations nav link
    if soup is not None:
        try:
            parsed = urlparse(url)
            for a in soup.find_all("a", href=True):
                txt  = a.get_text(strip=True).lower()
                href = a["href"].lower()
                if any(k in txt or k in href for k in NAV_LOCATION_HINTS):
                    raw_href = a["href"]
                    loc_url = (raw_href if raw_href.startswith("http")
                               else f"{parsed.scheme}://{parsed.netloc}{raw_href}")
                    if loc_url == url:
                        continue
                    try:
                        r2 = requests.get(loc_url, timeout=STATIC_TIMEOUT,
                                          headers=BROWSER_HEADERS)
                        s2 = BeautifulSoup(r2.text, "lxml")
                        for t in s2(["script", "style"]):
                            t.decompose()
                        loc_text = s2.get_text(" ", strip=True)
                        locations_snippet = loc_text[:800]
                        count = _count_in_text(loc_text)
                        if count > 0:
                            log.info(f"[T0 Locations] /locations page → {count} locations")
                            return count, locations_snippet
                    except Exception:
                        pass
                    break
        except Exception:
            pass

    # Strategy 2: count in main scraped text
    if scraped_text:
        count = _count_in_text(scraped_text)
        if count > 0:
            log.info(f"[T0 Locations] text count → {count} locations")
            return count, locations_snippet

    return None, locations_snippet


# ── 4. Google Maps ────────────────────────────────────────────────────────────

def _google_maps_count(name: str) -> int | None:
    if not GOOGLE_MAPS_KEY or not name:
        return None
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params={"input": name, "inputtype": "textquery",
                    "fields": "name,place_id", "key": GOOGLE_MAPS_KEY},
            timeout=8,
        )
        if not r.json().get("candidates"):
            return None
        r2 = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": name, "key": GOOGLE_MAPS_KEY},
            timeout=8,
        )
        results = r2.json().get("results", [])
        return len(results) if results else None
    except Exception:
        return None


# ── 5. Apollo ─────────────────────────────────────────────────────────────────

def _apollo_enrich(domain: str) -> dict:
    if not APOLLO_KEY or not domain:
        return {}
    try:
        r = requests.post(
            "https://api.apollo.io/v1/organizations/enrich",
            json={"api_key": APOLLO_KEY, "domain": domain},
            timeout=10,
        )
        if r.status_code == 200:
            org = r.json().get("organization") or {}
            return {
                "apollo_industry":  (org.get("industry") or "").lower(),
                "apollo_employees": org.get("estimated_num_employees"),
            }
    except Exception:
        pass
    return {}


# ── Public API ────────────────────────────────────────────────────────────────

def collect(company: dict) -> dict:
    """
    Full data collection for one company. T1/T2/T3 read only from this output.

    Returns dict with:
        _skip, skip_reason          — if pipeline should stop
        scraped_text, scrape_stage, scrape_queued
        location_count              — website-based location estimate
        maps_count                  — Google Maps listing count
        locations_snippet           — /locations page text for AI context
        apollo_industry, apollo_employees
        cost_usd                    — Maps API cost
    """
    name   = (company.get("name") or "").strip()
    domain = (company.get("domain") or "").lower().replace("www.", "")
    url    = _build_url(company)

    ctx = {
        **company,
        "scraped_text":      "",
        "scrape_stage":      0,
        "scrape_queued":     False,
        "location_count":    None,
        "maps_count":        None,
        "locations_snippet": "",
        "apollo_industry":   "",
        "apollo_employees":  None,
        "cost_usd":          0.0,
    }

    # ── 1. Domain validation ──────────────────────────────────────────────────
    if not url and not name:
        ctx["_skip"] = True
        ctx["skip_reason"] = "no_domain_or_website_or_name"
        return ctx

    if url:
        reachable, bot_blocked = _is_reachable(url)
        if not reachable:
            found_domain, found_title = _ddg_search(name)
            if found_domain:
                domain_stem = re.sub(r"\.\w{2,4}$", "", found_domain)
                sim = max(
                    _name_similarity(name, domain_stem),
                    _name_similarity(name, found_title) if found_title else 0.0,
                )
                if sim >= SIMILARITY_THRESHOLD:
                    ctx["domain"]            = found_domain
                    ctx["website"]           = f"https://{found_domain}"
                    ctx["_domain_corrected"] = True
                    ctx["_original_domain"]  = domain
                    url = f"https://{found_domain}"
                    log.info(f"[T0] Domain corrected: {domain} → {found_domain}")
                else:
                    log.info(f"[T0] {domain} unreachable, search found {found_domain} "
                             f"(sim={sim:.2f} < threshold) — name-only mode")
                    url = None
            else:
                log.info(f"[T0] {domain} unreachable, no DDG result — name-only mode")
                url = None
        if bot_blocked:
            ctx["_bot_blocked"] = True

    # ── 2. 4-Stage scraping ───────────────────────────────────────────────────
    if url:
        scraped_text, scrape_stage, scrape_queued, soup = _run_scraper(url, company)
        ctx["scraped_text"]  = scraped_text
        ctx["scrape_stage"]  = scrape_stage
        ctx["scrape_queued"] = scrape_queued
    else:
        soup = None

    # ── 3. Location extraction ────────────────────────────────────────────────
    if url:
        location_count, locations_snippet = _extract_locations(
            url, ctx["scraped_text"], soup)
        ctx["location_count"]    = location_count
        ctx["locations_snippet"] = locations_snippet

    # ── 4. Google Maps ────────────────────────────────────────────────────────
    maps_count = _google_maps_count(name)
    if maps_count is not None:
        ctx["maps_count"] = maps_count
        ctx["cost_usd"]  += _MAPS_COST
        # Reliability check: high Maps count with no website location evidence
        # likely means partner/class listings (e.g. Ujam, Zumba), not owned locations.
        # Flag this so T2/T3 treat the count with appropriate scepticism.
        if maps_count >= 11 and (location_count is None or location_count < 3) and not locations_snippet:
            ctx["maps_count_reliable"] = False
            log.info(f"[T0] Maps: {maps_count} listings for '{name}' — flagged unreliable "
                     f"(no website location corroboration)")
        else:
            ctx["maps_count_reliable"] = True
            log.info(f"[T0] Maps: {maps_count} listings for '{name}'")

    # ── 5. Apollo ─────────────────────────────────────────────────────────────
    apollo = _apollo_enrich(domain)
    ctx.update(apollo)

    log.info(f"[T0 ✓] {name} — stage={scrape_stage} "
             f"loc={location_count} maps={maps_count} "
             f"apollo={ctx.get('apollo_industry','—')}")
    return ctx
