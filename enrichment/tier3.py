"""
Tier 3 — Claude Haiku, deep fallback (~$0.001/call).
Last resort. Full page content + explicit confidence scoring.
If BOTH < 90% after Tier 3 → modality = "Other", brand_tier = "" (blank).
"""
import os
import re
import json
import requests
from bs4 import BeautifulSoup
from .tier1 import TIMEOUT, _url
from .tier2 import VALID_MODALITIES

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Claude Haiku 4.5 pricing (per token)
_HAIKU_IN  = 0.80 / 1_000_000
_HAIKU_OUT = 4.00 / 1_000_000

HAIKU_SYSTEM = """You are a fitness industry analyst. Classify a company based on website content.

Return ONLY valid JSON:
{
  "modality": "<one of: Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null>,
  "reasoning": "<one sentence>"
}

Rules:
- SMB = 1 location, MID = 2-10, Enterprise = 11+
- brand_tier blank if you truly cannot determine location count
- Be honest about confidence — only score 90+ when genuinely certain"""


def _deep_scrape(url: str) -> str:
    """Full page scrape — body content + about page if found."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"},
                         allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        parts = []
        if soup.title:
            parts.append(f"Title: {soup.title.string.strip()}")
        for meta in soup.find_all("meta"):
            if meta.get("name", "").lower() in ("description", "keywords"):
                parts.append(f"Meta: {meta.get('content','')}")
        for h in soup.find_all(["h1","h2","h3"])[:10]:
            t = h.get_text(strip=True)
            if t:
                parts.append(f"H: {t}")
        nav = soup.find("nav")
        if nav:
            parts.append(f"Nav: {nav.get_text(' ', strip=True)[:400]}")
        main = soup.find("main") or soup.find(id=re.compile(r"main|content", re.I)) or soup.body
        if main:
            parts.append(f"Content: {main.get_text(' ', strip=True)[:2500]}")
        footer = soup.find("footer")
        if footer:
            parts.append(f"Footer: {footer.get_text(' ', strip=True)[:600]}")

        # Follow about/services page for more context
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(k in href for k in ["/about", "/services", "/what-we-do", "/our-story"]):
                try:
                    from urllib.parse import urlparse
                    p = urlparse(url)
                    about_url = a["href"] if a["href"].startswith("http") else f"{p.scheme}://{p.netloc}{a['href']}"
                    r2 = requests.get(about_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
                    s2 = BeautifulSoup(r2.text, "lxml")
                    for t in s2(["script","style"]):
                        t.decompose()
                    parts.append(f"About page: {s2.get_text(' ', strip=True)[:800]}")
                except Exception:
                    pass
                break

        return "\n".join(parts)[:5000]
    except Exception:
        return ""


def _call_haiku(name: str, content: str) -> dict:
    if not ANTHROPIC_KEY:
        return {}
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "system": HAIKU_SYSTEM,
                "messages": [{"role": "user", "content": f"Company: {name}\n\n{content}"}],
            },
            timeout=20,
        )
        resp = r.json()
        text = resp.get("content", [{}])[0].get("text", "")
        usage = resp.get("usage", {})
        cost = (usage.get("input_tokens", 0) * _HAIKU_IN +
                usage.get("output_tokens", 0) * _HAIKU_OUT)
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            result = json.loads(m.group())
            mod = result.get("modality", "")
            tier_val = result.get("brand_tier", "")
            return {
                "modality": mod if mod in VALID_MODALITIES else "",
                "modality_confidence": int(result.get("modality_confidence", 0)),
                "brand_tier": tier_val if tier_val in ("SMB", "MID", "Enterprise", "") else "",
                "brand_tier_confidence": int(result.get("brand_tier_confidence", 0)),
                "location_count": result.get("location_count"),
                "_cost": cost,
            }
    except Exception:
        pass
    return {}


def enrich(company: dict, previous: dict | None = None) -> dict:
    """
    Always returns a final result.
    If BOTH confidences < 90 → modality = "Other", brand_tier = "" (blank).
    """
    name = (company.get("name") or "").strip()
    url = _url(company)

    content = _deep_scrape(url) if url else ""

    result = _call_haiku(name, content) if content or name else {}

    mod_conf = result.get("modality_confidence", 0)
    tier_conf = result.get("brand_tier_confidence", 0)
    modality = result.get("modality", "")
    brand_tier = result.get("brand_tier", "")
    cost = result.get("_cost", 0.0)

    # If previous partial result had higher confidence on one property, prefer it
    if previous:
        if previous.get("modality_confidence", 0) > mod_conf and previous.get("modality"):
            modality = previous["modality"]
            mod_conf = previous["modality_confidence"]
        if previous.get("brand_tier_confidence", 0) > tier_conf and previous.get("brand_tier"):
            brand_tier = previous["brand_tier"]
            tier_conf = previous["brand_tier_confidence"]

    # Final gate — if either < 90, fall back to defaults
    if mod_conf < 90:
        modality = "Other"
        mod_conf = 0
    if tier_conf < 90:
        brand_tier = ""
        tier_conf = 0

    return {
        "modality": modality,
        "brand_tier": brand_tier,
        "modality_confidence": mod_conf,
        "brand_tier_confidence": tier_conf,
        "cost_usd": cost,
        "tier": 3,
        "method": "haiku_deep",
    }
