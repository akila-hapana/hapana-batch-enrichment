"""
Tier 2 — Gemini 1.5 Flash via Vertex AI (~$0.0008/call).
Called when Tier 1 couldn't reach 90% confidence on both properties.
Returns result only if BOTH modality_confidence >= 90 AND brand_tier_confidence >= 90.
"""
import os
import re
import json
import requests
from bs4 import BeautifulSoup
from .tier1 import (
    brand_tier_from_count, NAV_LOCATION_HINTS, TIMEOUT,
    _url, _match_strong, _match_signal
)

GCP_PROJECT        = os.environ.get("VERTEX_PROJECT", "hapana-internal-platform")
GCP_LOCATION       = "us-central1"
GEMINI_MODEL       = "gemini-1.5-flash"
APOLLO_KEY         = os.environ.get("APOLLO_API_KEY", "")
SA_KEY_JSON        = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")
GOOGLE_MAPS_KEY    = os.environ.get("GOOGLE_PLACES_API_KEY", "")

VALID_MODALITIES = [
    "Boxing", "Dance", "Education", "EMS", "Golf", "Gym", "HIIT/Functional",
    "Injury Prevention", "Martial Arts", "Other", "Pilates", "Spin/Indoor Cycle",
    "Tanning", "Wellness/Recovery", "Yoga",
]

GEMINI_PROMPT = """You are a fitness industry analyst. Classify this company.

Return ONLY valid JSON — no markdown, no explanation:
{{
  "modality": "<one of: Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null>,
  "reasoning": "<one sentence>"
}}

Rules:
- SMB = 1 location, MID = 2-10 locations, Enterprise = 11+ locations
- brand_tier blank ("") if you cannot determine location count
- modality_confidence reflects how certain you are this is the right fitness category
- brand_tier_confidence reflects how certain you are about the number of locations

Company: {name}
{context}"""


def _get_vertex_token() -> str | None:
    """Get a short-lived access token from the GCP service account key."""
    if not SA_KEY_JSON:
        return None
    try:
        import google.oauth2.service_account as sa
        from google.auth.transport.requests import Request
        key_data = json.loads(SA_KEY_JSON)
        credentials = sa.Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        credentials.refresh(Request())
        return credentials.token
    except Exception:
        return None


def _call_gemini(name: str, context: str) -> dict:
    """Call Gemini 1.5 Flash via Vertex AI REST API."""
    token = _get_vertex_token()
    if not token:
        return {}
    prompt = GEMINI_PROMPT.format(name=name, context=context)
    url = (f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1/projects/{GCP_PROJECT}"
           f"/locations/{GCP_LOCATION}/publishers/google/models/{GEMINI_MODEL}:generateContent")
    try:
        r = requests.post(url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 250, "temperature": 0},
            },
            timeout=20,
        )
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
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
                "reasoning": result.get("reasoning", ""),
            }
    except Exception:
        pass
    return {}


def _scrape_structured(url: str) -> str:
    """Extract structured text from a website: title + meta + headings + nav + footer."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"},
                         allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        parts = []
        if soup.title:
            parts.append(f"Title: {soup.title.string.strip()}")
        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            if name in ("description", "keywords"):
                parts.append(f"Meta {name}: {meta.get('content','')}")
        for h in soup.find_all(["h1", "h2", "h3"])[:8]:
            t = h.get_text(strip=True)
            if t:
                parts.append(f"Heading: {t}")

        # Nav
        nav = soup.find("nav")
        if nav:
            parts.append(f"Navigation: {nav.get_text(' ', strip=True)[:300]}")

        # Footer
        footer = soup.find("footer")
        if footer:
            parts.append(f"Footer: {footer.get_text(' ', strip=True)[:600]}")

        # Follow locations page
        loc_url = None
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            href = a["href"]
            if any(k in txt for k in NAV_LOCATION_HINTS):
                from urllib.parse import urlparse
                parsed = urlparse(url)
                loc_url = href if href.startswith("http") else f"{parsed.scheme}://{parsed.netloc}{href}"
                break

        if loc_url and loc_url != url:
            try:
                r2 = requests.get(loc_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
                s2 = BeautifulSoup(r2.text, "lxml")
                for tag in s2(["script","style"]):
                    tag.decompose()
                loc_text = s2.get_text(" ", strip=True)[:800]
                # Count postcodes as location proxy
                postcodes = set(re.findall(r'\b\d{4,5}\b', loc_text))
                parts.append(f"Locations page ({len(postcodes)} postcodes found): {loc_text[:500]}")
            except Exception:
                pass

        return "\n".join(parts)[:3000]
    except Exception:
        return ""


def _apollo_enrich(domain: str) -> dict:
    if not APOLLO_KEY or not domain:
        return {}
    try:
        r = requests.post("https://api.apollo.io/v1/organizations/enrich",
            json={"api_key": APOLLO_KEY, "domain": domain}, timeout=10)
        if r.status_code == 200:
            org = r.json().get("organization") or {}
            return {
                "industry": (org.get("industry") or "").lower(),
                "employees": org.get("estimated_num_employees"),
            }
    except Exception:
        pass
    return {}


def _google_maps_location_count(company_name: str, domain: str) -> int | None:
    """
    Search Google Maps Places API for the brand — use result count as location proxy.
    Free tier: $200/month credit (~11,700 Find Place calls free).
    Returns approximate location count, or None if API key not set.
    """
    if not GOOGLE_MAPS_KEY:
        return None
    try:
        # Find Place to confirm it's the right business
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params={
                "input": company_name,
                "inputtype": "textquery",
                "fields": "name,place_id,types,formatted_address",
                "key": GOOGLE_MAPS_KEY,
            },
            timeout=8,
        )
        candidates = r.json().get("candidates", [])
        if not candidates:
            return None

        # Text Search to find all locations of this brand
        r2 = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": company_name, "key": GOOGLE_MAPS_KEY},
            timeout=8,
        )
        results = r2.json().get("results", [])
        return len(results) if results else None
    except Exception:
        return None


def enrich(company: dict, tier1_result: dict | None = None) -> dict | None:
    """
    Returns result if BOTH modality_confidence >= 90 AND brand_tier_confidence >= 90.
    Otherwise returns None to escalate to Tier 3.
    """
    name = (company.get("name") or "").strip()
    domain = (company.get("domain") or "").replace("www.", "")
    url = _url(company)

    # Build context string for Gemini
    parts = []

    # Apollo
    apollo = _apollo_enrich(domain)
    if apollo.get("industry"):
        parts.append(f"Industry (Apollo): {apollo['industry']}")
    if apollo.get("employees"):
        employees = apollo["employees"]
        parts.append(f"Employees (Apollo): ~{employees}")

    # Google Maps location count
    maps_count = _google_maps_location_count(name, domain)
    if maps_count is not None:
        parts.append(f"Google Maps search results for '{name}': {maps_count} listings found")
        if maps_count >= 11:
            parts.append("(Google Maps: 11+ locations — likely Enterprise)")
        elif maps_count >= 2:
            parts.append(f"(Google Maps: {maps_count} locations — likely MID)")
        else:
            parts.append("(Google Maps: 1 location — likely SMB)")

    # Website structured scrape
    if url:
        structured = _scrape_structured(url)
        if structured:
            parts.append(structured)

    if not parts:
        return None  # No data to work with

    context = "\n".join(parts)

    # Call Gemini
    result = _call_gemini(name, context)

    if not result:
        return None

    mod_conf = result.get("modality_confidence", 0)
    tier_conf = result.get("brand_tier_confidence", 0)

    # Pass if both >= 90
    if mod_conf >= 90 and tier_conf >= 90 and result.get("modality"):
        return {
            "modality": result["modality"],
            "brand_tier": result.get("brand_tier", ""),
            "modality_confidence": mod_conf,
            "brand_tier_confidence": tier_conf,
            "location_count": result.get("location_count"),
            "reasoning": result.get("reasoning", ""),
            "tier": 2,
            "method": "gemini_flash",
        }

    # Return partial for Tier 3 to build on
    return {
        "_partial": True,
        "modality": result.get("modality", ""),
        "brand_tier": result.get("brand_tier", ""),
        "modality_confidence": mod_conf,
        "brand_tier_confidence": tier_conf,
        "location_count": result.get("location_count"),
    }
