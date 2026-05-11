"""
Tier 2 — Low cost: Apollo enrichment + locations page scraping + Google Places + Claude Haiku.
Called when Tier 1 couldn't resolve modality, OR resolved modality but not brand_tier.
"""
import os
import re
import json
import requests
from bs4 import BeautifulSoup
from .tier1 import match_keywords, brand_tier_from_count, TIMEOUT

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", "")

VALID_MODALITIES = [
    "Boxing", "Dance", "Education", "EMS", "Golf", "Gym", "HIIT/Functional",
    "Injury Prevention", "Martial Arts", "Other", "Pilates", "Spin/Indoor Cycle",
    "Tanning", "Wellness/Recovery", "Yoga",
]

HAIKU_SYSTEM = """You are a fitness industry classifier. Given a company name and website text, return JSON with:
- "modality": one of exactly: Boxing, Dance, Education, EMS, Golf, Gym, HIIT/Functional, Injury Prevention, Martial Arts, Other, Pilates, Spin/Indoor Cycle, Tanning, Wellness/Recovery, Yoga
- "brand_tier": "SMB" (1 location), "MID" (2-10 locations), "Enterprise" (11+ locations), or "" if unknown
- "location_count": integer if determinable, else null
Only return JSON, no explanation."""


INDUSTRY_TO_MODALITY = {
    "health, wellness and fitness": "Gym",
    "sports": "Gym",
    "yoga": "Yoga",
    "pilates": "Pilates",
    "martial arts": "Martial Arts",
    "dance": "Dance",
    "physical fitness": "Gym",
    "leisure": "Gym",
    "recreational facilities": "Gym",
    "fitness technology": "Gym",
}

NAV_LOCATION_KEYWORDS = [
    "location", "locations", "studio", "studios", "gym", "gyms",
    "find us", "find a studio", "find a gym", "our clubs", "clubs",
    "branches", "centres", "centers",
]


def apollo_enrich(domain: str) -> dict:
    """Query Apollo.io for company data."""
    try:
        r = requests.post(
            "https://api.apollo.io/v1/organizations/enrich",
            json={"api_key": APOLLO_API_KEY, "domain": domain},
            timeout=10,
        )
        if r.status_code == 200:
            org = r.json().get("organization") or {}
            return {
                "industry": (org.get("industry") or "").lower(),
                "employees": org.get("estimated_num_employees"),
                "name": org.get("name"),
            }
    except Exception:
        pass
    return {}


def employees_to_brand_tier(employees: int | None) -> str | None:
    if not employees:
        return None
    if employees < 20:
        return "SMB"
    if employees < 200:
        return "MID"
    return "Enterprise"


def scrape_locations_page(base_url: str) -> int | None:
    """
    Look for a locations/studios nav link and count addresses on that page.
    Returns location count or None.
    """
    try:
        r = requests.get(base_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "lxml")

        # Find nav links matching location keywords
        location_url = None
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            if any(kw in text for kw in NAV_LOCATION_KEYWORDS):
                if href.startswith("http"):
                    location_url = href
                elif href.startswith("/"):
                    from urllib.parse import urlparse
                    parsed = urlparse(base_url)
                    location_url = f"{parsed.scheme}://{parsed.netloc}{href}"
                break

        if not location_url or location_url == base_url:
            return None

        # Scrape the locations page
        r2 = requests.get(location_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        soup2 = BeautifulSoup(r2.text, "lxml")

        # Count address blocks — look for repeated card/item structures
        address_tags = soup2.find_all(["address"]) or []
        if address_tags:
            return len(address_tags)

        # Count postcodes as proxy
        text = soup2.get_text(" ")
        postcodes = set(re.findall(r'\b[0-9]{4,5}\b', text))
        if len(postcodes) > 1:
            return len(postcodes)

        return None
    except Exception:
        return None


def google_places_search(company_name: str, api_key: str) -> dict:
    """Find Place from Text — returns business type and basic info."""
    if not api_key:
        return {}
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params={
                "input": company_name,
                "inputtype": "textquery",
                "fields": "name,types,formatted_address",
                "key": api_key,
            },
            timeout=8,
        )
        candidates = r.json().get("candidates", [])
        if candidates:
            return candidates[0]
    except Exception:
        pass
    return {}


PLACES_TYPE_MAP = {
    "gym": "Gym",
    "health": "Gym",
    "yoga_studio": "Yoga",
    "pilates": "Pilates",
    "martial_arts": "Martial Arts",
    "boxing": "Boxing",
    "dance": "Dance",
    "spa": "Wellness/Recovery",
    "golf_course": "Golf",
    "golf_club": "Golf",
    "physiotherapist": "Injury Prevention",
}


def types_to_modality(types: list) -> str | None:
    for t in (types or []):
        t_lower = t.lower()
        for key, mod in PLACES_TYPE_MAP.items():
            if key in t_lower:
                return mod
    return None


def classify_with_haiku(company_name: str, text_snippet: str) -> dict:
    """Send a small structured prompt to Claude Haiku for classification."""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 100,
                "system": HAIKU_SYSTEM,
                "messages": [{"role": "user", "content": f"Company: {company_name}\n\n{text_snippet[:1500]}"}],
            },
            timeout=15,
        )
        content = r.json().get("content", [{}])[0].get("text", "")
        # Extract JSON from response
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            result = json.loads(match.group())
            mod = result.get("modality")
            tier_val = result.get("brand_tier")
            loc = result.get("location_count")
            if mod in VALID_MODALITIES:
                return {"modality": mod, "brand_tier": tier_val or None, "location_count": loc}
    except Exception:
        pass
    return {}


def enrich(company: dict, tier1_result: dict | None = None) -> dict | None:
    """
    Returns: {modality, brand_tier, tier, method} or None to escalate.
    tier1_result may have modality already set but brand_tier missing.
    """
    name = company.get("name", "")
    domain = company.get("domain", "") or ""
    website = company.get("website", "") or ""
    url = website if website.startswith("http") else (f"https://{website}" if website else f"https://{domain}" if domain else None)

    modality = tier1_result.get("modality") if tier1_result else None
    brand_tier = None

    # 1. Apollo enrichment (free to query with our key)
    apollo = {}
    if domain:
        apollo = apollo_enrich(domain)
        if not modality and apollo.get("industry"):
            for industry_key, mod in INDUSTRY_TO_MODALITY.items():
                if industry_key in apollo["industry"]:
                    modality = mod
                    break
        if not brand_tier:
            brand_tier = employees_to_brand_tier(apollo.get("employees"))

    # 2. Locations page scrape
    if url and not brand_tier:
        loc_count = scrape_locations_page(url)
        brand_tier = brand_tier_from_count(loc_count)

    # 3. Google Places (only if API key set)
    if GOOGLE_PLACES_API_KEY and not modality:
        places = google_places_search(name, GOOGLE_PLACES_API_KEY)
        if not modality:
            modality = types_to_modality(places.get("types", []))

    # 4. Claude Haiku with structured text (only if still missing something)
    if url and (not modality or not brand_tier):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text[:20000], "lxml")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            # Extract: title + meta + h1/h2 + nav text + footer snippet
            parts = []
            if soup.title:
                parts.append(f"Title: {soup.title.string}")
            for meta in soup.find_all("meta", attrs={"name": re.compile(r"description|keywords", re.I)}):
                parts.append(f"Meta: {meta.get('content','')}")
            for h in soup.find_all(["h1", "h2"])[:6]:
                parts.append(f"Heading: {h.get_text(strip=True)}")
            nav = soup.find("nav")
            if nav:
                parts.append(f"Nav: {nav.get_text(' ', strip=True)[:300]}")
            footer = soup.find("footer")
            if footer:
                parts.append(f"Footer: {footer.get_text(' ', strip=True)[:500]}")
            structured = "\n".join(parts)

            haiku_result = classify_with_haiku(name, structured)
            if not modality and haiku_result.get("modality"):
                modality = haiku_result["modality"]
            if not brand_tier and haiku_result.get("brand_tier"):
                brand_tier = haiku_result["brand_tier"]
            if not brand_tier and haiku_result.get("location_count"):
                brand_tier = brand_tier_from_count(haiku_result["location_count"])
        except Exception:
            pass

    if modality:
        return {
            "modality": modality,
            "brand_tier": brand_tier,
            "tier": 2,
            "method": f"apollo+scrape" + ("+haiku" if not brand_tier else ""),
            "apollo": bool(apollo),
        }

    return None
