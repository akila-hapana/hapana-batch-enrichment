"""
Tier 1 — Free enrichment: keyword matching + website <head> scrape + footer address count.
No AI calls. No paid APIs. Target: ~50-60% resolution rate.
"""
import re
import requests
from bs4 import BeautifulSoup

TIMEOUT = 8

MODALITY_KEYWORDS = {
    "Boxing": ["boxing gym", "boxing club", "box gym", " boxing"],
    "Dance": ["dance studio", "dancing", "ballet", "zumba", "barre studio", "barre class"],
    "Education": ["education", "training academy", "coaching school"],
    "EMS": ["ems training", "electro muscle", "miha bodytec", "electrical muscle"],
    "Golf": ["golf club", "golf academy", "golf training", "golf studio"],
    "Gym": [
        "health club", "fitness club", "fitness center", "fitness centre",
        "anytime fitness", "planet fitness", "snap fitness", "jetts fitness",
        "goodlife", "genesis gym", "world gym", "24 hour fitness", "la fitness",
        "crunch fitness", "equinox", "pure gym",
    ],
    "HIIT/Functional": [
        "crossfit", "f45", "orange theory", "orangetheory", "9round",
        "barry's bootcamp", "barrys bootcamp", "hiit", "bootcamp",
        "boot camp", "functional fitness", "functional training",
        "circuit training", "wod ", " wod",
    ],
    "Injury Prevention": [
        "physiotherapy", "physio ", " physio", "rehabilitation", "rehab gym",
        "chiropractic", "sports therapy", "injury prevention", "sports medicine",
    ],
    "Martial Arts": [
        "mma", "mixed martial arts", "jiu jitsu", "bjj", "muay thai",
        "kickboxing", "karate", "taekwondo", "judo", "wrestling", "ufc gym",
        "combat sports", "grappling", "martial arts",
    ],
    "Pilates": ["pilates", "reformer pilates", "clinical pilates"],
    "Spin/Indoor Cycle": [
        "spin class", "indoor cycling", "cyclebar", "spinning studio",
        "cycle studio", "peloton studio",
    ],
    "Tanning": ["tanning salon", "spray tan", "solarium", "sun studio"],
    "Wellness/Recovery": [
        "wellness centre", "wellness center", "float tank", "floatation",
        "cryotherapy", "infrared sauna", "salt cave", "massage therapy",
        "recovery centre", "recovery center", "spa and wellness",
    ],
    "Yoga": [
        "yoga studio", "yoga centre", "yoga center", "hot yoga",
        "bikram yoga", "power yoga", "ashtanga", "vinyasa studio",
    ],
}

# Domain → (modality, brand_tier) for known brands
KNOWN_BRANDS = {
    "anytimefitness.com": ("Gym", "Enterprise"),
    "planetfitness.com": ("Gym", "Enterprise"),
    "snapfitness.com": ("Gym", "Enterprise"),
    "f45training.com": ("HIIT/Functional", "Enterprise"),
    "orangetheory.com": ("HIIT/Functional", "Enterprise"),
    "goodlife.com.au": ("Gym", "Enterprise"),
    "jetts.com.au": ("Gym", "Enterprise"),
    "crossfit.com": ("HIIT/Functional", "Enterprise"),
    "9round.com": ("HIIT/Functional", "Enterprise"),
    "pilatesplus.com.au": ("Pilates", "MID"),
    "fitstop.com.au": ("HIIT/Functional", "MID"),
    "ufc.com": ("Martial Arts", "Enterprise"),
    "ufcgym.com": ("Martial Arts", "Enterprise"),
    "equinox.com": ("Gym", "Enterprise"),
    "crunchfitness.com": ("Gym", "Enterprise"),
}


def match_keywords(text: str) -> str | None:
    if not text:
        return None
    text_lower = text.lower()
    for modality, keywords in MODALITY_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                return modality
    return None


def scrape_head(url: str) -> dict:
    """Fetch only the <head> of a website — fast, minimal bandwidth."""
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"},
                         allow_redirects=True)
        soup = BeautifulSoup(r.text[:8000], "lxml")  # Only parse first 8KB
        title = soup.title.string.strip() if soup.title else ""
        meta_desc = ""
        for tag in soup.find_all("meta"):
            if tag.get("name", "").lower() in ("description", "keywords"):
                meta_desc += " " + (tag.get("content") or "")
        return {"title": title, "meta": meta_desc.strip()}
    except Exception:
        return {}


def count_locations_from_footer(url: str) -> int | None:
    """
    Scrape the page and count unique addresses in the footer.
    Returns number of locations found, or None if can't determine.
    """
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"},
                         allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")

        # Remove scripts/styles
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # Look for footer
        footer = soup.find("footer") or soup.find(class_=re.compile(r"footer", re.I))
        text_to_search = footer.get_text(" ", strip=True) if footer else soup.get_text(" ", strip=True)

        # Count postcode patterns (AU: 4-digit, US: 5-digit ZIP, UK: postcode)
        au_postcodes = set(re.findall(r'\b[0-9]{4}\b', text_to_search))
        us_zipcodes = set(re.findall(r'\b[0-9]{5}(?:-[0-9]{4})?\b', text_to_search))
        # Generic "123 Street Name" address lines
        street_addresses = re.findall(
            r'\b\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Way|Boulevard|Blvd|Lane|Ln|Place|Pl)',
            text_to_search
        )

        count = max(len(au_postcodes), len(us_zipcodes), len(set(street_addresses)))
        return count if count > 0 else None
    except Exception:
        return None


def brand_tier_from_count(count: int | None) -> str | None:
    if count is None:
        return None
    if count == 1:
        return "SMB"
    if count <= 10:
        return "MID"
    return "Enterprise"


def enrich(company: dict) -> dict | None:
    """
    Returns: {modality, brand_tier, tier, method} or None to escalate.
    brand_tier may be None if count couldn't be determined — escalate tier2 for that only.
    """
    name = company.get("name", "")
    domain = company.get("domain", "") or ""
    website = company.get("website", "") or ""

    # 0. Known brand lookup
    domain_clean = domain.lower().replace("www.", "")
    if domain_clean in KNOWN_BRANDS:
        mod, tier_val = KNOWN_BRANDS[domain_clean]
        return {"modality": mod, "brand_tier": tier_val, "tier": 1, "method": "known_brand"}

    url = website if website.startswith("http") else (f"https://{website}" if website else f"https://{domain}" if domain else None)

    # 1. Company name keyword match
    modality = match_keywords(name)

    # 2. Website <head> scrape if no match yet
    head_data = {}
    if url:
        head_data = scrape_head(url)
        if not modality:
            combined = f"{head_data.get('title','')} {head_data.get('meta','')}"
            modality = match_keywords(combined)

    if not modality:
        return None  # Can't determine modality at Tier 1

    # 3. Location count from footer
    location_count = count_locations_from_footer(url) if url else None
    brand_tier = brand_tier_from_count(location_count)

    return {
        "modality": modality,
        "brand_tier": brand_tier,  # May be None — tier2 will fill it
        "tier": 1,
        "method": "keyword" if not head_data else "keyword+head",
        "location_count": location_count,
    }
