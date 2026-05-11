"""
Tier 1 — Free, deterministic, rule-based.
Only returns a result when confidence >= 90% in BOTH modality AND brand_tier.
No AI calls, no paid APIs.
"""
import re
import requests
from bs4 import BeautifulSoup
from enrichment import BROWSER_HEADERS

TIMEOUT = 8

# ---------------------------------------------------------------------------
# Known brands — loaded from data/brand_whitelist.txt (domain | modality)
# These give 100% modality confidence. Brand tier still needs location scrape.
# ---------------------------------------------------------------------------
import os as _os

def _load_whitelist() -> dict[str, str]:
    path = _os.path.join(_os.path.dirname(__file__), "..", "data", "brand_whitelist.txt")
    brands = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "|" in line:
                    domain, modality = line.split("|", 1)
                    domain = domain.strip().lower().replace("www.", "").replace("http://", "").replace("https://", "")
                    brands[domain] = modality.strip()
    except Exception:
        pass
    return brands

BRAND_WHITELIST = _load_whitelist()  # domain → modality (321 brands from existing app)

# Known brands with BOTH modality AND brand_tier (Enterprise chains only)
KNOWN_BRANDS = {
    # Gym / General Fitness
    "anytimefitness.com":     ("Gym", "Enterprise"),
    "planetfitness.com":      ("Gym", "Enterprise"),
    "snapfitness.com":        ("Gym", "Enterprise"),
    "goodlife.com.au":        ("Gym", "Enterprise"),
    "jetts.com.au":           ("Gym", "Enterprise"),
    "jetts.com":              ("Gym", "Enterprise"),
    "crunchfitness.com":      ("Gym", "Enterprise"),
    "equinox.com":            ("Gym", "Enterprise"),
    "24hourfitness.com":      ("Gym", "Enterprise"),
    "lifetimefitness.com":    ("Gym", "Enterprise"),
    "lifetime.life":          ("Gym", "Enterprise"),
    "goldsgym.com":           ("Gym", "Enterprise"),
    "lafitness.com":          ("Gym", "Enterprise"),
    "worldgym.com":           ("Gym", "Enterprise"),
    "fitnessworld.com":       ("Gym", "Enterprise"),
    "virginactive.com":       ("Gym", "Enterprise"),
    "virginactive.com.au":    ("Gym", "Enterprise"),
    "thefitnessgroup.com.au": ("Gym", "Enterprise"),
    "gym.com.au":             ("Gym", "MID"),
    "genesis.com.au":         ("Gym", "Enterprise"),
    "genesisgym.com.au":      ("Gym", "Enterprise"),
    "fitstop.com.au":         ("HIIT/Functional", "Enterprise"),
    # HIIT / Functional
    "f45training.com":        ("HIIT/Functional", "Enterprise"),
    "orangetheory.com":       ("HIIT/Functional", "Enterprise"),
    "9round.com":             ("HIIT/Functional", "Enterprise"),
    "crossfit.com":           ("HIIT/Functional", "Enterprise"),
    "barrysfitness.com":      ("HIIT/Functional", "Enterprise"),
    "barrys.com":             ("HIIT/Functional", "Enterprise"),
    "1rebel.com":             ("HIIT/Functional", "MID"),
    "bodyfit.com.au":         ("HIIT/Functional", "Enterprise"),
    "bodyfittraining.com":    ("HIIT/Functional", "Enterprise"),
    "ufcgym.com":             ("Martial Arts", "Enterprise"),
    "ufc.com":                ("Martial Arts", "Enterprise"),
    # Pilates
    "reformerpilatesnation.com.au": ("Pilates", "MID"),
    "clubpilates.com":        ("Pilates", "Enterprise"),
    "pvolve.com":             ("Pilates", "Enterprise"),
    # Yoga
    "corepoweryoga.com":      ("Yoga", "Enterprise"),
    "bikiramyoga.com":        ("Yoga", "Enterprise"),
    "hotyogaaustralia.com.au":("Yoga", "MID"),
    "yogaworks.com":          ("Yoga", "Enterprise"),
    "yogatribe.com.au":       ("Yoga", "MID"),
    # Spin
    "soulcycle.com":          ("Spin/Indoor Cycle", "Enterprise"),
    "cyclebar.com":           ("Spin/Indoor Cycle", "Enterprise"),
    # Wellness / Recovery
    "floatation.com.au":      ("Wellness/Recovery", "SMB"),
    # Boxing / Martial Arts
    "titleboxingclub.com":    ("Boxing", "Enterprise"),
    "rumble.com":             ("Boxing", "Enterprise"),
    "ilovekickboxing.com":    ("Martial Arts", "Enterprise"),
    "graceacademy.com.au":    ("Martial Arts", "SMB"),
}

# ---------------------------------------------------------------------------
# Strong modality keywords — each term gives ≥90% modality confidence
# (keyword must appear as a standalone word/phrase in the name)
# ---------------------------------------------------------------------------
STRONG_KEYWORDS = {
    "HIIT/Functional": [
        "crossfit", "f45", "orange theory", "orangetheory", "9round",
        "barry's", "barrys", "functional fitness", "functional training",
        "hiit studio", "bootcamp fitness", "barbell club",
    ],
    "Yoga": [
        "yoga studio", "yoga centre", "yoga center", "hot yoga",
        "bikram yoga", "power yoga", "ashtanga yoga", "vinyasa yoga",
        "yin yoga", "hatha yoga", "yoga space",
    ],
    "Pilates": [
        "pilates studio", "pilates centre", "pilates center",
        "reformer pilates", "clinical pilates", "mat pilates",
    ],
    "Martial Arts": [
        "mma gym", "mixed martial arts", "jiu jitsu", "bjj academy",
        "muay thai", "taekwondo academy", "karate dojo", "ufc gym",
        "kickboxing academy", "martial arts academy", "judo club",
    ],
    "Boxing": [
        "boxing gym", "boxing studio", "boxing club", "boxing academy",
        "title boxing",
    ],
    "Spin/Indoor Cycle": [
        "spin studio", "indoor cycling studio", "cyclebar", "soul cycle",
        "cycling studio",
    ],
    "Dance": [
        "dance studio", "dance academy", "ballet school",
        "zumba studio", "hip hop dance", "dance fitness",
    ],
    "Barre": [
        "barre studio", "barre class", "barre fitness", "barre academy",
        "pure barre", "barre method",
    ],
    "Personal Training": [
        "personal training studio", "personal training gym",
        "personal trainer studio", "private training studio",
        "one on one training", "1-on-1 training", "pt studio",
    ],
    "Wellness/Recovery": [
        "float tank", "flotation centre", "cryotherapy", "infrared sauna",
        "salt cave", "wellness spa", "recovery studio",
    ],
    "EMS": [
        "ems training", "electro muscle", "miha bodytec",
    ],
    "Golf": [
        "golf academy", "golf studio", "indoor golf",
    ],
}

# Weaker keywords — give modality signal but not ≥90% alone
SIGNAL_KEYWORDS = {
    "Gym":               ["gym", "fitness", "health club", "fitness center", "fitness centre", "athletic club"],
    "HIIT/Functional":   ["crossfit", "bootcamp", "hiit", "functional"],
    "Yoga":              ["yoga"],
    "Pilates":           ["pilates"],
    "Martial Arts":      ["mma", "martial arts", "jiu jitsu", "muay thai", "kickboxing", "karate", "taekwondo"],
    "Boxing":            ["boxing"],
    "Spin/Indoor Cycle": ["spin", "cycling studio", "indoor cycle"],
    "Dance":             ["dance", "ballet", "zumba", "urban dance"],
    "Barre":             ["barre"],
    "Personal Training": ["personal training", "personal trainer", "private trainer", "1-on-1", "one-on-one"],
    "Wellness/Recovery": ["wellness", "recovery", "float", "cryotherapy", "infrared"],
    "EMS":               ["ems"],
    "Golf":              ["golf"],
    "Tanning":           ["tanning salon", "spray tan", "solarium"],
    "Education":         ["fitness education", "fitness academy", "personal training course"],
}

NAV_LOCATION_HINTS = [
    "location", "locations", "studio", "studios", "find us",
    "our clubs", "our gyms", "find a gym", "find a studio", "branches",
]


def _url(company: dict) -> str | None:
    w = company.get("website") or ""
    d = company.get("domain") or ""
    if w.startswith("http"):
        return w
    if w:
        return "https://" + w
    if d:
        return "https://" + d
    return None


def _match_strong(text: str) -> str | None:
    t = text.lower()
    for modality, phrases in STRONG_KEYWORDS.items():
        for p in phrases:
            if p in t:
                return modality
    return None


def _match_signal(text: str) -> str | None:
    t = text.lower()
    for modality, phrases in SIGNAL_KEYWORDS.items():
        for p in phrases:
            if p in t:
                return modality
    return None


def _is_bot_wall(text: str) -> bool:
    t = text.lower()[:2000]
    phrases = ["attention required", "cloudflare", "you have been blocked",
               "access denied", "enable javascript", "ddos protection",
               "checking your browser"]
    return sum(1 for p in phrases if p in t) >= 2


def _scrape_head(url: str) -> dict:
    try:
        r = requests.get(url, timeout=TIMEOUT, headers=BROWSER_HEADERS,
                         allow_redirects=True)
        if _is_bot_wall(r.text):
            return {"_bot_blocked": True}
        soup = BeautifulSoup(r.text[:10000], "lxml")
        title = (soup.title.string or "").strip() if soup.title else ""
        meta = " ".join(
            tag.get("content", "") for tag in soup.find_all("meta")
            if tag.get("name", "").lower() in ("description", "keywords")
        )
        h1 = " ".join(h.get_text(strip=True) for h in soup.find_all("h1")[:3])
        nav_links = []
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(k in txt for k in NAV_LOCATION_HINTS):
                nav_links.append(a["href"])
        return {"title": title, "meta": meta, "h1": h1,
                "nav_location_links": nav_links, "soup": soup, "html": r.text}
    except Exception:
        return {}


def _count_locations(url: str, head: dict) -> int | None:
    """
    Returns a location count if we can determine it with high confidence.
    Follows a locations nav link if found.
    """
    nav_links = head.get("nav_location_links", [])
    soup_main = head.get("soup")
    html = head.get("html", "")

    # Follow the locations page if nav link found
    locations_page_html = None
    if nav_links:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            href = nav_links[0]
            loc_url = href if href.startswith("http") else f"{parsed.scheme}://{parsed.netloc}{href}"
            r2 = requests.get(loc_url, timeout=TIMEOUT, headers=BROWSER_HEADERS)
            locations_page_html = r2.text
        except Exception:
            pass

    # Count addresses in the most content-rich source available
    for content in [locations_page_html, html]:
        if not content:
            continue
        soup = BeautifulSoup(content[:80000], "lxml")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(" ")

        # Count AU/NZ postcodes (4-digit), US ZIP (5-digit)
        postcodes = set(re.findall(r'\b\d{4,5}\b', text))
        # Count "Level/Floor X" occurrences as address lines
        floor_mentions = re.findall(r'\b(?:Level|Floor|Suite)\s+\d+\b', text, re.I)
        # Street address pattern
        streets = re.findall(r'\b\d{1,5}\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s+'
                             r'(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Way|Blvd|Lane)\b', text)

        count = max(len(postcodes), len(set(streets)), len(set(floor_mentions)))
        if count > 0:
            return count

    return None


def brand_tier_from_count(count: int | None) -> str | None:
    if count is None:
        return None
    if count == 1:
        return "SMB"
    if count <= 10:
        return "MID"
    return "Enterprise"


def enrich(company: dict, scraped_text: str = "") -> dict | None:
    """
    Returns result dict with confidence scores, or None to escalate.
    scraped_text: pre-scraped content from scraper.py (all stages already attempted).
    Only returns if modality_confidence >= 90 AND brand_tier_confidence >= 90.
    """
    name = (company.get("name") or "").strip()
    domain = (company.get("domain") or "").lower().replace("www.", "")
    url = _url(company)

    # --- Known brand exact domain match (KNOWN_BRANDS = both properties) ---
    if domain in KNOWN_BRANDS:
        mod, tier = KNOWN_BRANDS[domain]
        return {"modality": mod, "brand_tier": tier,
                "modality_confidence": 100, "brand_tier_confidence": 100,
                "tier": 1, "method": "known_brand"}

    # --- Brand whitelist match (modality only, 100% confidence) ---
    whitelist_modality = BRAND_WHITELIST.get(domain)

    # --- Use pre-scraped text; fall back to scraping head if not provided ---
    if scraped_text:
        combined_text = f"{name} {scraped_text[:1000]}"
        head = {}
    else:
        head = _scrape_head(url) if url else {}
        if head.get("_bot_blocked"):
            combined_text = name
        else:
            combined_text = f"{name} {head.get('title','')} {head.get('meta','')} {head.get('h1','')}"

    # --- Modality: whitelist wins, then strong keyword ---
    if whitelist_modality:
        modality = whitelist_modality
        mod_confidence = 100
    else:
        modality = _match_strong(name) or _match_strong(combined_text)
        mod_confidence = 95 if modality else 0

    if not modality:
        return None

    # --- Location count for brand_tier (still scrapes locations page) ---
    loc_count = _count_locations(url, head) if url else None
    brand_tier = brand_tier_from_count(loc_count)

    if brand_tier is None:
        return None

    tier_confidence = 90 if loc_count and loc_count > 1 else 85

    if mod_confidence >= 90 and tier_confidence >= 90:
        return {"modality": modality, "brand_tier": brand_tier,
                "modality_confidence": mod_confidence,
                "brand_tier_confidence": tier_confidence,
                "location_count": loc_count,
                "tier": 1, "method": "keyword+location_scrape"}

    return None
