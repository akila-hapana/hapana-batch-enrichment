"""
Tier 1 — Pure keyword classification. No HTTP calls.
Reads scraped_text, location_count, maps_count from T0 context.
Only returns when BOTH modality_confidence >= 90 AND brand_tier_confidence >= 90.
"""
import os as _os

# ---------------------------------------------------------------------------
# Known brands — loaded from data/brand_whitelist.txt (domain | modality)
# ---------------------------------------------------------------------------

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
                    domain = (domain.strip().lower()
                              .replace("www.", "")
                              .replace("http://", "")
                              .replace("https://", ""))
                    brands[domain] = modality.strip()
    except Exception:
        pass
    return brands


BRAND_WHITELIST = _load_whitelist()

# Known brands with BOTH modality AND brand_tier (Enterprise chains)
KNOWN_BRANDS = {
    # Gym
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
    # Wellness
    "floatation.com.au":      ("Wellness/Recovery", "SMB"),
    # Boxing / Martial Arts
    "titleboxingclub.com":    ("Boxing", "Enterprise"),
    "rumble.com":             ("Boxing", "Enterprise"),
    "ilovekickboxing.com":    ("Martial Arts", "Enterprise"),
    "graceacademy.com.au":    ("Martial Arts", "SMB"),
}

# Strong keywords — each gives ≥90% modality confidence on its own
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
        "mobile personal training", "in-home personal training",
        "in-home training", "we come to you", "travel to clients",
        "home fitness training", "virtual personal training",
        "online personal trainer",
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
    "Education": [
        "fitness association", "fitness federation", "fitness professionals",
        "instructor certification", "instructor training program",
        "fitness licensing", "licensing program fitness",
        "fitness industry association", "fitness trade association",
        "certified fitness professionals", "fitness certification body",
    ],
}

# Weaker keywords — signal but not ≥90% alone
SIGNAL_KEYWORDS = {
    "Gym":               ["gym", "fitness", "health club", "fitness center",
                          "fitness centre", "athletic club"],
    "HIIT/Functional":   ["crossfit", "bootcamp", "hiit", "functional"],
    "Yoga":              ["yoga"],
    "Pilates":           ["pilates"],
    "Martial Arts":      ["mma", "martial arts", "jiu jitsu", "muay thai",
                          "kickboxing", "karate", "taekwondo"],
    "Boxing":            ["boxing"],
    "Spin/Indoor Cycle": ["spin", "cycling studio", "indoor cycle"],
    "Dance":             ["dance", "ballet", "zumba", "urban dance"],
    "Barre":             ["barre"],
    "Personal Training": ["personal training", "personal trainer",
                          "private trainer", "1-on-1", "one-on-one",
                          "mobile training", "in-home", "we come to you",
                          "virtual coaching", "online coach"],
    "Wellness/Recovery": ["wellness", "recovery", "float", "cryotherapy",
                          "infrared"],
    "EMS":               ["ems"],
    "Golf":              ["golf"],
    "Tanning":           ["tanning salon", "spray tan", "solarium"],
    "Education":         ["fitness education", "fitness academy",
                          "personal training course", "association",
                          "federation", "certification", "licensing",
                          "fitness professionals", "instructor program"],
}


def _match_strong(text: str) -> str | None:
    t = text.lower()
    for modality, phrases in STRONG_KEYWORDS.items():
        for p in phrases:
            if p in t:
                return modality
    return None


def brand_tier_from_count(count: int | None) -> str | None:
    if count is None:
        return None
    if count == 1:
        return "SMB"
    if count <= 10:
        return "MID"
    return "Enterprise"


def enrich(t0: dict) -> dict | None:
    """
    Pure keyword classification from T0-collected data. No HTTP calls.
    Returns result dict if both confidences >= 90%, else None to escalate.
    """
    name   = (t0.get("name") or "").strip()
    domain = (t0.get("domain") or "").lower().replace("www.", "")
    scraped_text   = t0.get("scraped_text", "")
    location_count = t0.get("location_count")
    maps_count     = t0.get("maps_count")

    # --- Known brand exact match (both properties, 100% confidence) ---
    if domain in KNOWN_BRANDS:
        mod, tier = KNOWN_BRANDS[domain]
        return {"modality": mod, "brand_tier": tier,
                "modality_confidence": 100, "brand_tier_confidence": 100,
                "tier": 1, "method": "known_brand"}

    # --- Brand whitelist (modality 100%, tier from location data) ---
    whitelist_modality = BRAND_WHITELIST.get(domain)

    # --- Build text for matching ---
    combined = f"{name} {scraped_text[:1000]}" if scraped_text else name

    # --- Modality ---
    if whitelist_modality:
        modality       = whitelist_modality
        mod_confidence = 100
    else:
        modality       = _match_strong(name) or _match_strong(combined)
        mod_confidence = 95 if modality else 0

    if not modality:
        return None

    # --- Brand tier from T0 location data (no scraping here) ---
    count = location_count if location_count is not None else maps_count
    brand_tier = brand_tier_from_count(count)

    if brand_tier is None:
        return None

    # SMB (single location) is only 85% confident — still escalates to T2
    tier_confidence = 90 if count and count > 1 else 85

    if mod_confidence >= 90 and tier_confidence >= 90:
        return {
            "modality":              modality,
            "brand_tier":            brand_tier,
            "modality_confidence":   mod_confidence,
            "brand_tier_confidence": tier_confidence,
            "location_count":        count,
            "tier": 1, "method": "keyword+t0_location",
        }

    return None
