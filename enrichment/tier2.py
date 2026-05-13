"""
Tier 2 — Gemini 1.5 Flash via Vertex AI (~$0.0004/call).
Pure interpretation — reads all context from T0. No HTTP calls to websites.
Returns result if BOTH confidences >= 90%, else partial for T3 to build on.
"""
import os
import re
import json
import requests

GCP_PROJECT  = os.environ.get("VERTEX_PROJECT", "hapana-internal-platform")
GCP_LOCATION = "us-central1"
GEMINI_MODEL = "gemini-1.5-flash"
SA_KEY_JSON  = os.environ.get("GCP_SERVICE_ACCOUNT_KEY", "")

_GEMINI_IN  = 0.075 / 1_000_000
_GEMINI_OUT = 0.30  / 1_000_000

VALID_MODALITIES = [
    "Barre", "Boxing", "Dance", "Education", "EMS", "Golf", "Gym",
    "HIIT/Functional", "Injury Prevention", "Martial Arts", "Other",
    "Personal Training", "Pilates", "Spin/Indoor Cycle", "Tanning",
    "Wellness/Recovery", "Yoga",
]

GEMINI_PROMPT = """You are a fitness industry analyst. Classify this company in two steps.

STEP 1 — Determine business model (critical — this drives everything else):
  OPERATOR    = Owns/operates physical fitness studios, gyms, or classes at its OWN locations
  LICENSOR    = Licenses a fitness format/program to OTHER gyms (e.g. Les Mills, Ujam, Zumba) — does NOT own locations
  ASSOCIATION = Industry trade org, certification body, professional development for fitness pros (e.g. IHRSA, FBA, NASM, ACE) — also includes universities, colleges, and schools (campus rec centres are fitness customers)
  NON_FITNESS = No meaningful fitness connection (tech company, marketing agency, insurance broker, etc.)

STEP 2 — Apply classification rules by model:
  OPERATOR    → choose modality + brand_tier from OWNED location count (SMB=1, MID=2-10, Enterprise=11+)
  LICENSOR    → choose modality + brand_tier based on REACH/SCALE (global program = Enterprise, regional = MID, local = SMB)
  ASSOCIATION → modality = "Education" + brand_tier based on scale (university/large org = Enterprise, regional = MID, local = SMB)
  NON_FITNESS → modality = "Other", brand_tier based on estimated company size (SMB/MID/Enterprise)

⚠ brand_tier applies to ALL business models — every company has a scale. Only leave blank if you have zero information.

⚠ GOOGLE MAPS WARNING: A high Maps listing count does NOT always mean the company owns those locations.
  Instructor-licensing programs (Ujam, Zumba, Les Mills) show up in Maps at partner gyms worldwide.
  If Maps count is high BUT the website has no /locations page and no owned address patterns →
  those are partner/class listings, NOT owned locations. Do NOT use Maps count alone for Enterprise.
  Only assign Enterprise if you have clear evidence of 11+ company-owned locations.

Return ONLY valid JSON — no markdown, no explanation:
{{
  "business_model": "<Operator|Licensor|Association|Non_fitness>",
  "modality": "<one of: Barre|Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Personal Training|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null — OWNED locations only, null if unknown>,
  "reasoning": "<one sentence explaining the key signals that determined business model + modality>"
}}

Additional modality notes:
- Personal Training includes: mobile/in-home PT, virtual coaching, "we come to you" services
- Education includes: fitness associations, certification bodies, industry trade orgs, licensing programs, universities, colleges, schools
- SMB = 1 location/operator or very small reach, MID = 2-10 locations or regional reach, Enterprise = 11+ or global reach — applies to ALL business models
- Personal Training brand_tier only if you have clear evidence of physical locations — leave blank for mobile/virtual/solo PTs with no fixed studio
- Global fitness licensing programs (Ujam, Zumba, Les Mills) → Enterprise
- Universities and large associations → Enterprise

Company: {name}
{context}"""


def _get_vertex_token() -> str | None:
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
    token = _get_vertex_token()
    if not token:
        return {}
    prompt = GEMINI_PROMPT.format(name=name, context=context)
    url = (f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1/projects/{GCP_PROJECT}"
           f"/locations/{GCP_LOCATION}/publishers/google/models/{GEMINI_MODEL}:generateContent")
    try:
        r = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            json={
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 250, "temperature": 0},
            },
            timeout=20,
        )
        resp = r.json()
        text = resp["candidates"][0]["content"]["parts"][0]["text"]
        usage = resp.get("usageMetadata", {})
        cost  = (usage.get("promptTokenCount", 0) * _GEMINI_IN +
                 usage.get("candidatesTokenCount", 0) * _GEMINI_OUT)
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            result   = json.loads(m.group())
            mod      = result.get("modality", "")
            tier_val = result.get("brand_tier", "")
            biz      = result.get("business_model", "")
            return {
                "business_model":        biz if biz in ("Operator","Licensor","Association","Non_fitness") else "",
                "modality":              mod if mod in VALID_MODALITIES else "",
                "modality_confidence":   int(result.get("modality_confidence", 0)),
                "brand_tier":            tier_val if tier_val in ("SMB", "MID", "Enterprise", "") else "",
                "brand_tier_confidence": int(result.get("brand_tier_confidence", 0)),
                "location_count":        result.get("location_count"),
                "reasoning":             result.get("reasoning", ""),
                "_cost":                 cost,
            }
    except Exception:
        pass
    return {}


def enrich(t0: dict, tier1_result: dict | None = None) -> dict | None:
    """
    Pure Gemini classification using T0-collected context. No HTTP calls.
    Returns result if both >= 90%, else partial dict for T3.
    """
    name              = (t0.get("name") or "").strip()
    scraped_text      = t0.get("scraped_text", "")
    location_count    = t0.get("location_count")
    maps_count        = t0.get("maps_count")
    locations_snippet = t0.get("locations_snippet", "")
    apollo_industry   = t0.get("apollo_industry", "")
    apollo_employees  = t0.get("apollo_employees")

    parts = []

    # Apollo context
    if apollo_industry:
        parts.append(f"Industry (Apollo): {apollo_industry}")
    if apollo_employees:
        parts.append(f"Employees (Apollo): ~{apollo_employees}")

    # Google Maps location signal
    if maps_count is not None:
        reliable = t0.get("maps_count_reliable", True)
        if reliable:
            parts.append(f"Google Maps listings for '{name}': {maps_count}")
            if maps_count >= 11:
                parts.append("(Maps: 11+ locations — likely Enterprise)")
            elif maps_count >= 2:
                parts.append(f"(Maps: {maps_count} locations — likely MID)")
            else:
                parts.append("(Maps: 1 location — likely SMB)")
        else:
            parts.append(
                f"Google Maps returned {maps_count} listings for '{name}' — "
                f"WARNING: these are likely partner/class listings at other gyms, NOT owned locations. "
                f"Do NOT use this count to determine brand_tier or classify as Enterprise. "
                f"Look only at the website content for owned location evidence."
            )

    # Website-based location count
    if location_count is not None:
        parts.append(f"Website location count (postcodes/addresses found): {location_count}")

    # Locations page snippet
    if locations_snippet:
        parts.append(f"Locations page content:\n{locations_snippet}")

    # Main scraped content
    if scraped_text:
        parts.append(scraped_text[:3000])

    if not parts:
        return None

    context = "\n".join(parts)
    result  = _call_gemini(name, context)

    if not result:
        return None

    mod_conf  = result.get("modality_confidence", 0)
    tier_conf = result.get("brand_tier_confidence", 0)
    cost      = result.get("_cost", 0.0)

    if mod_conf >= 90 and tier_conf >= 90 and result.get("modality"):
        return {
            "business_model":        result.get("business_model", ""),
            "modality":              result["modality"],
            "brand_tier":            result.get("brand_tier", ""),
            "modality_confidence":   mod_conf,
            "brand_tier_confidence": tier_conf,
            "location_count":        result.get("location_count"),
            "reasoning":             result.get("reasoning", ""),
            "cost_usd":              cost,
            "tier": 2, "method": "gemini_flash",
        }

    return {
        "_partial":              True,
        "business_model":        result.get("business_model", ""),
        "modality":              result.get("modality", ""),
        "brand_tier":            result.get("brand_tier", ""),
        "modality_confidence":   mod_conf,
        "brand_tier_confidence": tier_conf,
        "location_count":        result.get("location_count"),
        "reasoning":             result.get("reasoning", ""),
        "cost_usd":              cost,
    }
