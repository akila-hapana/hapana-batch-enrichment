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

GEMINI_PROMPT = """You are a fitness industry analyst. Classify this company.

Return ONLY valid JSON — no markdown, no explanation:
{{
  "modality": "<one of: Barre|Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Personal Training|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null>,
  "reasoning": "<one sentence>"
}}

Rules:
- SMB = 1 location, MID = 2-10 locations, Enterprise = 11+ locations
- brand_tier blank ("") if you cannot determine location count
- Be honest about confidence — only score 90+ when genuinely certain

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
            result = json.loads(m.group())
            mod      = result.get("modality", "")
            tier_val = result.get("brand_tier", "")
            return {
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
        parts.append(f"Google Maps listings for '{name}': {maps_count}")
        if maps_count >= 11:
            parts.append("(Maps: 11+ locations — likely Enterprise)")
        elif maps_count >= 2:
            parts.append(f"(Maps: {maps_count} locations — likely MID)")
        else:
            parts.append("(Maps: 1 location — likely SMB)")

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
        "modality":              result.get("modality", ""),
        "brand_tier":            result.get("brand_tier", ""),
        "modality_confidence":   mod_conf,
        "brand_tier_confidence": tier_conf,
        "location_count":        result.get("location_count"),
        "reasoning":             result.get("reasoning", ""),
        "cost_usd":              cost,
    }
