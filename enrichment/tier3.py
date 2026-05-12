"""
Tier 3 — Claude Haiku 4.5 (~$0.002/call).
Last resort. Pure interpretation from T0 context. No HTTP calls.
If BOTH confidences < 90 after Haiku → modality="Other", brand_tier="" (blank).
"""
import os
import re
import json
import requests

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

_HAIKU_IN  = 0.80 / 1_000_000
_HAIKU_OUT = 4.00 / 1_000_000

VALID_MODALITIES = [
    "Barre", "Boxing", "Dance", "Education", "EMS", "Golf", "Gym",
    "HIIT/Functional", "Injury Prevention", "Martial Arts", "Other",
    "Personal Training", "Pilates", "Spin/Indoor Cycle", "Tanning",
    "Wellness/Recovery", "Yoga",
]

HAIKU_SYSTEM = """You are a fitness industry analyst. Classify fitness companies in two steps.

STEP 1 — Determine business model:
  OPERATOR    = Owns/operates physical fitness studios or gyms at its OWN locations
  LICENSOR    = Licenses a fitness format/program to OTHER gyms — does NOT own locations
  ASSOCIATION = Industry trade org, certification body, professional development for fitness pros — also universities, colleges, and schools (campus rec centres are fitness customers)
  NON_FITNESS = No meaningful fitness connection

STEP 2 — Apply rules by model:
  OPERATOR    → choose modality + count OWNED locations for brand_tier
  LICENSOR    → choose fitness modality, brand_tier = "" (no owned locations)
  ASSOCIATION → modality = "Education", brand_tier = ""
  NON_FITNESS → modality = "Other", brand_tier = ""

Return ONLY valid JSON:
{
  "business_model": "<Operator|Licensor|Association|Non_fitness>",
  "modality": "<one of: Barre|Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Personal Training|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null — OWNED locations only>,
  "reasoning": "<one sentence explaining key signals>"
}

Critical rules:
- SMB = 1 owned location, MID = 2-10, Enterprise = 11+ — only for OPERATOR type
- brand_tier blank if you truly cannot determine owned location count
- Be honest about confidence — only score 90+ when genuinely certain
- If website content is unavailable, use training knowledge about the brand
- Dance includes: dance fitness, urban dance, Zumba-style, choreographed fitness
- Personal Training includes: mobile PT, in-home training, "we come to you", virtual coaching
- Education includes: fitness associations, certification bodies, industry trade orgs, universities, colleges, schools"""


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
        resp  = r.json()
        text  = resp.get("content", [{}])[0].get("text", "")
        usage = resp.get("usage", {})
        cost  = (usage.get("input_tokens", 0) * _HAIKU_IN +
                 usage.get("output_tokens", 0) * _HAIKU_OUT)
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


def enrich(t0: dict, previous: dict | None = None) -> dict:
    """
    Final classification using T0 scraped content. Always returns a result.
    If both confidences < 90 → modality="Other", brand_tier="" (blank).
    """
    name         = (t0.get("name") or "").strip()
    scraped_text = t0.get("scraped_text", "")

    content = scraped_text if scraped_text else (
        "[Website content unavailable — classify based on company name "
        "and brand knowledge only]"
    )

    result   = _call_haiku(name, content) if name else {}
    mod_conf = result.get("modality_confidence", 0)
    tier_conf= result.get("brand_tier_confidence", 0)
    modality = result.get("modality", "")
    brand_tier = result.get("brand_tier", "")
    cost     = result.get("_cost", 0.0)

    # If a previous partial result had higher confidence on one property, prefer it
    if previous:
        if previous.get("modality_confidence", 0) > mod_conf and previous.get("modality"):
            modality  = previous["modality"]
            mod_conf  = previous["modality_confidence"]
        if previous.get("brand_tier_confidence", 0) > tier_conf and previous.get("brand_tier"):
            brand_tier = previous["brand_tier"]
            tier_conf  = previous["brand_tier_confidence"]

    # Final gate
    if mod_conf < 90:
        modality  = "Other"
        mod_conf  = 0
    if tier_conf < 90:
        brand_tier = ""
        tier_conf  = 0

    return {
        "business_model":        result.get("business_model", ""),
        "modality":              modality,
        "brand_tier":            brand_tier,
        "modality_confidence":   mod_conf,
        "brand_tier_confidence": tier_conf,
        "reasoning":             result.get("reasoning", ""),
        "cost_usd":              cost,
        "tier": 3, "method": "haiku_deep",
    }
