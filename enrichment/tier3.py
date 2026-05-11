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

HAIKU_SYSTEM = """You are a fitness industry analyst. Classify fitness companies by modality and size.

Return ONLY valid JSON:
{
  "modality": "<one of: Barre|Boxing|Dance|Education|EMS|Golf|Gym|HIIT/Functional|Injury Prevention|Martial Arts|Other|Personal Training|Pilates|Spin/Indoor Cycle|Tanning|Wellness/Recovery|Yoga>",
  "modality_confidence": <0-100>,
  "brand_tier": "<SMB|MID|Enterprise|>",
  "brand_tier_confidence": <0-100>,
  "location_count": <integer or null>,
  "reasoning": "<one sentence>"
}

Rules:
- SMB = 1 location, MID = 2-10, Enterprise = 11+
- brand_tier blank if you truly cannot determine location count
- Be honest about confidence — only score 90+ when genuinely certain
- IMPORTANT: If website content is unavailable or blocked, use your training knowledge
  about the company name/brand to classify. Well-known brands like "Ujam Fitness",
  "Pure Barre", "Barry's Bootcamp" etc. should be classified from brand knowledge alone.
- Dance modality includes: dance fitness, urban dance, Zumba-style, choreographed fitness
- Personal Training: private/1-on-1 studios, PT-only facilities"""


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
        "modality":              modality,
        "brand_tier":            brand_tier,
        "modality_confidence":   mod_conf,
        "brand_tier_confidence": tier_conf,
        "reasoning":             result.get("reasoning", ""),
        "cost_usd":              cost,
        "tier": 3, "method": "haiku_deep",
    }
