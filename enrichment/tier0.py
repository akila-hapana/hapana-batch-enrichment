"""
Tier 0 — Pre-flight domain validation (free, no AI).
Checks if the domain is reachable before spending any tokens.
If unreachable, attempts ONE DuckDuckGo search for the company name.
If still no confident match → returns _skip=True (modality=Other, brand_tier=blank).
"""
import re
import requests
from difflib import SequenceMatcher
from urllib.parse import urlparse

REACH_TIMEOUT = 5    # seconds for reachability HEAD request
SEARCH_TIMEOUT = 6   # seconds for DDG search

# Company name must be ≥40% similar to found domain name to use it
SIMILARITY_THRESHOLD = 0.40


def _is_reachable(url: str) -> bool:
    """HEAD request — fast check, follows redirects."""
    try:
        r = requests.head(
            url, timeout=REACH_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return r.status_code < 400
    except Exception:
        # Try GET fallback (some servers block HEAD)
        try:
            r = requests.get(
                url, timeout=REACH_TIMEOUT,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
                stream=True,
            )
            r.close()
            return r.status_code < 400
        except Exception:
            return False


def _name_similarity(a: str, b: str) -> float:
    """Case-insensitive token overlap ratio."""
    a = re.sub(r"[^\w\s]", "", a.lower().strip())
    b = re.sub(r"[^\w\s]", "", b.lower().strip())
    return SequenceMatcher(None, a, b).ratio()


def _ddg_search(company_name: str) -> tuple[str | None, str | None]:
    """
    One DuckDuckGo instant-answer lookup.
    Returns (domain, title) of the best result, or (None, None).
    """
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={
                "q": company_name,
                "format": "json",
                "no_html": 1,
                "skip_disambig": 1,
            },
            timeout=SEARCH_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        data = r.json()

        # Best case: a direct abstract with URL
        if data.get("AbstractURL"):
            parsed = urlparse(data["AbstractURL"])
            domain = parsed.netloc.replace("www.", "")
            title = data.get("Heading", "")
            return domain, title

        # Fall back to first related topic
        for topic in data.get("RelatedTopics", [])[:2]:
            if isinstance(topic, dict) and topic.get("FirstURL"):
                parsed = urlparse(topic["FirstURL"])
                domain = parsed.netloc.replace("www.", "")
                title = topic.get("Text", "")
                return domain, title

    except Exception:
        pass
    return None, None


def check(company: dict) -> dict:
    """
    Returns the company dict (possibly with corrected domain) if we should proceed,
    or a dict with _skip=True and skip_reason if the company should be classified
    as Other/blank without spending any tokens.
    """
    name = (company.get("name") or "").strip()
    domain = (company.get("domain") or "").lower().replace("www.", "")
    website = company.get("website") or ""

    # Build URL to test reachability
    if website.startswith("http"):
        url = website
    elif website:
        url = "https://" + website
    elif domain:
        url = "https://" + domain
    else:
        return {**company, "_skip": True, "skip_reason": "no_domain_or_website"}

    # ── 1. Reachability check ──
    if _is_reachable(url):
        return company  # All good — proceed to T1

    # ── 2. Domain unreachable — one DuckDuckGo search ──
    found_domain, found_title = _ddg_search(name)

    if not found_domain:
        return {**company, "_skip": True,
                "skip_reason": f"unreachable ({domain}) · no search result"}

    # ── 3. Similarity check: company name vs. found domain name or title ──
    # Strip TLD from domain for comparison (e.g. "groupmarketing" from "groupmarketing.com")
    domain_stem = re.sub(r"\.\w{2,4}$", "", found_domain)
    sim_domain = _name_similarity(name, domain_stem)
    sim_title  = _name_similarity(name, found_title) if found_title else 0.0
    best_sim   = max(sim_domain, sim_title)

    if best_sim < SIMILARITY_THRESHOLD:
        return {**company, "_skip": True,
                "skip_reason": f"unreachable ({domain}) · search found {found_domain} (sim={best_sim:.2f} < {SIMILARITY_THRESHOLD})"}

    # ── 4. Use the corrected domain ──
    updated = dict(company)
    updated["domain"]            = found_domain
    updated["website"]           = f"https://{found_domain}"
    updated["_domain_corrected"] = True
    updated["_original_domain"]  = domain
    return updated
