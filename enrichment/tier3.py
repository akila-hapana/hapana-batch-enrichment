"""
Tier 3 — Advanced: full page scrape + Claude Haiku with extended content.
Only called for companies that couldn't be resolved in Tier 1 or 2.
Small volume expected (~10-15% of batch).
"""
import re
import json
import requests
from bs4 import BeautifulSoup
from .tier2 import classify_with_haiku, brand_tier_from_count, VALID_MODALITIES, TIMEOUT


def deep_scrape(url: str) -> str:
    """
    Scrape full page content, clean it up, return structured text.
    Follows up to 1 internal link (about page / services page) for more context.
    """
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup(["script", "style", "noscript", "svg", "img"]):
            tag.decompose()

        parts = []
        if soup.title:
            parts.append(f"Title: {soup.title.string}")

        # Meta
        for meta in soup.find_all("meta", attrs={"name": re.compile(r"description|keywords", re.I)}):
            parts.append(f"Meta: {meta.get('content','')}")

        # All headings
        for h in soup.find_all(["h1", "h2", "h3"])[:10]:
            text = h.get_text(strip=True)
            if text:
                parts.append(f"H: {text}")

        # Nav
        nav = soup.find("nav")
        if nav:
            parts.append(f"Nav: {nav.get_text(' ', strip=True)[:400]}")

        # Main content (first 1500 chars)
        main = soup.find("main") or soup.find(id=re.compile(r"main|content", re.I)) or soup.body
        if main:
            body_text = main.get_text(" ", strip=True)
            parts.append(f"Content: {body_text[:1500]}")

        # Footer
        footer = soup.find("footer")
        if footer:
            parts.append(f"Footer: {footer.get_text(' ', strip=True)[:600]}")

        # Look for an about/services page and grab its meta
        about_url = None
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if any(k in href for k in ["/about", "/services", "/what-we-do", "/our-story"]):
                from urllib.parse import urlparse
                parsed = urlparse(url)
                about_url = f"{parsed.scheme}://{parsed.netloc}{a['href']}" if a["href"].startswith("/") else a["href"]
                break

        if about_url:
            try:
                r2 = requests.get(about_url, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
                soup2 = BeautifulSoup(r2.text, "lxml")
                for tag in soup2(["script", "style"]):
                    tag.decompose()
                about_text = soup2.get_text(" ", strip=True)
                parts.append(f"About page: {about_text[:800]}")
            except Exception:
                pass

        return "\n".join(parts)
    except Exception:
        return ""


def enrich(company: dict, previous: dict | None = None) -> dict:
    """
    Always returns a result — fallback to Other/blank if nothing found.
    """
    name = company.get("name", "")
    domain = company.get("domain", "") or ""
    website = company.get("website", "") or ""
    url = website if website.startswith("http") else (f"https://{website}" if website else f"https://{domain}" if domain else None)

    modality = previous.get("modality") if previous else None
    brand_tier = previous.get("brand_tier") if previous else None

    if url and (not modality or not brand_tier):
        text = deep_scrape(url)
        if text:
            result = classify_with_haiku(name, text)
            if not modality and result.get("modality") in VALID_MODALITIES:
                modality = result["modality"]
            if not brand_tier and result.get("brand_tier"):
                brand_tier = result["brand_tier"]
            if not brand_tier and result.get("location_count"):
                brand_tier = brand_tier_from_count(result["location_count"])

    return {
        "modality": modality or "Other",
        "brand_tier": brand_tier or "",
        "tier": 3,
        "method": "deep_scrape+haiku",
    }
