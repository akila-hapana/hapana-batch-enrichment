"""HubSpot API client — read companies from a list, write back enrichment results."""
import os
import time
import requests

HUBSPOT_KEY = os.environ.get("HUBSPOT_API_KEY", "")
BASE = "https://api.hubapi.com"


def _headers():
    return {"Authorization": f"Bearer {HUBSPOT_KEY}", "Content-Type": "application/json"}


def get_list_companies(list_id: str) -> list[dict]:
    """Return all companies in a HubSpot list with name, domain, website."""
    ids = []
    after = None
    while True:
        params = {"limit": 250}
        if after:
            params["after"] = after
        r = requests.get(f"{BASE}/crm/v3/lists/{list_id}/memberships",
                         headers=_headers(), params=params)
        data = r.json()
        ids.extend([x["recordId"] for x in data.get("results", [])])
        nxt = data.get("paging", {}).get("next", {}).get("after")
        if not nxt:
            break
        after = nxt
        time.sleep(0.05)

    companies = []
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/batch/read",
            headers=_headers(),
            json={"inputs": [{"id": cid} for cid in batch],
                  "properties": ["name", "domain", "website", "modality", "brand_tier"]},
        )
        for obj in r.json().get("results", []):
            p = obj.get("properties", {})
            companies.append({
                "id": obj["id"],
                "name": p.get("name") or "",
                "domain": p.get("domain") or "",
                "website": p.get("website") or "",
                "existing_modality": p.get("modality") or "",
                "existing_brand_tier": p.get("brand_tier") or "",
            })
        time.sleep(0.1)

    return companies


def clear_enrichment(company_ids: list[str]) -> int:
    """Clear modality + brand_tier for a list of company IDs. Returns count cleared."""
    cleared = 0
    for i in range(0, len(company_ids), 100):
        batch = company_ids[i:i + 100]
        r = requests.post(
            f"{BASE}/crm/v3/objects/companies/batch/update",
            headers=_headers(),
            json={"inputs": [
                {"id": cid, "properties": {"modality": "", "brand_tier": ""}}
                for cid in batch
            ]},
        )
        if r.status_code in (200, 207):
            cleared += len(batch)
        time.sleep(0.1)
    return cleared


def write_enrichment(company_id: str, modality: str, brand_tier: str) -> bool:
    """Write modality + brand_tier back to a HubSpot company."""
    props = {}
    if modality:
        props["modality"] = modality
    if brand_tier:
        props["brand_tier"] = brand_tier
    if not props:
        return False

    r = requests.patch(
        f"{BASE}/crm/v3/objects/companies/{company_id}",
        headers=_headers(),
        json={"properties": props},
    )
    return r.status_code in (200, 204)
