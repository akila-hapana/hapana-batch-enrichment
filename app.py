"""
Batch Data Enrichment App
Real-time company enrichment with 3-tier classification pipeline.
"""
import os
import json
import queue
import threading
import time
import logging
from flask import Flask, Response, render_template, request, jsonify
import google.cloud.firestore as firestore

from enrichment import hubspot_client
from enrichment import tier0, tier1, tier2, tier3

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Global state — single-worker Cloud Run, one batch at a time
_event_queue: queue.Queue = queue.Queue()
_batch_thread: threading.Thread | None = None
_stop_flag = threading.Event()

GCP_PROJECT = os.environ.get("FIRESTORE_PROJECT", "verdant-wave-440404-g9")
HUBSPOT_BATCH_LIST_ID = os.environ.get("HUBSPOT_BATCH_LIST_ID", "23812")  # Batch 2 — 100 companies


def get_db():
    return firestore.Client(project=GCP_PROJECT)


def emit(event: dict):
    """Push SSE event to queue and log to terminal stream."""
    _event_queue.put(event)


def log_terminal(message: str, level: str = "info"):
    """Emit a terminal log line visible in the UI log panel."""
    import time as _time
    emit({"type": "log", "level": level, "message": message,
          "ts": _time.strftime("%H:%M:%S")})


# ---------------------------------------------------------------------------
# Enrichment pipeline
# ---------------------------------------------------------------------------

CONFIDENCE_THRESHOLD = 90


def process_company(company: dict) -> dict:
    """
    T0 collects all data (scraping + location + Maps + Apollo).
    T1/T2/T3 only interpret — no HTTP calls inside any tier.
    """
    name   = company.get("name", "")
    domain = company.get("domain", "")
    cid    = company["id"]

    emit({"type": "company_start", "id": cid, "name": name, "domain": domain})
    log_terminal(f"── Starting: {name} ({domain or 'no domain'})")

    # ── Tier 0: collect everything ────────────────────────────────────────────
    emit({"type": "tier_attempt", "id": cid, "tier": 0,
          "method": "domain check · 4-stage scrape · location extract · Maps · Apollo"})
    log_terminal(f"[T0] Domain validation + scraping + location data collection...")

    t0 = tier0.collect(company)

    if t0.get("_skip"):
        reason = t0.get("skip_reason", "unknown")
        log_terminal(f"[T0] ✗ Skipping — {reason}", "error")
        return {"modality": "Other", "brand_tier": "",
                "modality_confidence": 0, "brand_tier_confidence": 0,
                "cost_usd": 0.0, "tier": 0, "method": "skipped_invalid_domain",
                "scrape_stage": 0, "scrape_queued": False, "id": cid}

    if t0.get("_domain_corrected"):
        log_terminal(f"[T0] Domain corrected: {t0.get('_original_domain', domain)} → {t0['domain']}", "warn")

    stage_labels = {0: "no content", 1: "static HTML", 2: "Jina Reader",
                    3: "Playwright headless", 4: "local Chrome"}
    scrape_stage  = t0["scrape_stage"]
    scrape_queued = t0["scrape_queued"]
    scraped_text  = t0["scraped_text"]

    if scraped_text:
        log_terminal(
            f"[T0] ✓ Scrape S{scrape_stage} ({stage_labels.get(scrape_stage,'?')}) "
            f"— {len(scraped_text)} chars", "success")
    elif scrape_queued:
        log_terminal(f"[T0] Local machine offline — queued for Stage 4", "warn")
    else:
        log_terminal(f"[T0] All scrape stages returned no content — name-only", "warn")

    loc = t0.get("location_count")
    maps = t0.get("maps_count")
    if loc is not None:
        log_terminal(f"[T0] Website location count: {loc}")
    if maps is not None:
        log_terminal(f"[T0] Google Maps listings: {maps}")
    if t0.get("apollo_industry"):
        log_terminal(f"[T0] Apollo industry: {t0['apollo_industry']}")

    emit({"type": "scrape_done", "id": cid, "stage": scrape_stage,
          "queued": scrape_queued, "chars": len(scraped_text)})

    total_cost = t0.get("cost_usd", 0.0)   # Maps API cost already tracked

    # ── Tier 1: keyword classification ───────────────────────────────────────
    emit({"type": "tier_attempt", "id": cid, "tier": 1,
          "method": "known brand lookup · keyword match · T0 location data"})
    log_terminal(f"[T1] Keyword matching on pre-scraped content...")

    result1 = tier1.enrich(t0)

    if result1:
        mc = result1.get("modality_confidence", 0)
        tc = result1.get("brand_tier_confidence", 0)
        log_terminal(f"[T1] → {result1.get('modality')} ({mc}%) · "
                     f"{result1.get('brand_tier')} ({tc}%) via {result1.get('method')}")
        # Build human-readable reasoning for T1 (rule-based, no AI reasoning returned)
        if result1.get("method") == "known_brand":
            result1["reasoning"] = (f"Exact domain match in the known brand database — "
                                    f"modality and brand tier are pre-confirmed.")
        else:
            lc = result1.get("location_count")
            mc_maps = t0.get("maps_count")
            result1["reasoning"] = (f"Strong keyword match in company name or scraped website content. "
                                    f"Location data: website={lc}, Google Maps={mc_maps} → {result1.get('brand_tier')}.")
        if mc >= CONFIDENCE_THRESHOLD and tc >= CONFIDENCE_THRESHOLD:
            log_terminal(f"[T1] ✓ Resolved at Tier 1", "success")
            result1["id"]           = cid
            result1["cost_usd"]     = total_cost
            result1["scrape_stage"] = scrape_stage
            result1["scrape_queued"]= scrape_queued
            result1["maps_count"]   = t0.get("maps_count")
            result1["apollo_industry"] = t0.get("apollo_industry", "")
            if result1.get("location_count") is None:
                result1["location_count"] = t0.get("location_count")
            return result1
        log_terminal(f"[T1] Confidence too low — escalating to T2", "warn")
    else:
        log_terminal(f"[T1] No strong keyword match — escalating to T2", "warn")

    # ── Tier 2: Gemini ───────────────────────────────────────────────────────
    emit({"type": "tier_attempt", "id": cid, "tier": 2,
          "method": "Gemini 1.5 Flash (Vertex AI) · T0 context"})
    log_terminal(f"[T2] Sending T0 context to Gemini 1.5 Flash...")

    result2 = tier2.enrich(t0, tier1_result=result1)

    if result2 and not result2.get("_partial"):
        mc = result2.get("modality_confidence", 0)
        tc = result2.get("brand_tier_confidence", 0)
        total_cost += result2.get("cost_usd", 0.0)
        log_terminal(f"[T2] Gemini → {result2.get('modality')} ({mc}%) · "
                     f"{result2.get('brand_tier')} ({tc}%)")
        if mc >= CONFIDENCE_THRESHOLD and tc >= CONFIDENCE_THRESHOLD:
            log_terminal(f"[T2] ✓ Resolved at Tier 2", "success")
            result2["id"]           = cid
            result2["cost_usd"]     = total_cost
            result2["scrape_stage"] = scrape_stage
            result2["scrape_queued"]= scrape_queued
            result2["maps_count"]   = t0.get("maps_count")
            result2["apollo_industry"] = t0.get("apollo_industry", "")
            if result2.get("location_count") is None:
                result2["location_count"] = t0.get("location_count")
            return result2
        log_terminal(f"[T2] Reasoning: {result2.get('reasoning','')}", "muted")
        log_terminal(f"[T2] Below threshold — escalating to T3", "warn")
    else:
        if result2:
            total_cost += result2.get("cost_usd", 0.0)
        log_terminal(f"[T2] No usable result — escalating to T3", "warn")

    # ── Tier 3: Claude Haiku ─────────────────────────────────────────────────
    emit({"type": "tier_attempt", "id": cid, "tier": 3,
          "method": "Claude Haiku 4.5 · T0 content"})
    log_terminal(f"[T3] Sending to Claude Haiku for final classification...")

    result3 = tier3.enrich(t0, previous=result2 or result1)
    total_cost += result3.get("cost_usd", 0.0)

    mc = result3.get("modality_confidence", 0)
    tc = result3.get("brand_tier_confidence", 0)

    if mc >= CONFIDENCE_THRESHOLD and tc >= CONFIDENCE_THRESHOLD:
        log_terminal(f"[T3] ✓ Haiku → {result3.get('modality')} ({mc}%) · "
                     f"{result3.get('brand_tier')} ({tc}%)", "success")
    else:
        log_terminal(f"[T3] ✗ Below threshold — modality=Other, brand_tier=blank", "error")

    result3["id"]              = cid
    result3["cost_usd"]        = total_cost
    result3["scrape_stage"]    = scrape_stage
    result3["scrape_queued"]   = scrape_queued
    result3["maps_count"]      = t0.get("maps_count")
    result3["apollo_industry"] = t0.get("apollo_industry", "")
    if result3.get("location_count") is None:
        result3["location_count"] = t0.get("location_count")
    return result3


def run_batch(companies: list[dict], batch_id: str):
    """Background thread: process all companies, emit events, write to HubSpot + Firestore."""
    db = get_db()
    batch_ref = db.collection("enrichment_batches").document(batch_id)
    batch_ref.set({
        "status": "running",
        "total": len(companies),
        "enriched": 0,
        "failed": 0,
        "started_at": firestore.SERVER_TIMESTAMP,
    })

    emit({"type": "batch_started", "batch_id": batch_id, "total": len(companies),
          "companies": [{"id": c["id"], "name": c["name"], "domain": c["domain"]} for c in companies]})

    enriched_count = 0
    failed_count = 0
    total_cost = 0.0

    for i, company in enumerate(companies):
        if _stop_flag.is_set():
            emit({"type": "batch_stopped", "processed": i, "enriched": enriched_count})
            break

        cid = company["id"]

        # Skip if already has both properties
        if company.get("existing_modality") and company.get("existing_brand_tier"):
            emit({"type": "company_done", "id": cid, "name": company["name"],
                  "modality": company["existing_modality"],
                  "brand_tier": company["existing_brand_tier"],
                  "tier": 0, "method": "already_enriched", "skipped": True})
            enriched_count += 1
        else:
            try:
                result = process_company(company)
                modality = result.get("modality", "Other")
                brand_tier = result.get("brand_tier") or ""
                cost = result.get("cost_usd", 0.0)
                total_cost += cost

                # Write back to HubSpot
                ok = hubspot_client.write_enrichment(cid, modality, brand_tier)

                # Save to Firestore — full audit trail (HubSpot only gets modality + brand_tier)
                db.collection("enrichment_batches").document(batch_id)\
                  .collection("companies").document(cid).set({
                      "name": company["name"],
                      "domain": company["domain"],
                      "modality": modality,
                      "brand_tier": brand_tier,
                      "tier": result.get("tier"),
                      "method": result.get("method"),
                      "business_model": result.get("business_model", ""),
                      "reasoning": result.get("reasoning", ""),
                      "modality_confidence": result.get("modality_confidence", 0),
                      "brand_tier_confidence": result.get("brand_tier_confidence", 0),
                      "location_count": result.get("location_count"),
                      "maps_count": result.get("maps_count"),
                      "apollo_industry": result.get("apollo_industry", ""),
                      "scrape_stage": result.get("scrape_stage", 0),
                      "hubspot_written": ok,
                      "enriched_at": firestore.SERVER_TIMESTAMP,
                  })

                emit({"type": "company_done", "id": cid, "name": company["name"],
                      "domain": company.get("domain", ""),
                      "modality": modality, "brand_tier": brand_tier,
                      "tier": result.get("tier"), "method": result.get("method"),
                      "cost_usd": round(cost, 6),
                      "scrape_stage": result.get("scrape_stage", 0),
                      "scrape_queued": result.get("scrape_queued", False),
                      "hubspot_written": ok,
                      "reasoning": result.get("reasoning", ""),
                      "modality_confidence": result.get("modality_confidence", 0),
                      "brand_tier_confidence": result.get("brand_tier_confidence", 0),
                      "location_count": result.get("location_count"),
                      "maps_count": result.get("maps_count"),
                      "apollo_industry": result.get("apollo_industry", "")})
                enriched_count += 1

            except Exception as e:
                log.exception(f"Failed to enrich {company['name']}: {e}")
                emit({"type": "company_failed", "id": cid, "name": company["name"],
                      "reason": str(e)[:100]})
                failed_count += 1

        # Progress event
        emit({"type": "progress", "current": i + 1, "total": len(companies),
              "enriched": enriched_count, "failed": failed_count,
              "total_cost": round(total_cost, 6)})

        batch_ref.update({"enriched": enriched_count, "failed": failed_count})
        time.sleep(0.3)  # Polite rate limiting

    batch_ref.update({"status": "complete", "completed_at": firestore.SERVER_TIMESTAMP})
    emit({"type": "batch_done", "total": len(companies),
          "enriched": enriched_count, "failed": failed_count,
          "total_cost": round(total_cost, 6)})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html",
                           batch_list_id=HUBSPOT_BATCH_LIST_ID)


@app.route("/load-batch")
def load_batch():
    """Return the list of companies to enrich."""
    companies = hubspot_client.get_list_companies(HUBSPOT_BATCH_LIST_ID)
    return jsonify({"companies": companies, "total": len(companies)})


@app.route("/start", methods=["POST"])
def start():
    global _batch_thread
    if _batch_thread and _batch_thread.is_alive():
        return jsonify({"error": "Batch already running"}), 409

    _stop_flag.clear()
    # Drain old events
    while not _event_queue.empty():
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break

    data = request.get_json(silent=True) or {}
    companies = data.get("companies")

    if not companies:
        companies = hubspot_client.get_list_companies(HUBSPOT_BATCH_LIST_ID)

    batch_id = f"batch-{int(time.time())}"
    _batch_thread = threading.Thread(target=run_batch, args=(companies, batch_id), daemon=True)
    _batch_thread.start()

    return jsonify({"status": "started", "batch_id": batch_id, "total": len(companies)})


@app.route("/stop", methods=["POST"])
def stop():
    _stop_flag.set()
    return jsonify({"status": "stopping"})


@app.route("/reset-batch", methods=["POST"])
def reset_batch():
    """Clear modality + brand_tier for all companies in the current batch list."""
    companies = hubspot_client.get_list_companies(HUBSPOT_BATCH_LIST_ID)
    ids = [c["id"] for c in companies]
    cleared = hubspot_client.clear_enrichment(ids)
    return jsonify({"status": "cleared", "count": cleared})


@app.route("/stream")
def stream():
    """SSE endpoint — browser connects here for real-time events."""
    def generate():
        yield "data: {\"type\":\"connected\"}\n\n"
        while True:
            try:
                event = _event_queue.get(timeout=20)
                yield f"data: {json.dumps(event)}\n\n"
            except queue.Empty:
                yield "data: {\"type\":\"heartbeat\"}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False, threaded=True)
