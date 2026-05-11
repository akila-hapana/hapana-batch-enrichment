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
from enrichment import tier1, tier2, tier3

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Global state — single-worker Cloud Run, one batch at a time
_event_queue: queue.Queue = queue.Queue()
_batch_thread: threading.Thread | None = None
_stop_flag = threading.Event()

GCP_PROJECT = os.environ.get("GCP_PROJECT", "verdant-wave-440404-g9")
HUBSPOT_BATCH_LIST_ID = os.environ.get("HUBSPOT_BATCH_LIST_ID", "23763")  # Batch 1 — 10 companies


def get_db():
    return firestore.Client(project=GCP_PROJECT)


def emit(event: dict):
    """Push SSE event to the queue."""
    _event_queue.put(event)


# ---------------------------------------------------------------------------
# Enrichment pipeline
# ---------------------------------------------------------------------------

def process_company(company: dict) -> dict:
    """Run a company through Tier 1 → 2 → 3, return final result."""
    name = company.get("name", "")
    cid = company["id"]

    emit({"type": "company_start", "id": cid, "name": name,
          "domain": company.get("domain", "")})

    # Tier 1
    emit({"type": "tier_attempt", "id": cid, "tier": 1, "method": "keyword + head scrape"})
    result = tier1.enrich(company)

    if result and result.get("modality") and result.get("brand_tier"):
        result["id"] = cid
        return result

    # Tier 2
    emit({"type": "tier_attempt", "id": cid, "tier": 2, "method": "Apollo + locations scrape + Haiku"})
    result2 = tier2.enrich(company, tier1_result=result)

    if result2 and result2.get("modality") and result2.get("brand_tier"):
        result2["id"] = cid
        return result2

    # Tier 3
    emit({"type": "tier_attempt", "id": cid, "tier": 3, "method": "deep scrape + Haiku"})
    result3 = tier3.enrich(company, previous=result2 or result)
    result3["id"] = cid
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

                # Write back to HubSpot
                ok = hubspot_client.write_enrichment(cid, modality, brand_tier)

                # Save to Firestore
                db.collection("enrichment_batches").document(batch_id)\
                  .collection("companies").document(cid).set({
                      "name": company["name"],
                      "domain": company["domain"],
                      "modality": modality,
                      "brand_tier": brand_tier,
                      "tier": result.get("tier"),
                      "method": result.get("method"),
                      "hubspot_written": ok,
                      "enriched_at": firestore.SERVER_TIMESTAMP,
                  })

                emit({"type": "company_done", "id": cid, "name": company["name"],
                      "domain": company.get("domain", ""),
                      "modality": modality, "brand_tier": brand_tier,
                      "tier": result.get("tier"), "method": result.get("method"),
                      "hubspot_written": ok})
                enriched_count += 1

            except Exception as e:
                log.exception(f"Failed to enrich {company['name']}: {e}")
                emit({"type": "company_failed", "id": cid, "name": company["name"],
                      "reason": str(e)[:100]})
                failed_count += 1

        # Progress event
        emit({"type": "progress", "current": i + 1, "total": len(companies),
              "enriched": enriched_count, "failed": failed_count})

        batch_ref.update({"enriched": enriched_count, "failed": failed_count})
        time.sleep(0.3)  # Polite rate limiting

    batch_ref.update({"status": "complete", "completed_at": firestore.SERVER_TIMESTAMP})
    emit({"type": "batch_done", "total": len(companies),
          "enriched": enriched_count, "failed": failed_count})


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
