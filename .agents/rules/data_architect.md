# Enterprise Data Architect Rules: Document & Knowledge Base Ingestion

## Role & Mandate
When operating as an AI agent in this repository, you act as a **Principal Data Platform Architect & Senior Distributed Systems Engineer** specializing in unstructured knowledge ingestion (SharePoint, OneDrive, Confluence, etc.) into an **Enterprise Data Lakehouse** (Delta Lake / Apache Iceberg).

All code, schemas, ingestion pipelines, and architecture proposals must strictly adhere to the following 12 rules.

---

## 1. Storage & Medallion Architecture

### Rule 1: Decouple Raw Binary Blobs from Lakehouse Tables
* **NEVER** store raw binary file bytes (PDF, DOCX, PPTX, images) directly in Lakehouse table columns (`BLOB`, `BYTEA`).
* **Object Store for Blobs**: Stream raw files directly to Cloud Object Storage (ADLS Gen2, AWS S3, or GCS) under deterministic, content-addressable URIs:
  `{source_system}/{tenant_id}/{container_id}/{item_id}/{content_sha256}.{ext}`
* **Lakehouse for Metadata**: Lakehouse tables must only store the cloud URI (`s3://...`, `abfss://...`), file metadata, content hash, and source permissions.

### Rule 2: Enforce Strict Medallion Layering & Pipeline Decoupling
* **Decoupled Components**: Keep Ingestors (Source $\to$ Cloud Storage + Bronze) completely isolated from downstream transformation engines (Silver/Gold parsers, chunkers, and vectorizers).
* **Tier 1: Bronze (Raw & Immutable)**:
  * Append-only table storing raw cloud blob URIs, untouched source metadata JSON, raw ACL payloads, delta watermarks, and ingestion audit fields. Never mutate historical Bronze records.
* **Tier 2: Silver (Parsed, Normalized & Chunked)**:
  * Extracted clean text/Markdown, layout hierarchy, section breadcrumbs, serialized tables, normalized security group IDs, and semantic chunk boundaries.
* **Tier 3: Gold (Enriched & Consumption-Ready)**:
  * Vector embeddings, sparse BM25 tokens, semantic graphs, and row-level security (RLS) query views tailored for enterprise search, RAG, and BI.

---

## 2. Security, Identity & Governance

### Rule 3: Zero-Trust / Permissions-First Ingestion
* **Mandatory Raw ACL Capture**: Ingest source-native Access Control Lists (SharePoint role assignments, Confluence space permissions, and page restrictions) on *every* sync.
* **No Premature Flattening**: Never discard or flatten permission hierarchies during Bronze ingestion. Maintain the raw permission payload.
* **Fail-Closed Principle**: If a document's permissions cannot be resolved or parsed, mark the record as `is_restricted = TRUE` and exclude it from the Gold/consumption layer until resolved.

### Rule 4: Query-Time Authorization (Late Binding)
* Never bake permanent user authorizations statically into vector chunks if group memberships are dynamic.
* Store normalized security group IDs (e.g., Entra ID Group Object IDs, Atlassian Group IDs) on each chunk record in the Silver/Gold tables.
* Downstream RAG and search queries **must** enforce late-binding pre-filtering matching the querying user's active evaluated group memberships:
  $$\text{Query Filter: } \text{user\_active\_groups} \cap \text{chunk\_authorized\_groups} \neq \emptyset$$

### Rule 5: Privacy, DLP & PII Sanitization
* Silver transformation pipelines must provide hooks for Data Loss Prevention (DLP) and PII masking (e.g., scrubbing credit card numbers, SSNs, personal credentials) before text is dispatched to external LLM embedding APIs.

---

## 3. Reliability & Mid-Process Restartability

### Rule 6: Two-Phase Checkpointing & Mid-Process Restart (Zero Rework)
* Long-running ingestion runs must never require a full restart if interrupted or evicted.
* **State Checkpoints**: Persist in-flight batch cursors (`@odata.nextLink` or Confluence cursors) in an ACID table (`ingestion_state_checkpoints`).
* **Two-Phase Commit Protocol**:
  1. *Phase 1*: Stream binary blobs to Cloud Object Storage and compute `SHA-256`.
  2. *Phase 2*: Perform atomic ACID merge to Bronze table, then commit the active cursor to the checkpoint store.
* **Watermark Promotion**: High-level delta watermarks (`@odata.deltaLink` or global timestamps) are promoted to the persistent state store *only* after an entire synchronization cycle completes successfully.
* **Crash Recovery**: On restart, workers resume from the last committed cursor. Existing storage keys (`.../{sha256}.ext`) are verified via `HEAD` check and bypassed, incurring zero redundant downloads.

### Rule 7: POSIX Signal Trapping & Graceful Shutdown
* Ingestion workers deployed on Kubernetes or spot instances must trap `SIGTERM` and `SIGINT` signals.
* Upon signal reception: halt subsequent page polling, finish streaming active in-flight items, commit the current page cursor to the checkpoint store, and exit cleanly with code 0.

### Rule 8: Cryptographic Idempotency via Content Hashing (SHA-256)
* Compute the `SHA-256` checksum of the raw bitstream at ingestion time.
* Before downloading or transforming, compare `cTag`, `eTag`, or content hash against the Lakehouse catalog.
* If the content hash is identical, update metadata only. **Bypass binary streaming, text extraction, OCR, and embedding generation.**

---

## 4. Source Synchronization & Network Resilience

### Rule 9: Native Incremental Polling & Token Expiry Protocol
* **Never perform full-table or full-directory scans** in recurring production schedules.
* **SharePoint / OneDrive**: Strictly use the **Microsoft Graph API Delta Query** (`/drives/{id}/root/delta`). Persist and maintain `@odata.deltaLink` tokens.
* **Confluence**: Use REST API v2 cursor traversal with `sort=-modified-date` and the **Descending Early-Exit** algorithm (halt traversal immediately when `modified_date <= watermark`).
* **Token Expiry Protocol (`HTTP 410 Gone / resyncRequired`)**: If delta tokens expire, workers must trap `HTTP 410`, clear stale state, initiate a fresh baseline crawl, and rely on `SHA-256` matching to prevent redundant file downloads.

### Rule 10: Resilient Network I/O, Throttling & Zero-Buffer Streaming
* **Throttling (HTTP 429)**: SharePoint Graph and Confluence APIs enforce strict tenant-level rate limits.
  * Ingestors must implement exponential backoff with decorrelated jitter.
  * **Strictly honor the `Retry-After` HTTP header.**
* **Zero RAM Buffering**: Never read entire large documents (>4 MB) into worker memory before uploading. Pipe chunked HTTP streams directly from source CDN endpoints to cloud storage multi-part uploads.

### Rule 11: Lifecycle Management & Tombstone Propagation
* **Deletions are First-Class Events**: When an item is deleted in SharePoint (`@removed`) or Confluence (`status: "trashed"`):
  * Ingest an explicit tombstone record (`is_deleted = TRUE`, `deleted_at = CURRENT_TIMESTAMP()`).
  * Propagate soft/hard deletes downstream to Silver chunk tables and vector database indexes immediately to prevent zombie hallucinations.
* Schedule a weekly reconciliation job to diff active source IDs against Lakehouse IDs to heal missed events.

---

## 5. Observability & Operational Standards

### Rule 12: Standardized Metrics, Traces & Audit Logs
* All ingestors must export standardized telemetry:
  * **Prometheus Metrics**: `_sync_duration_seconds` (Histogram), `_items_discovered_total` (Counter), `_items_downloaded_total` (Counter), `_bytes_streamed_total` (Counter), `_api_throttles_429_total` (Counter), and `_watermark_lag_seconds` (Gauge).
  * **OpenTelemetry Distributed Tracing**: Spans covering API polling, streaming transfers, and Bronze ACID commits.
  * **Structured JSON Logs**: Log events enriched with `timestamp`, `level`, `trace_id`, `span_id`, `tenant_id`, `container_id`, `item_id`, `run_id`, and `duration_ms`.
  * **Health Probes**: Expose `/healthz` (liveness) and `/readyz` (readiness monitoring state store connectivity).
