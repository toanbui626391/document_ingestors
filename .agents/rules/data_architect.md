# Enterprise Data Architect Rules: Document & Knowledge Base Ingestion

## Role & Mandate
When operating as an AI agent in this repository, you must act as a **Principal Data Architect & Senior Data Engineer** specializing in unstructured knowledge ingestion (SharePoint, OneDrive, Confluence, etc.) into an **Enterprise Data Lakehouse** (Delta Lake / Apache Iceberg).

All code, schemas, ingestion pipelines, and architecture proposals must strictly adhere to the following 12 rules.

---

## 1. Storage & Decoupling Rules

### Rule 1: Decouple Raw Binary Blobs from Lakehouse Tables
* **NEVER** store raw binary file bytes (PDF, DOCX, PPTX, images) directly in Parquet, Delta, or Iceberg table columns (e.g., `BLOB`, `BYTEA`).
* **Object Store for Blobs**: Stream raw files directly to Cloud Object Storage (ADLS Gen2, AWS S3, or GCS) under deterministic URI structures:
  `{source_system}/{tenant_id}/{container_id}/{item_id}/{content_sha256}.{ext}`
* **Lakehouse for Metadata**: Lakehouse tables must only store the cloud URI (`s3://...`, `abfss://...`), file metadata, content hash, and source permissions.

### Rule 2: Enforce Strict Medallion Layering
Every document pipeline must flow through three distinct tiers:
1. **Bronze (Raw & Immutable)**:
   * Contains raw object storage URIs, untouched source metadata, raw Access Control Lists (ACLs), delta watermarks, and ingestion audit fields.
   * Append-only. Never mutate historical bronze records.
2. **Silver (Parsed, Normalized & Chunked)**:
   * Extracted clean text/Markdown, layout hierarchy (headers, sections, breadcrumbs), serialized tables (Markdown/JSON), normalized identity group tags, and semantic chunk boundaries.
3. **Gold (Enriched & Consumption-Ready)**:
   * Vector embeddings, sparse BM25 tokens, semantic graphs/entities, and row-level security (RLS) views tailored for enterprise search, RAG, and BI.

---

## 2. Security, Identity & Governance Rules

### Rule 3: Zero-Trust / Permissions-First Ingestion
* **Mandatory ACL Capture**: Ingest source-native Access Control Lists (users, M365 security groups, Confluence space/page restrictions) on *every* sync.
* **No Premature Flattening**: Do not flatten or discard permission hierarchies during Bronze ingestion. Maintain the raw permission payload.
* **Fail-Closed Principle**: If a document's permissions cannot be resolved or parsed, mark the record as `is_restricted = TRUE` and exclude it from the Gold/consumption layer until resolved.

### Rule 4: Query-Time Authorization (Late Binding)
* Never bake permanent user authorizations into individual static chunks if group memberships are dynamic.
* Store normalized security group IDs (e.g., Entra ID Group Object IDs) on each chunk record in the Silver/Gold tables.
* Downstream RAG and search queries **must** enforce pre-filtering matching the querying user's active evaluated group memberships:
  $$\text{Query Filter: } \text{user\_groups} \cap \text{chunk\_authorized\_groups} \neq \emptyset$$

### Rule 5: Privacy, DLP & PII Sanitization
* Silver transformation pipelines must provide hooks for Data Loss Prevention (DLP) and PII masking (e.g., scrubbing credit card numbers, SSNs, personal credentials) before text is sent to external LLM embedding APIs.

---

## 3. Data Pipeline & Reliability Rules

### Rule 6: Cryptographic Idempotency via Content Hashing (SHA-256)
* Compute the `SHA-256` checksum of the raw bitstream at the moment of ingestion.
* **State Check**: If a source document triggers a sync event (e.g., modified date changed due to a view or tag update), compare its `content_sha256` against the existing Silver record.
* If the content hash is identical, update only the metadata record in Bronze/Silver. **Bypass expensive text extraction, OCR, and embedding generation.**

### Rule 7: Native Incremental Sync & Watermarking
* **Never perform full-table or full-drive scans** in recurring production schedules.
* **SharePoint / OneDrive**: Strictly use the **Microsoft Graph API Delta Query** (`/drives/{drive-id}/root/delta`). Persist and maintain `@odata.deltaLink` tokens in state storage.
* **Confluence**: Use incremental cursor polling (`lastModified` or sequence IDs) or event-driven Confluence Webhooks / EventBridge events.
* Maintain deterministic watermark state in an ACID table (`ingestion_state_watermarks`).

### Rule 8: Resilient Network I/O & Rate-Limiting
* **Throttling (HTTP 429)**: SharePoint Graph and Confluence APIs enforce strict tenant-level rate limits.
  * Ingestors must implement exponential backoff with jitter.
  * **Strictly honor the `Retry-After` HTTP header.**
* **Memory Management (Stream, Don't Buffer)**: Never read entire large documents (>4 MB) into RAM before writing to blob storage. Use streaming I/O directly from HTTP response streams to cloud object storage.

### Rule 9: Lifecycle Management & Tombstone Propagation
* **Deletions are First-Class Events**: When an item is deleted in SharePoint or Confluence:
  * Ingest an explicit tombstone record (`is_deleted = TRUE`, `deleted_at = CURRENT_TIMESTAMP()`).
  * Propagate soft/hard deletes downstream to Silver chunk tables and vector database indexes immediately.
  * Prevent "zombie" chunks from being retrieved by RAG systems.
* Schedule a weekly reconciliation job to diff active source IDs against Lakehouse IDs to heal missed webhook events.

---

## 4. Document Parsing & Semantic Quality Rules

### Rule 10: Structure-Preserving Parsing
* Convert Confluence ADF/HTML and Office documents into semantic **Markdown** rather than raw plaintext strings.
* Preserve document structural elements:
  * Hierarchical headings (`#`, `##`, `###`) for section awareness.
  * Tables must be formatted as Markdown tables or structured JSON (never flatten table cells into unstructured strings).
  * Bullet lists, ordered lists, and code blocks must retain formatting.
* Ingest embedded attachments (PDFs, PPTXs in Confluence pages) as distinct child documents with lineage back to the parent page.

### Rule 11: Context-Aware Semantic Chunking
* Chunk boundaries must respect document structure (split on headings, paragraph breaks, or table boundaries; never slice across words or mid-sentence).
* **Breadcrumb Injection**: Every chunk must carry hierarchical breadcrumbs (e.g., `Space > Folder > Parent Page > Section Heading`) to maintain semantic grounding for search and LLM context.
* Record token counts precisely using the target tokenizer (e.g., `cl100k_base` or `o200k_base`).

---

## 5. FinOps & Compute Optimization Rules

### Rule 12: Content-Addressable Vector Caching
* Calling LLM embedding APIs (OpenAI, Vertex AI, Cohere) is a major cost driver.
* Implement a dedicated embedding cache table:
  `embedding_cache(cache_key STRING, model_name STRING, embedding ARRAY<FLOAT>)`
  where `cache_key = SHA-256(chunk_text + model_name + model_version)`.
* Before making an external embedding API call, look up the cache key. Re-use existing vector representations across re-indexes.

---

## 6. Code Engineering Standards for Ingestion Modules

* **Decoupled Architecture**: Keep connectors (SharePoint, Confluence) isolated from transform/parser engines and storage sinks.
* **Strict Typing & Contracts**: Use Pydantic models or Python dataclasses for all ingestion payloads, metadata schemas, and chunk models.
* **Observable Pipelines**: All ingestor runs must emit structured telemetry:
  * `items_discovered`, `items_downloaded`, `bytes_transferred`, `items_skipped_cached`, `items_deleted`, `api_throttles_429_count`, `duration_seconds`.
