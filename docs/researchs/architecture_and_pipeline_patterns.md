# Event-Driven Document Ingestion Architecture & Pipeline Patterns

## Executive Summary
This document defines the production-ready, event-driven architectural patterns for ingesting unstructured documents from Microsoft SharePoint and Atlassian Confluence into an Enterprise Data Lakehouse (Delta Lake / Apache Iceberg).

It addresses real-world engineering challenges: **out-of-order event delivery, dropped webhooks, mass-burst throttling, credential renewal, and late-binding security enforcement.**

---

## 1. End-to-End Event Ingress Topology

```mermaid
flowchart TD
    subgraph Sources ["Event Sources"]
        SP_Hub["SharePoint (Graph) via Azure Event Hubs"]:::purple
        SP_Hook["SharePoint (Graph) via HTTPS Webhook"]:::purple
        CF_EB["Confluence via Amazon EventBridge"]:::purple
        CF_Hook["Confluence via Webhook / Forge"]:::purple
    end

    subgraph Ingress ["Ingress & Ingestion Buffer"]
        Gateway["API Gateway / Webhook Receiver\n(Signature Validation & Handshake)"]:::purple
        Buffer["Distributed Message Queue / Log\n(Kafka / Azure Event Hubs / AWS SQS)\nPartitioned by Tenant & Container ID"]:::amber
    end

    subgraph Workers ["Ingestion & Transformation Workers"]
        CrawlWorker["Discovery & Fetch Worker\n(Delta API / Stream Blob to S3/ADLS)"]:::blue
        ExtractWorker["Parsing & OCR Worker\n(Markdown / Docling / Unstructured)"]:::cyan
        ChunkWorker["Semantic Chunking & Embedding Worker\n(Hash Cache Check / LLM Embedding)"]:::cyan
    end

    subgraph Lakehouse ["Enterprise Data Lakehouse"]
        Bronze[("Bronze Layer\n(Raw Blobs + Metadata Delta Table)")]:::green
        Silver[("Silver Layer\n(Clean Markdown + Chunks + Normalized ACLs)")]:::green
        Gold[("Gold Layer\n(Vector Search Index + RLS Secured Views)")]:::green
        StateDB[("Watermark & State Store\n(Delta Tokens & Subscription Metadata)")]:::green
    end

    SP_Hub --> Buffer
    CF_EB --> Buffer
    SP_Hook --> Gateway
    CF_Hook --> Gateway
    Gateway --> Buffer

    Buffer --> CrawlWorker
    CrawlWorker --> Bronze
    CrawlWorker --> StateDB
    Bronze --> ExtractWorker
    ExtractWorker --> Silver
    Silver --> ChunkWorker
    ChunkWorker --> Gold

    %% Subgraphs: Clean transparent bounding boxes with distinct slate borders
    style Sources fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Ingress fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Workers fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Lakehouse fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569

    %% High-Visibility Link Arrows (Vivid Cobalt Blue)
    linkStyle default stroke:#0284c7,stroke-width:2px

    %% High-Luminance Card Palette (WCAG AAA Compliant: Crisp #0f172a Text on #ffffff Tiles)
    classDef default fill:#ffffff,stroke:#475569,stroke-width:2.5px,color:#0f172a;
    classDef blue    fill:#ffffff,stroke:#2563eb,stroke-width:2.5px,color:#0f172a;
    classDef green   fill:#ffffff,stroke:#059669,stroke-width:2.5px,color:#0f172a;
    classDef amber   fill:#ffffff,stroke:#d97706,stroke-width:2.5px,color:#0f172a;
    classDef purple  fill:#ffffff,stroke:#7c3aed,stroke-width:2.5px,color:#0f172a;
    classDef red     fill:#ffffff,stroke:#dc2626,stroke-width:2.5px,color:#0f172a;
    classDef cyan    fill:#ffffff,stroke:#0891b2,stroke-width:2.5px,color:#0f172a;
```

---

## 2. Core Architectural Pattern: "Notification-Triggered Delta Sync"

A common architectural antipattern is treating a webhook notification as the complete source of truth and assuming every changed file will have a distinct, successfully delivered webhook.

### Why Webhooks Alone Fail in Production:
1. **Network Glitches**: Webhooks are delivered via HTTP POST ("best effort"). Dropped connections result in permanently lost updates.
2. **Out-of-Order Delivery**: Under high concurrency, an "update" event may arrive *before* a "create" event for the same file.
3. **Mass Bursts**: If an administrator copies a directory containing 10,000 files, the source platform emits 10,000 webhook events simultaneously, creating a severe denial-of-service spike on downstream extraction workers.

### The Solution: Notification as a Wake-Up Signal
* The incoming webhook event is treated solely as a **wake-up trigger** that indicates: *"Container X (Drive or Space) has modifications."*
* The worker locks the container, reads the last committed `@odata.deltaLink` or cursor, and drains the changes via the native **Delta Query API**.

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'primaryColor': '#ffffff',
    'primaryTextColor': '#0f172a',
    'primaryBorderColor': '#2563eb',
    'lineColor': '#0284c7',
    'secondaryColor': '#f8fafc',
    'tertiaryColor': '#ffffff',
    'activationBorderColor': '#2563eb',
    'activationBkgColor': '#e0f2fe',
    'sequenceNumberColor': '#ffffff',
    'actorBkg': '#ffffff',
    'actorBorder': '#2563eb',
    'actorTextColor': '#0f172a',
    'actorLineColor': '#0284c7',
    'signalColor': '#0284c7',
    'signalTextColor': '#0f172a',
    'labelBoxBkgColor': '#ffffff',
    'labelBoxBorderColor': '#d97706',
    'labelTextColor': '#0f172a',
    'loopTextColor': '#0f172a',
    'noteBkgColor': '#fef3c7',
    'noteBorderColor': '#d97706',
    'noteTextColor': '#0f172a'
  }
}}%%
sequenceDiagram
    autonumber
    participant EventBus as Event Broker (SQS/Event Hubs)
    participant Worker as Ingestion Worker
    participant State as State Store (Delta Tokens)
    participant Source as SharePoint / Confluence API
    participant ObjectStore as Cloud Object Store (S3/ADLS)
    participant BronzeTable as Bronze Delta Table

    EventBus->>Worker: Consume Event (DriveId: b!1234, Change: updated)
    Worker->>State: Fetch last DeltaLink for DriveId
    State-->>Worker: Return @odata.deltaLink
    Worker->>Source: GET deltaLink
    Source-->>Worker: Return Page of Changes + new @odata.deltaLink
    loop For Each Item in Delta
        alt Item is Deleted
            Worker->>BronzeTable: Append Tombstone (is_deleted=true)
        else Item Content Modified
            Worker->>ObjectStore: Stream Blob (DownloadUrl -> s3://...)
            Worker->>Worker: Compute SHA-256
            Worker->>BronzeTable: Append Metadata + Raw ACLs + SHA-256
        end
    end
    Worker->>State: Commit new @odata.deltaLink
    Worker->>EventBus: Acknowledge Event
```

---

## 3. Reliability & Operational Guardrails

### A. Graph Subscription Auto-Renewal Daemon
Microsoft Graph Drive subscriptions expire in a maximum of **4,230 minutes (~2.9 days)**.
* **Architecture**: Deploy a lightweight recurring job (e.g., cron on Kubernetes or serverless timer):
  ```python
  # Runs every 6 hours
  def renew_expiring_subscriptions(db_session, graph_client):
      threshold = datetime.utcnow() + timedelta(hours=12)
      expiring_subs = db_session.query(Subscription).filter(Subscription.expires_at < threshold).all()
      for sub in expiring_subs:
          new_expiry = datetime.utcnow() + timedelta(days=2)
          graph_client.patch(f"/subscriptions/{sub.id}", json={
              "expirationDateTime": new_expiry.isoformat() + "Z"
          })
          sub.expires_at = new_expiry
          db_session.commit()
  ```

### B. Daily Reconciliation Sweeper (Self-Healing Ingestion)
To guarantee 100% data consistency even if webhooks fail:
* A scheduled batch sweeper runs every 24 hours.
* It invokes the Delta API using the stored `@odata.deltaLink`.
* If zero changes are returned, the lakehouse is verified in sync.
* If changes are returned, they are drained immediately, healing any dropped webhook events without human intervention.

### C. Tombstone Propagation & Vector Purging
When an item is deleted in SharePoint or Confluence:
1. The Delta Query returns `@removed: {"reason": "deleted"}` or Confluence fires `page_removed`.
2. Bronze table logs a tombstone record:
   `INSERT INTO bronze_documents VALUES (item_id, ..., is_deleted=TRUE, deleted_at=CURRENT_TIMESTAMP)`.
3. Silver pipeline flags all corresponding chunks:
   `UPDATE silver_chunks SET is_active = FALSE WHERE item_id = :item_id`.
4. The Vector Store executor deletes vectors by metadata filter:
   `vector_db.delete(filter={"item_id": item_id})`.
5. *Result*: Zombie chunks are purged immediately, eliminating outdated citations in enterprise RAG.

---

## 4. Reference Event Payload Schemas (Pydantic / Python)

```python
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class UnifiedChangeEvent(BaseModel):
    """Normalized internal representation for all incoming events."""
    event_id: str
    source_system: str  # 'sharepoint' | 'confluence'
    tenant_id: str
    container_id: str  # drive_id or space_key
    item_id: Optional[str] = None
    event_type: str    # 'created' | 'updated' | 'deleted' | 'permissions_changed'
    event_timestamp: datetime
    raw_payload: Dict[str, Any]

class DocumentMetadataRecord(BaseModel):
    """Bronze Layer Metadata Schema."""
    source_system: str
    tenant_id: str
    container_id: str
    item_id: str
    version_id: str
    file_name: str
    mime_type: str
    content_sha256: str
    raw_blob_uri: str
    raw_acls: Dict[str, Any]
    is_deleted: bool = False
    source_modified_at: datetime
    ingestion_timestamp: datetime = Field(default_factory=datetime.utcnow)
```
