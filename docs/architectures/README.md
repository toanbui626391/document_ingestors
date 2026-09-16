# Enterprise Pull-Based Document Ingestion Architecture

## Executive Summary
This architecture documentation suite details the end-to-end design for **pull-based, incremental ingestion** of unstructured enterprise documents and knowledge bases (Microsoft SharePoint, OneDrive, and Atlassian Confluence) into an **Enterprise Data Lakehouse** (Delta Lake / Apache Iceberg).

The ingestors are engineered to fulfill four core operational requirements:
1. **Pull-Based Approach**: Controlled, scheduled polling via native enterprise APIs without requiring inbound public webhooks, open ingress firewall ports, or public IP endpoints.
2. **Incremental Ingestion**: Native delta queries and cursor pagination that track creates, updates, and deletes with zero brute-force full scans.
3. **Mid-Process Restart Without Rework**: Checkpointed state machines, deterministic content-addressable storage paths, and two-phase batch commits that enable workers to crash and resume seamlessly without redundant downloads, parsing, or OCR.
4. **Enterprise Observability**: Integrated OpenTelemetry distributed tracing, Prometheus metrics catalog, structured JSON audit logging, and automated alerting.

---

## Architectural Suite Navigation

| Document | Primary Focus | Key API / Strategy |
| :--- | :--- | :--- |
| [SharePoint Pull Ingestor](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/architectures/sharepoint_pull_ingestor_architecture.md) | SharePoint Document Libraries, Site Lists & OneDrive | Microsoft Graph Delta Query API (`/delta`), `@odata.deltaLink`, `cTag`, `eTag` |
| [Confluence Pull Ingestor](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/architectures/confluence_pull_ingestor_architecture.md) | Confluence Cloud & Data Center Spaces, Pages, Blogposts, Attachments | REST API v2 Cursor Traversal (`sort=-modified-date`), Descending Early-Exit, CQL sliding window |

---

## High-Level System Ingestion Topology

```mermaid
flowchart TD
    subgraph Schedulers ["1. Orchestration & Scheduling Layer"]
        Cron["Kubernetes CronJob / Airflow / Temporal\n(Scheduled Periodic Triggers)"]:::blue
    end

    subgraph Sources ["2. Enterprise Knowledge Sources (Pull-Only)"]
        SP["SharePoint Online / OneDrive\n(Microsoft Graph Delta API)"]:::purple
        CF["Atlassian Confluence Cloud\n(REST API v2 Cursors / CQL)"]:::purple
    end

    subgraph IngestEngine ["3. Ingestion Worker Pool & Reliability Engine"]
        Worker["Ingestion Worker\n(Stateless Container / Pod)"]:::blue
        StateStore[("ACID Checkpoint State Store\n(Cursors, Watermarks, In-Flight Run IDs)")]:::amber
        BlobStreamer["Streaming I/O Engine\n(Direct HTTP to Cloud Storage)"]:::cyan
    end

    subgraph ObjectStore ["4. Cloud Object Storage (Blobs)"]
        RawBuckets[("Cloud Object Storage\n(ADLS Gen2 / AWS S3 / GCS)\nDeterministic: {source}/{tenant}/{id}/{sha256}.{ext}")]:::green
    end

    subgraph Lakehouse ["5. Enterprise Medallion Lakehouse (Delta Lake / Iceberg)"]
        Bronze[("Bronze Delta Table\n(Raw Blobs URI + Audit + Raw ACLs)")]:::green
        Silver[("Silver Delta Table\n(Clean Markdown + Chunks + Normalized Groups)")]:::green
        Gold[("Gold Layer\n(Vector Indexes + Late-Binding RLS Views)")]:::green
    end

    subgraph Monitoring ["6. Observability & Telemetry"]
        Otel["OpenTelemetry Collector\n(Distributed Traces)"]:::cyan
        Prom["Prometheus Server\n(Metrics: Lag, Throughput, 429s)"]:::amber
        Logs["Centralized Logging (Loki / ELK / CloudWatch)\n(Structured JSON with Trace Correlation)"]:::amber
    end

    %% Workflow Connections
    Cron -->|Trigger Run| Worker
    Worker <-->|Read / Commit Checkpoint| StateStore
    Worker -->|GET Delta Changes| SP
    Worker -->|GET Cursor Batch| CF
    Worker -->|Stream File Bits| BlobStreamer
    BlobStreamer -->|Write Blob Data| RawBuckets
    Worker -->|Idempotent ACID Upsert| Bronze
    Bronze --> Silver
    Silver --> Gold

    %% Telemetry Connections
    Worker -.-> Otel
    Worker -.-> Prom
    Worker -.-> Logs

    %% Subgraphs Style
    style Schedulers fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Sources fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style IngestEngine fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style ObjectStore fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Lakehouse fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
    style Monitoring fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569

    %% High-Visibility Link Arrows
    linkStyle default stroke:#0284c7,stroke-width:2px

    %% Dual-Mode Palette Classes
    classDef default fill:#ffffff,stroke:#475569,stroke-width:2.5px,color:#0f172a;
    classDef blue    fill:#ffffff,stroke:#2563eb,stroke-width:2.5px,color:#0f172a;
    classDef green   fill:#ffffff,stroke:#059669,stroke-width:2.5px,color:#0f172a;
    classDef amber   fill:#ffffff,stroke:#d97706,stroke-width:2.5px,color:#0f172a;
    classDef purple  fill:#ffffff,stroke:#7c3aed,stroke-width:2.5px,color:#0f172a;
    classDef red     fill:#ffffff,stroke:#dc2626,stroke-width:2.5px,color:#0f172a;
    classDef cyan    fill:#ffffff,stroke:#0891b2,stroke-width:2.5px,color:#0f172a;
```

---

## Cross-Cutting Architectural Pillars

### 1. Decoupled Storage: Blobs vs. Lakehouse Tables
* **Zero Binary Storage in Tables**: Under no circumstances are raw binary files (PDFs, DOCXs, PPTXs) stored directly in Parquet, Delta Lake, or Iceberg columns (`BLOB`, `BYTEA`).
* **Deterministic Object URIs**: Raw file bitstreams are streamed directly to Cloud Storage under content-addressed paths:
  $$\text{URI} = \text{scheme}://\text{bucket}/\text{source\_system}/\text{tenant\_id}/\text{container\_id}/\text{item\_id}/\{\text{content\_sha256}\}.\text{ext}$$
* **Lakehouse as the Metadata Brain**: Lakehouse tables only store cloud URIs, document headers, ACLs, and extraction states.

### 2. The Mid-Process Restart Engine (Zero-Rework Architecture)
Ingesting large enterprise workspaces involves long-running network operations across tens of thousands of files. Unplanned worker restarts (e.g., Kubernetes node eviction, OOM kill, network socket drop, or scheduled maintenance) must **never** cause the ingestor to re-download, re-parse, or re-embed already processed items.

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
    actor Scheduler as Orchestrator (Airflow / K8s)
    participant Worker as Ingestor Worker Pod
    participant StateDB as ACID Checkpoint Store
    participant Source as Source API (Graph / Confluence)
    participant CloudStore as Cloud Object Storage (S3 / ADLS)
    participant Bronze as Bronze Delta Table

    Scheduler->>Worker: Launch Ingest Task (container_id, run_id=R101)
    Worker->>StateDB: Get Last Checkpoint (container_id)
    StateDB-->>Worker: Return (Active Cursor C1, Completed Items Set)

    Worker->>Source: GET Batch (cursor=C1, limit=100)
    Source-->>Worker: Return 100 Items + nextCursor=C2

    loop For each item in batch
        alt Blob already exists in CloudStore with identical SHA-256
            Worker->>Worker: Skip Download (FinOps Cache Hit)
        else Blob is new or modified
            Worker->>CloudStore: Stream bits (HTTP -> S3/ADLS) & Compute SHA-256
            CloudStore-->>Worker: Storage Commit ACK
        end
    end

    Worker->>Bronze: Atomic Batch Upsert (100 metadata records + raw ACLs)
    Bronze-->>Worker: Commit ACK (Delta version N)

    Worker->>StateDB: Atomic Checkpoint Commit (cursor=C2, stage=COMMITTED, run_id=R101)
    StateDB-->>Worker: Checkpoint ACK

    note over Worker: Worker evicted or restarted here (Crash Simulation)
    Scheduler->>Worker: Restart Ingest Task (container_id, run_id=R102)
    Worker->>StateDB: Get Last Checkpoint (container_id)
    StateDB-->>Worker: Return (cursor=C2, stage=COMMITTED)
    Worker->>Source: Resume from cursor=C2 (ZERO rework for batch C1!)
```

#### The Zero-Rework Mechanics:
1. **Two-Phase Checkpointed Batches**: Cursors/page links are only advanced in the State Store after the entire batch of blobs is uploaded to Cloud Storage and written into the Bronze Delta table.
2. **Pre-flight Checksum Bypass**: Before streaming a file, the worker compares the source's content hash/eTag against the Lakehouse catalog. If identical, binary download and transformation are skipped.
3. **Atomic Watermark Promotion**: High watermarks (e.g., `@odata.deltaLink` or global modification timestamps) are only promoted to the production state store once the entire delta pagination cycle completes successfully.
4. **Idempotent Upserts**: Writing to the Lakehouse uses deterministic primary keys `(source_system, tenant_id, container_id, item_id, version_id)`. Re-executing an interrupted partial batch produces identical, non-duplicative state.

---

### 3. Zero-Trust Access Control (Late Binding)
* **Never Flatten Permissions Prematurely**: Raw ACLs (SharePoint role assignments, Confluence space permissions, and page restrictions) are captured in Bronze untouched.
* **Normalized Groups in Silver**: Security principals are mapped to normalized enterprise identifiers (e.g., Entra ID Group Object IDs) and attached to document chunks.
* **Query-Time Enforcement (Late Binding)**: Permissions are never baked statically into embeddings. Downstream search and RAG queries evaluate user group intersections at query execution time:
  $$\text{Authorized Access} \iff \left( \text{User Evaluated Groups} \cap \text{Chunk Allowed Groups} \neq \emptyset \right)$$

---

### 4. Telemetry & Observability Standard
Every ingestor pod exports real-time telemetry:
* **Prometheus Metrics**: High-frequency operational counters and gauges exposed on `/metrics`.
* **OpenTelemetry Distributed Tracing**: Spans covering API requests, blob streaming throughput, and Lakehouse write latencies.
* **Structured JSON Logs**: Machine-readable logs enriched with `trace_id`, `span_id`, `tenant_id`, `container_id`, `item_id`, and `run_id`.
* **Health Probes**: Kubernetes liveness (`/healthz`) and readiness (`/readyz`) endpoints monitoring state store connectivity and token freshness.
