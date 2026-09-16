# Event-Driven Document Ingestion Research: SharePoint & Confluence

This directory contains research, architectural patterns, and API analysis for building **event-driven document and knowledge base ingestors** that feed an Enterprise Data Lakehouse (Delta Lake / Apache Iceberg).

---

## Research Documents Index

1. **[SharePoint & OneDrive Event-Driven Ingestion](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/researchs/sharepoint_event_driven_apis.md)**
   * Microsoft Graph Change Notifications API (Webhooks)
   * Direct streaming to Azure Event Hubs (private enterprise ingress)
   * Rich notifications with encrypted resource data
   * Microsoft Graph Delta Query API (`@odata.deltaLink`)
   * Throttling, HTTP 429 backoff, and subscription expiration handling

2. **[Confluence Event-Driven Ingestion](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/researchs/confluence_event_driven_apis.md)**
   * Confluence Event Catalog (Pages, Attachments, Spaces, Permissions)
   * Confluence Cloud Automation (AWS EventBridge & Webhooks)
   * Atlassian Forge Event Triggers (`avi:confluence:...`)
   * Confluence Data Center REST Webhooks API
   * Content extraction, ADF/Storage format to Markdown parsing, and page restrictions

3. **[SharePoint & OneDrive Pull-Based Ingestion](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/researchs/sharepoint_pull_based_apis.md)**
   * Microsoft Graph Delta Query API (`/delta`) and `@odata.deltaLink` token persistence
   * Native tombstone detection via `@removed` facet
   * OData temporal filtering (`$filter=lastModifiedDateTime`) and Search (KQL)
   * Direct pre-authenticated binary downloads (`@microsoft.graph.downloadUrl`)
   * Broken ACL inheritance detection (`hasUniqueRoleAssignments`)

4. **[Confluence Pull-Based Ingestion](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/researchs/confluence_pull_based_apis.md)**
   * Confluence REST API v2 Cursor Traversal (`sort=-modified-date` + early-exit algorithm)
   * Confluence Query Language (CQL) incremental sliding windows
   * Parallel bounded slicing for large historical backfills
   * Version number short-circuiting and `SHA-256` content hashing
   * Audit Log APIs for security and permission synchronization

5. **[Architecture & Pipeline Patterns](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/researchs/architecture_and_pipeline_patterns.md)**
   * End-to-end Event Ingress Topology (API Gateway, Queues, Workers, Lakehouse)
   * The "Notification-Triggered Delta Sync" Pattern
   * Operational Guardrails: Auto-renewal daemon, Daily reconciliation sweeper, Tombstone propagation
   * Reference Pydantic Event & Metadata schemas

---

## Feature Comparison Matrix

| Capability / Dimension | Microsoft SharePoint / OneDrive | Atlassian Confluence |
| :--- | :--- | :--- |
| **Primary Event API (Push)** | Microsoft Graph Change Notifications (`POST /subscriptions`) | Atlassian Webhooks / Forge Triggers / Confluence Automation |
| **Primary Pull Ingestion API** | Microsoft Graph Delta Query (`/delta`) | REST API v2 Cursor Traversal & CQL Search API |
| **Incremental Watermarking Strategy** | Opaque `@odata.deltaLink` token | Descending modified date + cursor OR CQL sliding window |
| **Historical Parallel Backfilling** | Partitioning by Drive ID / Folder Subtrees | Parallel Bounded CQL Date/Space Slicing |
| **Cloud-Native Private Ingress** | Native streaming to **Azure Event Hubs** (no public webhook needed) | Native **Amazon EventBridge** integration via Confluence Automation |
| **Encrypted In-Line Payload** | Supported (Rich Notifications with X.509/RSA encryption) | Standard JSON payload (requires token/HMAC validation) |
| **Delta Query / State Link** | Native `@odata.deltaLink` (tracks creates, updates, deletes, moves) | Cursor-based sorting (`/wiki/api/v2/pages?sort=-modified-date`) |
| **Subscription Lifecycle** | **Expires in ~3 days** (requires active `PATCH` renewal daemon) | Persistent (until explicitly unregistered) |
| **Deletions & Tombstones** | Native `@removed: {"reason": "deleted"}` in delta query | Dedicated `page_removed` / `page_trashed` webhook events |
| **Permission Tracking** | `hasUniqueRoleAssignments` + `/permissions` endpoint | Space permissions + Page Read Restrictions (`/restriction/byOperation/read`) |
| **Attachment Handling** | Native DriveItem files in the document library | Separate attachment resources (`/wiki/api/v2/pages/{id}/attachments`) |
| **Rate Limit Standard** | HTTP 429 with mandatory `Retry-After` header | Standard HTTP 429 rate limit backoff |

---

## Related Project Guidelines & Solution Architectures
* **[Solution Architecture Documents](file:///c:/Users/ToanBX/dev/personal/document_ingestors/docs/architectures/README.md)**: Production-grade design specifications for SharePoint & Confluence pull ingestors.
* **[Data Architect Rules for AI Agents](file:///c:/Users/ToanBX/dev/personal/document_ingestors/.agents/rules/data_architect.md)**
* **[Repository Agent Guidelines](file:///c:/Users/ToanBX/dev/personal/document_ingestors/AGENTS.md)**
