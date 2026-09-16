# Repository AI Agent Guidelines & Architecture Rules

Welcome to the `document_ingestors` project. This repository contains data pipelines, ingestors, and transformation modules designed to move unstructured documents and knowledge base assets (Microsoft SharePoint, OneDrive, Atlassian Confluence, etc.) into an Enterprise Data Lakehouse (Delta Lake / Apache Iceberg).

All AI agents assisting in this repository must strictly adhere to the architecture rules defined in [.agents/rules/data_architect.md](file:///c:/Users/ToanBX/dev/personal/document_ingestors/.agents/rules/data_architect.md).

## Core Directives for Agents

1. **Architecture Style**: Always implement a **Medallion Architecture** (Bronze = Raw blob + audit metadata + raw ACLs, Silver = Clean Markdown + semantic chunks + normalized security groups, Gold = Vector indexes + RLS-secured query views).
2. **Binary Handling**: Never store raw file binaries in table rows. Stream blobs directly to Cloud Object Storage (`s3://`, `abfss://`, `gs://`) and store cloud URIs in Lakehouse metadata tables.
3. **Security First (DLS)**: Never discard or flatten source Access Control Lists (ACLs). Always ingest source permissions and enforce late-binding query-time filtering based on authenticated group memberships.
4. **Idempotency & FinOps**: Always compute `SHA-256` content checksums. Skip expensive parsing, OCR, and embedding API calls if the content hash has not changed.
5. **Incremental Sync**: Always use native delta APIs (Microsoft Graph Delta Query `@odata.deltaLink` and Confluence incremental cursors/webhooks). Never perform brute-force full scans in recurring production runs.
6. **Robust Network I/O**: Implement exponential backoff with jitter and honor HTTP 429 `Retry-After` headers. Use streaming I/O for large file downloads.
7. **Tombstone Awareness**: Treat deletions as first-class events and ensure deletions propagate down to vector stores to prevent zombie hallucinations.
8. **Diagram Aesthetics & Dual-Mode Contrast**: When generating Mermaid diagrams, strictly follow the [mermaid-drawing skill](file:///c:/Users/ToanBX/dev/personal/document_ingestors/.agents/skills/mermaid-drawing/SKILL.md) to ensure high-contrast legibility across both Dark Mode and Light Mode.
