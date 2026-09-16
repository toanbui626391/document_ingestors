# Confluence Event-Driven Ingestion: Features, APIs & Deep Dive

## Executive Summary
Atlassian Confluence is the de facto enterprise wiki and knowledge repository. Building an event-driven ingestor for Confluence requires capturing modifications not only to text pages, but also to embedded **attachments (PDFs, presentations, spreadsheets)**, space structures, and permission restrictions.

Depending on whether your enterprise runs **Confluence Cloud** or **Confluence Data Center (On-Premises)**, different event delivery channels and APIs must be utilized.

---

## 1. Confluence Event Model & Event Catalog

Confluence models knowledge hierarchically: `Space -> Page / Blogpost -> Attachments & Comments`.

### Supported Event Types
| Event Category | Event Name | Ingestor Action |
| :--- | :--- | :--- |
| **Page Lifecycle** | `page_created` | Trigger parse, chunking, and vector embedding. |
| | `page_updated` | Compute `SHA-256`. If content changed, re-chunk and update vectors. If only metadata changed, update Bronze/Silver metadata. |
| | `page_trashed` / `page_removed` | Emit tombstone record (`is_deleted = true`); purge corresponding chunks from Vector DB. |
| | `page_restored` | Revoke tombstone and restore active status in search indexes. |
| | `page_moved` | Update hierarchical breadcrumb paths for all child pages and chunks. |
| **Attachments** | `attachment_created` | Download binary blob from `/wiki/api/v2/attachments/{id}/download`; extract text via OCR/Docling; link lineage to parent page. |
| | `attachment_updated` | Compare content hash; re-process binary if updated. |
| | `attachment_removed` | Purge attachment chunks from vector store. |
| **Spaces & ACLs** | `space_created` | Register new space container in Bronze metadata registry. |
| | `space_permissions_updated`| Re-evaluate normalized group access for all pages in the space. |

---

## 2. Event Ingestion Channels & Integrations

### Option A: Confluence Automation (No-Code Cloud Integration)
Confluence Cloud includes a native workflow engine: **Automation for Confluence**.
* **Triggers**: "Page published", "Page edited", "Attachment created", "Page deleted".
* **Built-in Actions**:
  1. **"Put EventBridge / CloudWatch Events" Action**:
     * Directly authenticates to your AWS account using IAM roles.
     * Publishes a JSON event into an **Amazon EventBridge** bus or CloudWatch Events stream.
     * *Architectural Benefit*: Fully managed by Atlassian, zero public webhooks, automatic buffering into AWS SQS.
  2. **"Send Webhook" Action**:
     * Sends an HTTP POST to an arbitrary webhook receiver URL (e.g., Azure API Gateway, GCP Cloud Run, or custom endpoint).
     * Supports custom headers (e.g., `Authorization: Bearer <token>`) and custom JSON payloads.

### Option B: Atlassian Forge Framework (Cloud Native App)
**Atlassian Forge** is Atlassian's serverless development platform running in Atlassian's hosted environment.
* **Manifest Event Triggers (`manifest.yml`)**:
  ```yaml
  modules:
    trigger:
      - key: confluence-page-change-trigger
        function: handle-page-change
        events:
          - avi:confluence:created:page
          - avi:confluence:updated:page
          - avi:confluence:deleted:page
          - avi:confluence:created:attachment
  ```
* **Execution Flow**:
  1. The event executes a serverless Node.js/TypeScript function inside the Forge environment.
  2. The function makes an authenticated outbound call (`fetch()`) to your enterprise event bus (e.g., AWS EventBridge, Azure Event Hubs, or Kafka Gateway).
  3. Handles authentication securely via Forge environment variables and encrypted storage.

### Option C: Native Confluence REST Webhooks (Data Center & Cloud)
* **Endpoint (Data Center)**: `POST /wiki/rest/webhooks/1.0/webhook`
* **Configuration Payload**:
  ```json
  {
    "name": "Enterprise Lakehouse Document Ingestor",
    "url": "https://ingestor.enterprise.com/api/v1/webhooks/confluence",
    "events": [
      "page_created",
      "page_updated",
      "page_trashed",
      "page_removed",
      "attachment_created",
      "attachment_updated",
      "attachment_removed"
    ],
    "filters": {
      "space-key": ["ENG", "ARCH", "PRODUCT"]
    },
    "excludeBody": false
  }
  ```

### Sample Incoming Confluence Webhook Payload
```json
{
  "timestamp": 1773638400000,
  "webhookEvent": "page_updated",
  "page": {
    "id": "123456789",
    "title": "Enterprise Data Lakehouse Architecture",
    "version": 4,
    "spaceKey": "ENG",
    "self": "https://mycompany.atlassian.net/wiki/rest/api/content/123456789"
  },
  "user": {
    "accountId": "712020:a1b2c3d4-e5f6-7890-abcd-ef0123456789",
    "displayName": "Jane Doe"
  }
}
```

---

## 3. Data & Content Extraction APIs (Confluence REST API v2)

Once an event notifies the ingestor of a page or attachment change, the ingestor queries the **Confluence REST API v2** for content, attachments, and permissions.

### A. Fetching Page Content & Structure
* **Endpoint**: `GET /wiki/api/v2/pages/{id}?body-format=storage,atlas_doc_format`
* **Content Formats**:
  * **Storage Format (`body.storage.value`)**: XHTML-based XML format containing Confluence macro tags (`<ac:structured-macro>`).
  * **Atlassian Document Format (`body.atlas_doc_format.value`)**: JSON-based AST tree.
* **Transform Directive**: Convert the Storage Format or ADF into **Standard Markdown** using parsers like `pandoc`, `html2text`, or custom BeautifulSoup rules to remove macro UI boilerplate while preserving tables and code snippets.

### B. Extracting Page Restrictions & Space Permissions (ACLs)
Confluence enforces permissions at both the **Space level** and the **Page level (Restrictions)**:
1. **Space Permissions**:
   * `GET /wiki/api/v2/spaces/{id}`
   * Returns permissions granted to Atlassian groups or users.
2. **Page Content Restrictions**:
   * `GET /wiki/rest/api/content/{id}/restriction/byOperation/read`
   * Returns specific users or groups allowed to view the page when inheritance is restricted.
   * If a page has read restrictions, **only** those listed principals can read the document.

### C. Extracting Attachments
* **Endpoint**: `GET /wiki/api/v2/pages/{id}/attachments`
* **Download Endpoint**: `GET /wiki/api/v2/attachments/{attachment-id}/download`
* **Lineage Tracking**:
  Record `parent_page_id`, `attachment_file_name`, and `mime_type` in the Bronze metadata table so chunks from attachments are associated with the parent page's breadcrumbs and search queries.

---

## 4. Reconciler API: Handling Dropped Webhooks & Periodic Audits

Because webhooks are delivered "best effort," event-driven ingestors must incorporate a **scheduled reconciliation sweeper**:
* **API Endpoint**: `GET /wiki/api/v2/pages?sort=-modified-date&limit=250`
* **Cursor-Based Pagination**: Confluence v2 API uses cursor links (`/wiki/api/v2/pages?cursor=...`).
* **Audit Flow**:
  1. The sweeper runs daily or every few hours.
  2. Pulls recent pages modified since `last_audit_watermark`.
  3. Compares the `page.version` and `page.id` against the Lakehouse Bronze table.
  4. If any event was missed, queues an internal processing message to heal the delta.
