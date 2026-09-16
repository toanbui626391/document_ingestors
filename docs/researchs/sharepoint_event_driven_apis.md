# SharePoint & OneDrive Event-Driven Ingestion: Features, APIs & Deep Dive

## Executive Summary
For Microsoft SharePoint and OneDrive, Microsoft provides enterprise-grade, event-driven integration through the **Microsoft Graph API**. Instead of high-frequency polling of drives or sites, ingestors can subscribe to asynchronous change notifications (webhooks or Azure Event Hubs streams) paired with the **Delta Query API** to build a high-throughput, low-latency, and cost-effective ingestion pipeline.

---

## 1. Microsoft Graph Change Notifications (Webhooks)

The primary event mechanism in the Microsoft 365 ecosystem is the **Change Notification Subscription API**.

### A. Subscription Endpoint
* **API Endpoint**: `POST https://graph.microsoft.com/v1.0/subscriptions`
* **Supported Resources for Document Ingestion**:
  * **Drive Root**: `/drives/{drive-id}/root` (Monitors all files, folders, and subfolders within a document library).
  * **Site Document Library**: `/sites/{site-id}/lists/{list-id}` (Monitors items within a specific SharePoint list or library).
  * **Specific Folder**: `/drives/{drive-id}/items/{folder-id}` (Scoped to a particular folder hierarchy).
  * **User OneDrive**: `/users/{user-id}/drive/root`.
* **Supported Change Types (`changeType`)**:
  * `created`: Triggers when a new file or folder is uploaded or created.
  * `updated`: Triggers when content, metadata, or permissions change.
  * `deleted`: Triggers when a file is deleted or moved to the Recycle Bin.

### B. Subscription Creation Payload
```json
POST https://graph.microsoft.com/v1.0/subscriptions
Content-Type: application/json
Authorization: Bearer <access_token>

{
  "changeType": "created,updated,deleted",
  "notificationUrl": "https://ingestor.enterprise.com/api/v1/webhooks/sharepoint",
  "resource": "drives/b!AbC123XyZ/root",
  "expirationDateTime": "2026-09-19T18:23:45.000Z",
  "clientState": "SecretTokenOrSignatureForVerification-98765"
}
```

### C. Webhook Validation Handshake
When you call `POST /subscriptions`, Microsoft Graph sends a synchronous HTTP POST validation request to your `notificationUrl` before activating the subscription:
1. **Validation Request**: Contains a query parameter: `?validationToken=<random_token_string>`.
2. **Required Response**:
   * HTTP Status: `200 OK`.
   * Content-Type: `text/plain; charset=utf-8`.
   * Response Body: The exact `<random_token_string>` value.
   * **Timeout Constraint**: Your endpoint must respond within **10 seconds**, or subscription creation fails.

### D. Incoming Webhook Notification Payload
When a document is created, updated, or deleted, Microsoft Graph posts an event payload:
```json
{
  "value": [
    {
      "subscriptionId": "7f8b9a10-2345-6789-abcd-ef0123456789",
      "subscriptionExpirationDateTime": "2026-09-19T18:23:45.000Z",
      "changeType": "updated",
      "resource": "drives('b!AbC123XyZ')/root",
      "resourceData": {
        "@odata.type": "#Microsoft.Graph.DriveItem",
        "@odata.id": "drives('b!AbC123XyZ')/items('01ABCDEF123456')",
        "id": "01ABCDEF123456"
      },
      "clientState": "SecretTokenOrSignatureForVerification-98765",
      "tenantId": "11112222-3333-4444-5555-666677778888"
    }
  ]
}
```

---

## 2. Enterprise Delivery Channels: Azure Event Hubs Integration

In enterprise environments, hosting a public-facing HTTPS webhook endpoint violates security perimeter guidelines. Microsoft Graph natively supports delivering notifications directly to **Azure Event Hubs**.

### Key Advantages
1. **Zero Public Ingress**: No need to open inbound firewall ports or deploy an API Gateway to the public internet.
2. **Built-in Buffering**: Azure Event Hubs acts as a distributed commit log, absorbing massive change spikes (e.g., mass upload of 50,000 files) without overwhelming backend ingestors.
3. **No Validation Handshake**: When targeting Event Hubs, Graph does not perform the HTTP `validationToken` handshake.
4. **Native Lakehouse Streaming**: Event Hubs can be consumed directly by Databricks Spark Structured Streaming, Azure Functions, or containerized Kafka-compatible consumers.

### Configuration Protocol
* **Notification URL Syntax**:
  `notificationUrl: "EventHub:https://<eventhub-namespace>.servicebus.windows.net/<eventhub-name>?tenantId=<tenant-id>"`
* **Authorization**: Microsoft Graph authenticates to the Event Hub using Azure Entra ID (Azure AD) RBAC by granting the Microsoft Graph service principal the `Azure Event Hubs Data Sender` role on the target namespace.

---

## 3. Rich Notifications with Encrypted Resource Data

Standard notifications only include the resource ID. **Rich Notifications** bundle the actual updated metadata directly within the event payload, eliminating an extra network round-trip.

### How It Works
1. Your application generates an **asymmetric RSA key pair (X.509 certificate)**.
2. During subscription creation, you pass the base64-encoded public certificate in `encryptionCertificate`:
   ```json
   {
     "changeType": "updated",
     "notificationUrl": "https://ingestor.enterprise.com/webhooks",
     "resource": "drives/b!AbC123XyZ/root",
     "includeResourceData": true,
     "encryptionCertificate": "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...",
     "encryptionCertificateId": "my-cert-id-2026"
   }
   ```
3. Microsoft Graph generates a symmetric AES key, encrypts the resource metadata, encrypts the AES key with your RSA public key, and delivers the payload.
4. The ingestor decrypts the symmetric key using its private key and unpacks the resource metadata immediately.

---

## 4. The Graph Delta Query API (`@odata.deltaLink`)

While webhooks inform you that a change occurred, **Microsoft Graph Delta Query** is the critical API for state reconciliation and bulk drainage.

### Endpoint
`GET https://graph.microsoft.com/v1.0/drives/{drive-id}/root/delta`

### Features
1. **Comprehensive Change Detection**: Returns created items, updated content, updated metadata, renamed files, moved files, and deletions.
2. **Deletions Representation**: Deleted items are explicitly returned with a `@removed` facet:
   ```json
   {
     "@odata.type": "#microsoft.graph.driveItem",
     "id": "01ABCDEF999999",
     "name": "Deprecated_Architecture.docx",
     "@removed": {
       "reason": "deleted"
     }
   }
   ```
3. **State Watermarking via Delta Links**:
   * The initial query returns paginated items with `@odata.nextLink`.
   * The final page returns `@odata.deltaLink`:
     `https://graph.microsoft.com/v1.0/drives/{drive-id}/root/delta?token=a1b2c3d4e5f6...`
   * Subsequent syncs call this `deltaLink` URL directly to fetch *only* what changed since the token was issued.

---

## 5. Security & Permission Tracking via API

SharePoint documents inherit permissions from their parent folder or site unless inheritance is broken.
* To detect custom permissions, inspect the `hasUniqueRoleAssignments` property on the DriveItem.
* When `hasUniqueRoleAssignments == true`, query the permissions endpoint:
  `GET https://graph.microsoft.com/v1.0/drives/{drive-id}/items/{item-id}/permissions`
* Returns granted roles, user principals, and Entra ID Security Group IDs (e.g., `grantedToIdentitiesV2`).

---

## 6. Throttling, Quotas & Resilience Guardrails

| Constraint | Limit / Behavior | Ingestor Mitigation |
| :--- | :--- | :--- |
| **Subscription Expiration** | Maximum expiration for DriveItem subscriptions is **4,230 minutes (~2.9 days)**. | Deploy a recurring scheduled daemon (e.g., hourly) to execute `PATCH /v1.0/subscriptions/{id}` with a renewed `expirationDateTime`. |
| **Rate Limiting (HTTP 429)** | Per-tenant and per-app request quotas. Response includes `Retry-After: <seconds>`. | Implement exponential backoff with full jitter. Strictly pause worker threads for the duration of `Retry-After`. |
| **Large File Download** | Files > 4 MB should not be buffered in RAM. | Stream the `@microsoft.graph.downloadUrl` stream directly into Azure Data Lake Storage (ADLS Gen2) or AWS S3 multipart upload. |
| **Batching** | Up to 20 individual Graph requests can be combined into a single `$batch` request. | Use `$batch` when resolving metadata or permissions for multiple files discovered in a single delta run. |
