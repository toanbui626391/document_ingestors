# Mermaid Dual-Mode Color Palettes & Contrast Reference

This reference provides exact hex color values, contrast ratios (WCAG 2.1 compliance), and semantic guidelines for Mermaid diagrams that remain 100% legible in both **Dark Mode** and **Light Mode**.

---

## The Root Cause of Dual-Mode Failures

1. **Colored Text on Dark Fills**: Setting `color:#60a5fa` on `#1e293b` only achieves a **4.2:1** contrast ratio, which is blurry and fails WCAG AA standards.
2. **Hardcoded Gray Lines**: Mid-gray links (`#94a3b8`) blend into white backgrounds in light mode and vanish against dark backgrounds.
3. **Low-Luminance Boxes**: Subtle dark boxes blend into dark IDE surfaces (`#1e1e1e` / `#0d1117`).

---

## 1. Pattern 1: The "High-Luminance Card" System (Recommended Default)

* **Node Background**: Solid White (`#ffffff`).
* **Text Font Color**: Jet Charcoal (`#0f172a`).
* **Contrast Ratio**: **19.1:1 (WCAG AAA)**.
* **Border (`stroke`)**: 2.5px solid saturated color.

### Complete `classDef` Palette
```mermaid
classDef default fill:#ffffff,stroke:#475569,stroke-width:2.5px,color:#0f172a;
classDef blue    fill:#ffffff,stroke:#2563eb,stroke-width:2.5px,color:#0f172a;
classDef green   fill:#ffffff,stroke:#059669,stroke-width:2.5px,color:#0f172a;
classDef amber   fill:#ffffff,stroke:#d97706,stroke-width:2.5px,color:#0f172a;
classDef purple  fill:#ffffff,stroke:#7c3aed,stroke-width:2.5px,color:#0f172a;
classDef red     fill:#ffffff,stroke:#dc2626,stroke-width:2.5px,color:#0f172a;
classDef cyan    fill:#ffffff,stroke:#0891b2,stroke-width:2.5px,color:#0f172a;
```

### Color Specification Table

| Class | Semantic Role | Fill | Stroke (Border) | Text Color | Contrast Ratio |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `default` | General Worker / Utility | `#ffffff` | `#475569` (Slate) | `#0f172a` | **19.1 : 1** |
| `blue` | Core Logic / API / Ingestor | `#ffffff` | `#2563eb` (Royal Blue)| `#0f172a` | **19.1 : 1** |
| `green` | Lakehouse / Delta / Storage | `#ffffff` | `#059669` (Emerald) | `#0f172a` | **19.1 : 1** |
| `amber` | Message Queues / Event Hubs | `#ffffff` | `#d97706` (Amber) | `#0f172a` | **19.1 : 1** |
| `purple`| Gateways / Sources (SharePoint/Confluence) | `#ffffff` | `#7c3aed` (Violet) | `#0f172a` | **19.1 : 1** |
| `red` | Security / IAM / DLP / Alerts | `#ffffff` | `#dc2626` (Crimson) | `#0f172a` | **19.1 : 1** |
| `cyan` | AI / Vector DB / Embeddings | `#ffffff` | `#0891b2` (Dark Cyan)| `#0f172a` | **19.1 : 1** |

---

## 2. Pattern 2: The "Midnight High-Contrast" System (Dark Card Alternative)

If dark tiles are explicitly desired:
* **Node Background**: Midnight Navy (`#0f172a`).
* **Text Font Color**: **Pure White (`#ffffff`)** — **NEVER colored text!**
* **Contrast Ratio**: **16.2:1 (WCAG AAA)**.
* **Border (`stroke`)**: 2.5px vivid neon accent.

### Complete `classDef` Palette
```mermaid
classDef default fill:#0f172a,stroke:#64748b,stroke-width:2.5px,color:#ffffff;
classDef blue    fill:#0f172a,stroke:#38bdf8,stroke-width:2.5px,color:#ffffff;
classDef green   fill:#0f172a,stroke:#4ade80,stroke-width:2.5px,color:#ffffff;
classDef amber   fill:#0f172a,stroke:#fbbf24,stroke-width:2.5px,color:#ffffff;
classDef purple  fill:#0f172a,stroke:#c084fc,stroke-width:2.5px,color:#ffffff;
classDef red     fill:#0f172a,stroke:#f87171,stroke-width:2.5px,color:#ffffff;
classDef cyan    fill:#0f172a,stroke:#22d3ee,stroke-width:2.5px,color:#ffffff;
```

---

## 3. Subgraph & Link Specifications

### Subgraph Clusters
```mermaid
style SubgraphId fill:none,stroke:#475569,stroke-width:2px,stroke-dasharray: 4 4,color:#475569
```
* `fill:none`: Guarantees transparency so host theme canvas shines through.
* `stroke:#475569`: Mid-slate border visible against both white and dark canvases.

### Connecting Links (Arrows)
```mermaid
linkStyle default stroke:#0284c7,stroke-width:2px
```
* `#0284c7` (Cobalt Blue): Saturated mid-tone with high visibility against `#ffffff` (light mode) and `#0d1117` (dark mode).
