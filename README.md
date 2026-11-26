# gridlock

I am building the standalone interactive UI/UX mockup for **Gridlock (MVP v1.1)** using pure Tailwind CSS utility classes and client-side simulated Atlas `$rankFusion`, Voyage Multimodal-3, and OpenUSD `$graphLookup` intelligence.

### Highlights of the Mockup

* **Pure Tailwind CSS (Zero Custom `<style>` Tags):** Uses Tailwind's dark studio palette (`zinc-950`, `zinc-900`, `amber-500`, `cyan-400`) and inline SVG schematics so every shot, shader ball, and USD rig renders crisply without external image links.
* **Atlas `$rankFusion` & Frame-Accurate Scrubbing:** Switch between `$rankFusion`, `Visual`, and `USD/Lexical` search modes, click prompt chips like `"backlit sandstorm"`, or scrub individual keyframes (`f048`, `f101`, `f184`) in the right inspector drawer.
* **OpenUSD `$graphLookup` & OCIO LUT Preview:** Toggle between raw linear and `OCIO: sRGB LUT ON`, inspect interactive USD sublayer dependencies (`shd_spice_sand_v04`, `rig_worm_spine_v09`), copy 15-minute presigned R2/S3 URLs, or simulate ingesting a new `.exr` file via **+ Ingest Media**.

---

# Gridlock — MVP Technical Implementation Spec (v1.1)

**Vision:** AI-native Media Production & Asset Intelligence layer powered by MongoDB Atlas, Voyage AI, and Atlas Search.

**MVP Wedge:** Ingest, index, and search unstructured studio media (renders, turntables, concept art, notes) without manual tagging, syncing safely with legacy trackers.

---

## 1. Tech Stack

* **Database & Search:** MongoDB Atlas (M10+ cluster), Atlas Search, Atlas Vector Search (`$vectorSearch` + `$rankFusion`), `$graphLookup`.
* **AI / Embeddings:** Voyage AI (`voyage-multimodal-3` for unified image + text embeddings, 1024 dimensions).
* **Backend & Ingestion:** Python (FastAPI) + Temporal.io (or Celery/Redis) for async media processing workers.
* **Media & Color Processing:** OpenImageIO (`oiiotool` + OpenColorIO / ACEScg-to-sRGB LUTs), FFmpeg (keyframe extraction, proxy transcoding), OpenUSD Python bindings (`usdcat` / `pxr` dependency parsing).
* **Frontend:** Next.js (React) + Tailwind CSS + TanStack Virtual (for 60fps virtualized asset grids).
* **Storage & Security:** AWS S3 / Cloudflare R2 (private buckets with short-lived presigned URLs for raw EXRs/MOVs and WebP/H.264 proxies).

---

## 2. Core MongoDB Schema & Indexes

To avoid BSON document bloat on long sequences and enable timestamp-accurate vector retrieval, the data model separates entity metadata from frame-level embeddings across two collections.

### Collection A: `entities` (Polymorphic Pattern)

Unifies Assets, Shots, and Versions in a single collection for cross-entity lookups and `$graphLookup` lineage trees. All lineage references standardize on human-readable `code` strings to align with OpenUSD layer resolution.

**Required Index:**

```javascript
db.entities.createIndex({ org_id: 1, project_id: 1, code: 1 }, { unique: true });

```

**Document Schema:**

```json
{
  "_id": { "$oid": "6745a1b2c3d4e5f6a7b8c9d0" },
  "org_id": "studio_apex",
  "project_id": "proj_dune_03",
  "entity_type": "version",
  "code": "chr_sandworm_v014",
  "status": "approved",
  "department": "fx",
  "lineage": {
    "parent_code": "asset_sandworm_hero",
    "usd_dependency_codes": ["asset_sand_shader_v04", "rig_worm_spine_v09"]
  },
  "media": {
    "source_key": "dune03/fx/chr_sandworm_v014.mov",
    "proxy_stream_key": "proxies/dune03/v014.mp4",
    "hero_thumb_key": "thumbs/dune03/v014_f101.webp",
    "duration_sec": 12.5,
    "fps": 24,
    "color_space": "ACEScg"
  },
  "context": {
    "artist": "a.chen",
    "notes": "Increased trailing dust density and backlit rim highlights per sup notes.",
    "generative_metadata": {
      "model": "comfyui-flux-dev",
      "seed": 88492011,
      "prompt": "massive desert worm erupting through backlit sandstorm"
    }
  },
  "external_sync": {
    "tracker": "shotgrid",
    "external_id": "Version_99412",
    "last_sync_id": "evt_sg_88210a",
    "last_synced_at": { "$date": "2026-11-26T00:05:00Z" }
  },
  "created_at": { "$date": "2026-11-26T00:00:00Z" }
}

```

### Collection B: `keyframes` (Frame-Level Hybrid & Vector Index)

Stores individual extracted frames, their 1024-dim embeddings, and denormalized searchable text fields (`notes`, `artist`, `usd_dependency_codes`) so `$rankFusion` executes on a single collection and returns exact matching timestamps.

**Required Index:**

```javascript
db.keyframes.createIndex({ org_id: 1, project_id: 1, entity_id: 1, frame_num: 1 });

```

**Document Schema:**

```json
{
  "_id": { "$oid": "6745a1b2c3d4e5f6a7b8c9d1" },
  "org_id": "studio_apex",
  "project_id": "proj_dune_03",
  "entity_id": { "$oid": "6745a1b2c3d4e5f6a7b8c9d0" },
  "code": "chr_sandworm_v014",
  "status": "approved",
  "department": "fx",
  "artist": "a.chen",
  "notes": "Increased trailing dust density and backlit rim highlights per sup notes.",
  "usd_dependency_codes": ["asset_sand_shader_v04", "rig_worm_spine_v09"],
  "timestamp_sec": 4.2,
  "frame_num": 101,
  "thumb_key": "thumbs/dune03/v014_f101.webp",
  "multimodal_embedding": [0.0124, -0.0482, "... (1024 floats)"]
}

```

---

## 3. Asynchronous Ingestion & Color Pipeline

1. **Trigger:** S3 Event Notification (`s3:ObjectCreated:*`) or DCC Python publish hook calls `POST /api/v1/ingest` with scoped JWT credentials.
2. **Metadata & USD Parsing (Worker):**
* Run `ffprobe` / `oiiotool --info` and `usdcat` to extract frame rate, resolution, source color space, and OpenUSD layer dependency codes (`usd_dependency_codes`).


3. **Color-Managed Tone-Mapping & Transcoding:**
* If input is linear HDR (`.exr`, `.dpx`), apply an OpenColorIO (`oiiotool --colorconfig studio.ocio --colorconvert ACEScg "Output - sRGB"`) transform prior to encoding so dark values and highlights match studio dailies.
* Transcode a lightweight H.264/WebM review proxy (CRF 24) and extract 1 WebP keyframe every 2 seconds (or on scene cuts via `select='gt(scene,0.3)'`).


4. **Multimodal Embedding (Voyage AI):**
* Batch normalized WebP keyframes alongside `context.notes` and `code` to `voyage-multimodal-3`.
* Receive 1024-dim dense vectors bridging visual pixels and production text.


5. **Upsert, Denormalization Sync & Stream:**
* Upsert parent document in `entities` and bulk-insert frame documents (with denormalized `notes`, `artist`, `status`, and `usd_dependency_codes`) into `keyframes`.
* When metadata updates occur post-ingest (e.g., via ShotGrid webhook), execute `db.keyframes.updateMany({ org_id, project_id, entity_id }, { $set: ... })` alongside the `entities` update.
* MongoDB Change Streams push real-time WebSocket updates (containing short-lived presigned thumbnail URLs) to authorized studio web clients.



---

## 4. Atlas Hybrid Search & Lineage Architecture

### Search Index Definitions

* **Vector Index (`frame_vector_idx` on `keyframes`):** `numDimensions: 1024`, `similarity: "cosine"`, `path: "multimodal_embedding"`, filterable on `org_id`, `project_id`, `status`, and `department`.
* **Atlas Search Index (`frame_lexical_idx` on `keyframes`):** Dynamic mapping on `code`, `notes`, `artist`, and `usd_dependency_codes`, plus token filters on `org_id`, `project_id`, `status`, and `department`.
* **Atlas Search Index (`entity_lexical_idx` on `entities`):** Dynamic mapping on `code`, `context.notes`, `context.artist`, and `lineage.usd_dependency_codes` for non-frame entity lookups.

### Frame-Level Hybrid Query (`$rankFusion` on `keyframes`)

Searches at the frame level so results return the exact `timestamp_sec` and `thumb_key`, then joins parent entity metadata via `$lookup`:

```javascript
db.keyframes.aggregate([
  {
    $rankFusion: {
      input: {
        pipelines: {
          semantic: [
            {
              $vectorSearch: {
                index: "frame_vector_idx",
                path: "multimodal_embedding",
                queryVector: "<VOYAGE_QUERY_EMBEDDING>",
                numCandidates: 150,
                limit: 30,
                filter: {
                  org_id: { $eq: "studio_apex" },
                  project_id: { $eq: "proj_dune_03" }
                }
              }
            }
          ],
          lexical: [
            {
              $search: {
                index: "frame_lexical_idx",
                compound: {
                  must: [
                    {
                      text: {
                        query: "backlit sandstorm",
                        path: ["code", "notes", "artist", "usd_dependency_codes"]
                      }
                    }
                  ],
                  filter: [
                    { equals: { path: "org_id", value: "studio_apex" } },
                    { equals: { path: "project_id", value: "proj_dune_03" } }
                  ]
                }
              }
            }
          ]
        }
      }
    }
  },
  { $limit: 20 },
  {
    $lookup: {
      from: "entities",
      localField: "entity_id",
      foreignField: "_id",
      as: "entity"
    }
  },
  { $unwind: "$entity" }
])

```

### USD Dependency Tree Query (`$graphLookup` on `entities`)

Traverses upstream/downstream OpenUSD dependencies across the project using `code` matching:

```javascript
db.entities.aggregate([
  {
    $match: {
      org_id: "studio_apex",
      project_id: "proj_dune_03",
      code: "chr_sandworm_v014"
    }
  },
  {
    $graphLookup: {
      from: "entities",
      startWith: "$lineage.usd_dependency_codes",
      connectFromField: "lineage.usd_dependency_codes",
      connectToField: "code",
      as: "dependency_tree",
      maxDepth: 10,
      restrictSearchWithMatch: {
        org_id: "studio_apex",
        project_id: "proj_dune_03"
      }
    }
  }
])

```

---

## 5. Security, Multi-Tenancy & Legacy Tracker Sync

### Studio IP Protection & RBAC

* **Tenant & Project Isolation:** Every query enforces server-side `org_id` and `project_id` predicates derived from the user's SSO/JWT claims—never trusted from client query parameters.
* **Zero Public Bucket URLs:** S3/R2 buckets remain strictly private. The API stores object keys (`media.proxy_stream_key`) and mints 15-minute presigned URLs on demand after verifying project membership.

### Loop-Safe Bidirectional Sync (ShotGrid / ftrack)

* **Ownership Boundary:** Legacy trackers (ShotGrid/ftrack) remain the source of truth for task assignment and approval status; Gridlock owns media proxies, embeddings, and USD lineage.
* **Echo Loop Prevention:** Every outbound webhook or API update from Gridlock attaches an `X-Gridlock-Sync-Id` header and updates `external_sync.last_sync_id`. Incoming webhooks check if the incoming event ID matches `last_sync_id` (or originated from the Gridlock service account) and drop duplicate echo events immediately.

---

## 6. MVP Execution Roadmap (6 Weeks)

* **Weeks 1–2 (Core Data, Color & Ingestion):** Deploy MongoDB Atlas cluster, compound indexes, private S3/R2 buckets, OCIO/FFmpeg/USD worker pipeline, and Voyage AI embedding batcher writing to `entities` and `keyframes`.
* **Weeks 3–4 (Hybrid Search, Lineage & Security):** Implement RBAC-enforced `$rankFusion` endpoint (returning timestamp-accurate frame hits + presigned URLs) and `$graphLookup` endpoint for USD dependency trees.
* **Weeks 5–6 (Studio UI & Loop-Safe Sync):** Build virtualized visual browser with timestamp-accurate frame scrubbing, real-time Change Stream WebSocket updates, and idempotent webhook bridge with ShotGrid/ftrack.
