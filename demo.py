import asyncio
import base64
import json
import os
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import httpx
from botocore.config import Config
from bson import ObjectId
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# ==============================================================================
# 1. CONFIGURATION & CLIENTS
# ==============================================================================

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority")
DB_NAME = os.getenv("MONGODB_DB", "gridlock_mvp")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY", "")
VOYAGE_MODEL = "voyage-multimodal-3"

S3_BUCKET = os.getenv("S3_BUCKET", "gridlock-studio-private")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")  # Optional for Cloudflare R2
AWS_REGION = os.getenv("AWS_REGION", "auto")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-studio-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
OCIO_CONFIG_PATH = os.getenv("OCIO_CONFIG_PATH", "")  # Path to studio.ocio if installed

app = FastAPI(title="Gridlock MVP API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = AsyncIOMotorClient(MONGODB_URI)
db = mongo_client[DB_NAME]
entities_col = db["entities"]
keyframes_col = db["keyframes"]

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    region_name=AWS_REGION,
    config=Config(signature_version="s3v4"),
)

security = HTTPBearer(auto_error=False)


# ==============================================================================
# 2. AUTH / MULTI-TENANT RBAC (Derived strictly from JWT claims)
# ==============================================================================

class TenantContext(BaseModel):
    org_id: str
    project_id: str
    user_id: str


async def get_tenant(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> TenantContext:
    """
    Enforces tenant & project isolation from verified SSO/JWT claims.
    Falls back to a dev studio token ONLY if JWT_SECRET is the default dev string.
    """
    if not creds:
        if JWT_SECRET == "dev-studio-secret-change-in-prod":
            return TenantContext(org_id="studio_apex", project_id="proj_dune_03", user_id="a.chen")
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    try:
        payload = jwt.decode(creds.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return TenantContext(
            org_id=payload["org_id"],
            project_id=payload["project_id"],
            user_id=payload.get("sub", "unknown"),
        )
    except (JWTError, KeyError) as exc:
        raise HTTPException(status_code=401, detail=f"Invalid studio JWT: {exc}")


def mint_presigned_url(object_key: Optional[str], expires_in: int = 900) -> Optional[str]:
    """Generates a 15-minute private S3/R2 URL on demand."""
    if not object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": object_key},
            ExpiresIn=expires_in,
        )
    except Exception:
        # Fallback placeholder if S3 credentials aren't configured in local dev
        return f"https://private-r2.local/{object_key}?X-Amz-Expires={expires_in}"


# ==============================================================================
# 3. PYDANTIC MODELS
# ==============================================================================

class IngestRequest(BaseModel):
    code: str = Field(..., examples=["chr_sandworm_v014"])
    entity_type: str = Field("version", pattern="^(asset|shot|version)$")
    department: str = Field(..., examples=["fx", "lighting", "lookdev", "rigging"])
    status: str = Field("wip", examples=["wip", "pending_review", "approved"])
    source_key: str = Field(..., examples=["dune03/fx/chr_sandworm_v014.mov"])
    parent_code: Optional[str] = None
    usd_dependency_codes: List[str] = Field(default_factory=list)
    artist: str = "system"
    notes: str = ""
    color_space: str = "ACEScg"
    generative_metadata: Optional[Dict[str, Any]] = None
    external_id: Optional[str] = None


class ShotGridWebhookPayload(BaseModel):
    event_id: str
    org_id: str
    project_id: str
    entity_code: str
    status: Optional[str] = None
    notes: Optional[str] = None
    artist: Optional[str] = None


# ==============================================================================
# 4. VOYAGE AI MULTIMODAL EMBEDDINGS & MEDIA WORKER
# ==============================================================================

async def get_voyage_multimodal_embedding(
    text: str, image_bytes: Optional[bytes] = None, input_type: str = "document"
) -> List[float]:
    """
    Calls Voyage AI `voyage-multimodal-3` (1024-dim) for unified image + text or query-only vectors.
    """
    if not VOYAGE_API_KEY:
        # Deterministic mock 1024-d vector for local testing without an API key
        seed = sum(ord(c) for c in text) % 100
        return [round(((seed + i) % 100) / 1000.0, 5) for i in range(1024)]

    content_items: List[Dict[str, Any]] = [{"type": "text", "text": text}]
    if image_bytes:
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        content_items.append({
            "type": "image_base64",
            "image_base64": f"data:image/webp;base64,{b64}",
        })

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.voyageai.com/v1/multimodalembeddings",
            headers={"Authorization": f"Bearer {VOYAGE_API_KEY}"},
            json={
                "inputs": [{"content": content_items}],
                "model": VOYAGE_MODEL,
                "input_type": input_type,
            },
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


def parse_usd_dependencies(local_path: Path) -> List[str]:
    """Extracts referenced layer/asset codes using OpenUSD (`usdcat`) if available."""
    if local_path.suffix.lower() not in {".usd", ".usda", ".usdc"}:
        return []
    try:
        out = subprocess.check_output(["usdcat", str(local_path)], text=True, stderr=subprocess.DEVNULL)
        deps = []
        for line in out.splitlines():
            if "@" in line and (".usd" in line or ".abc" in line):
                ref = line.split("@")[1]
                deps.append(Path(ref).stem)
        return list(dict.fromkeys(deps))
    except Exception:
        return []


def extract_color_managed_keyframes(
    source_path: Path, work_dir: Path, color_space: str
) -> List[Dict[str, Any]]:
    """
    1. Applies OpenColorIO ACEScg -> sRGB transform on linear HDR (.exr/.dpx) if configured.
    2. Extracts 1 WebP keyframe every 2 seconds via FFmpeg.
    """
    processable_path = source_path

    # Step A: OCIO Color Transform for linear EXR/DPX
    if source_path.suffix.lower() in {".exr", ".dpx"} and OCIO_CONFIG_PATH:
        srgb_exr = work_dir / f"{source_path.stem}_srgb.png"
        subprocess.run(
            [
                "oiiotool",
                str(source_path),
                "--colorconfig",
                OCIO_CONFIG_PATH,
                "--colorconvert",
                color_space,
                "Output - sRGB",
                "-o",
                str(srgb_exr),
            ],
            check=False,
        )
        if srgb_exr.exists():
            processable_path = srgb_exr

    # Step B: Extract WebP frames every 2s (0.5 fps)
    frame_pattern = work_dir / "frame_%04d.webp"
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(processable_path),
            "-vf",
            "fps=0.5,scale=1024:-1",
            "-q:v",
            "82",
            str(frame_pattern),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    extracted = []
    for idx, frame_file in enumerate(sorted(work_dir.glob("frame_*.webp"))):
        extracted.append(
            {
                "frame_num": idx * 48 + 1,
                "timestamp_sec": round(idx * 2.0, 2),
                "local_path": frame_file,
            }
        )
    return extracted


async def process_ingest_pipeline(tenant: TenantContext, req: IngestRequest):
    """
    Background worker (swap BackgroundTasks for Temporal.io / Celery in high-volume prod).
    Downloads from private S3/R2 -> Parses USD/OCIO/FFmpeg -> Embeds via Voyage -> Upserts Atlas.
    """
    now = datetime.now(timezone.utc)
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        local_source = work_dir / Path(req.source_key).name

        # 1. Download raw media from private bucket (if present)
        has_local_media = False
        try:
            s3_client.download_file(S3_BUCKET, req.source_key, str(local_source))
            has_local_media = True
        except Exception:
            has_local_media = False

        # 2. Merge explicit USD dependencies with parsed USD layers
        parsed_deps = parse_usd_dependencies(local_source) if has_local_media else []
        merged_usd_deps = list(dict.fromkeys(req.usd_dependency_codes + parsed_deps))

        # 3. Extract frames (or synthesize a single metadata-only frame if non-visual asset like a rig)
        frames = (
            extract_color_managed_keyframes(local_source, work_dir, req.color_space)
            if has_local_media
            else []
        )

        hero_thumb_key = f"thumbs/{tenant.project_id}/{req.code}_f001.webp"

        # 4. Upsert parent document in `entities`
        entity_doc = {
            "org_id": tenant.org_id,
            "project_id": tenant.project_id,
            "entity_type": req.entity_type,
            "code": req.code,
            "status": req.status,
            "department": req.department,
            "lineage": {
                "parent_code": req.parent_code,
                "usd_dependency_codes": merged_usd_deps,
            },
            "media": {
                "source_key": req.source_key,
                "proxy_stream_key": f"proxies/{tenant.project_id}/{req.code}.mp4",
                "hero_thumb_key": hero_thumb_key,
                "color_space": req.color_space,
            },
            "context": {
                "artist": req.artist,
                "notes": req.notes,
                "generative_metadata": req.generative_metadata,
            },
            "external_sync": {
                "tracker": "shotgrid",
                "external_id": req.external_id,
                "last_synced_at": now,
            },
            "updated_at": now,
        }

        await entities_col.update_one(
            {"org_id": tenant.org_id, "project_id": tenant.project_id, "code": req.code},
            {"$set": entity_doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        parent = await entities_col.find_one(
            {"org_id": tenant.org_id, "project_id": tenant.project_id, "code": req.code}
        )
        entity_id = parent["_id"]

        # 5. Embed & bulk-write keyframes (handles both video frames and non-visual USD/rig assets)
        text_context = f"{req.code} | {req.department} | {req.notes} | USD: {' '.join(merged_usd_deps)}"
        keyframe_docs = []

        if frames:
            for f in frames:
                thumb_key = f"thumbs/{tenant.project_id}/{req.code}_f{f['frame_num']:04d}.webp"
                img_bytes = f["local_path"].read_bytes()
                try:
                    s3_client.upload_file(str(f["local_path"]), S3_BUCKET, thumb_key)
                except Exception:
                    pass

                vec = await get_voyage_multimodal_embedding(text_context, img_bytes, "document")
                keyframe_docs.append(
                    {
                        "org_id": tenant.org_id,
                        "project_id": tenant.project_id,
                        "entity_id": entity_id,
                        "code": req.code,
                        "status": req.status,
                        "department": req.department,
                        "artist": req.artist,
                        "notes": req.notes,
                        "usd_dependency_codes": merged_usd_deps,
                        "timestamp_sec": f["timestamp_sec"],
                        "frame_num": f["frame_num"],
                        "thumb_key": thumb_key,
                        "multimodal_embedding": vec,
                    }
                )
        else:
            # Cold-start / non-visual asset (e.g. .usd rig or shader without render)
            vec = await get_voyage_multimodal_embedding(text_context, None, "document")
            keyframe_docs.append(
                {
                    "org_id": tenant.org_id,
                    "project_id": tenant.project_id,
                    "entity_id": entity_id,
                    "code": req.code,
                    "status": req.status,
                    "department": req.department,
                    "artist": req.artist,
                    "notes": req.notes,
                    "usd_dependency_codes": merged_usd_deps,
                    "timestamp_sec": 0.0,
                    "frame_num": 1,
                    "thumb_key": None,
                    "multimodal_embedding": vec,
                }
            )

        await keyframes_col.delete_many(
            {"org_id": tenant.org_id, "project_id": tenant.project_id, "entity_id": entity_id}
        )
        if keyframe_docs:
            await keyframes_col.insert_many(keyframe_docs)


# ==============================================================================
# 5. API ENDPOINTS (Ingest, Hybrid Search, USD Lineage, Webhooks, Live Stream)
# ==============================================================================

@app.on_event("startup")
async def ensure_indexes():
    await entities_col.create_index([("org_id", 1), ("project_id", 1), ("code", 1)], unique=True)
    await keyframes_col.create_index([("org_id", 1), ("project_id", 1), ("entity_id", 1), ("frame_num", 1)])


@app.post("/api/v1/ingest", status_code=202)
async def ingest_media(
    req: IngestRequest,
    background_tasks: BackgroundTasks,
    tenant: TenantContext = Depends(get_tenant),
):
    """Triggered by S3 ObjectCreated event or DCC (Houdini/Maya/Nuke) Python publish hook."""
    background_tasks.add_task(process_ingest_pipeline, tenant, req)
    return {
        "accepted": True,
        "org_id": tenant.org_id,
        "project_id": tenant.project_id,
        "code": req.code,
    }


@app.get("/api/v1/search")
async def hybrid_search(
    q: str = Query(..., min_length=1, description="Natural language or USD/shot code query"),
    department: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(20, ge=1, le=50),
    tenant: TenantContext = Depends(get_tenant),
):
    """
    Executes MongoDB Atlas `$rankFusion` (Voyage Multimodal Vector + Atlas Lexical Search),
    deduplicates by `entity_id` while preserving the highest-scoring timestamp,
    joins parent entity metadata, and mints 15-minute presigned R2/S3 URLs.
    """
    query_vector = await get_voyage_multimodal_embedding(q, None, input_type="query")

    vector_filter: Dict[str, Any] = {
        "org_id": {"$eq": tenant.org_id},
        "project_id": {"$eq": tenant.project_id},
    }
    lexical_filters: List[Dict[str, Any]] = [
        {"equals": {"path": "org_id", "value": tenant.org_id}},
        {"equals": {"path": "project_id", "value": tenant.project_id}},
    ]

    if department:
        vector_filter["department"] = {"$eq": department}
        lexical_filters.append({"equals": {"path": "department", "value": department}})
    if status:
        vector_filter["status"] = {"$eq": status}
        lexical_filters.append({"equals": {"path": "status", "value": status}})

    pipeline = [
        {
            "$rankFusion": {
                "input": {
                    "pipelines": {
                        "semantic": [
                            {
                                "$vectorSearch": {
                                    "index": "frame_vector_idx",
                                    "path": "multimodal_embedding",
                                    "queryVector": query_vector,
                                    "numCandidates": 150,
                                    "limit": 40,
                                    "filter": vector_filter,
                                }
                            }
                        ],
                        "lexical": [
                            {
                                "$search": {
                                    "index": "frame_lexical_idx",
                                    "compound": {
                                        "must": [
                                            {
                                                "text": {
                                                    "query": q,
                                                    "path": [
                                                        "code",
                                                        "notes",
                                                        "artist",
                                                        "usd_dependency_codes",
                                                    ],
                                                }
                                            }
                                        ],
                                        "filter": lexical_filters,
                                    },
                                }
                            },
                            {"$limit": 40},
                        ],
                    }
                }
            }
        },
        {"$addFields": {"fusion_score": {"$meta": "rankFusionScore"}}},
        # Deduplicate multiple frame hits from the same version while keeping best timestamp
        {
            "$group": {
                "_id": "$entity_id",
                "best_score": {"$first": "$fusion_score"},
                "matched_frame_num": {"$first": "$frame_num"},
                "matched_timestamp_sec": {"$first": "$timestamp_sec"},
                "matched_thumb_key": {"$first": "$thumb_key"},
                "matching_timestamps": {"$push": "$timestamp_sec"},
            }
        },
        {"$sort": {"best_score": -1}},
        {"$limit": limit},
        {
            "$lookup": {
                "from": "entities",
                "localField": "_id",
                "foreignField": "_id",
                "as": "entity",
            }
        },
        {"$unwind": "$entity"},
    ]

    results = []
    async for doc in keyframes_col.aggregate(pipeline):
        ent = doc["entity"]
        media = ent.get("media", {})
        results.append(
            {
                "entity_id": str(ent["_id"]),
                "code": ent["code"],
                "entity_type": ent["entity_type"],
                "status": ent["status"],
                "department": ent["department"],
                "score": round(doc.get("best_score", 0.0), 4),
                "matched_frame": {
                    "frame_num": doc["matched_frame_num"],
                    "timestamp_sec": doc["matched_timestamp_sec"],
                    "thumb_url": mint_presigned_url(doc.get("matched_thumb_key")),
                    "all_matching_timestamps": sorted(list(set(doc["matching_timestamps"])))[:5],
                },
                "media_urls": {
                    "proxy_stream_url": mint_presigned_url(media.get("proxy_stream_key")),
                    "hero_thumb_url": mint_presigned_url(media.get("hero_thumb_key")),
                },
                "lineage": ent.get("lineage", {}),
                "context": ent.get("context", {}),
            }
        )

    return {"query": q, "count": len(results), "results": results}


@app.get("/api/v1/entities/{code}/lineage")
async def get_usd_lineage(code: str, tenant: TenantContext = Depends(get_tenant)):
    """Traverses OpenUSD dependency graphs via MongoDB `$graphLookup`."""
    pipeline = [
        {
            "$match": {
                "org_id": tenant.org_id,
                "project_id": tenant.project_id,
                "code": code,
            }
        },
        {
            "$graphLookup": {
                "from": "entities",
                "startWith": "$lineage.usd_dependency_codes",
                "connectFromField": "lineage.usd_dependency_codes",
                "connectToField": "code",
                "as": "dependency_tree",
                "maxDepth": 10,
                "restrictSearchWithMatch": {
                    "org_id": tenant.org_id,
                    "project_id": tenant.project_id,
                },
            }
        },
    ]

    cursor = entities_col.aggregate(pipeline)
    docs = await cursor.to_list(length=1)
    if not docs:
        raise HTTPException(status_code=404, detail=f"Entity '{code}' not found")

    root = docs[0]
    tree = [
        {
            "code": node["code"],
            "entity_type": node["entity_type"],
            "department": node["department"],
            "status": node["status"],
            "usd_dependency_codes": node.get("lineage", {}).get("usd_dependency_codes", []),
        }
        for node in root.get("dependency_tree", [])
    ]
    return {
        "root_code": root["code"],
        "direct_dependencies": root.get("lineage", {}).get("usd_dependency_codes", []),
        "resolved_tree": tree,
    }


@app.post("/api/v1/webhooks/shotgrid")
async def shotgrid_webhook(
    payload: ShotGridWebhookPayload,
    x_gridlock_sync_id: Optional[str] = Header(default=None),
):
    """
    Loop-safe bidirectional sync with ShotGrid / ftrack.
    Drops echo events originated by Gridlock and atomically syncs denormalized keyframe metadata.
    """
    if x_gridlock_sync_id:
        return {"status": "ignored", "reason": "gridlock_origin_header"}

    existing = await entities_col.find_one(
        {
            "org_id": payload.org_id,
            "project_id": payload.project_id,
            "code": payload.entity_code,
        }
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Target entity not found")

    if existing.get("external_sync", {}).get("last_sync_id") == payload.event_id:
        return {"status": "ignored", "reason": "duplicate_event_id"}

    entity_updates: Dict[str, Any] = {
        "external_sync.last_sync_id": payload.event_id,
        "external_sync.last_synced_at": datetime.now(timezone.utc),
    }
    keyframe_updates: Dict[str, Any] = {}

    if payload.status is not None:
        entity_updates["status"] = payload.status
        keyframe_updates["status"] = payload.status
    if payload.notes is not None:
        entity_updates["context.notes"] = payload.notes
        keyframe_updates["notes"] = payload.notes
    if payload.artist is not None:
        entity_updates["context.artist"] = payload.artist
        keyframe_updates["artist"] = payload.artist

    await entities_col.update_one({"_id": existing["_id"]}, {"$set": entity_updates})

    if keyframe_updates:
        await keyframes_col.update_many(
            {
                "org_id": payload.org_id,
                "project_id": payload.project_id,
                "entity_id": existing["_id"],
            },
            {"$set": keyframe_updates},
        )

    return {"status": "synced", "entity_code": payload.entity_code, "event_id": payload.event_id}


@app.websocket("/ws/stream/{org_id}/{project_id}")
async def project_change_stream(websocket: WebSocket, org_id: str, project_id: str):
    """
    Streams real-time MongoDB Change Stream events on `entities` to connected studio clients,
    injecting fresh 15-minute presigned thumbnail URLs on the fly.
    """
    await websocket.accept()
    pipeline = [
        {
            "$match": {
                "operationType": {"$in": ["insert", "update", "replace"]},
                "fullDocument.org_id": org_id,
                "fullDocument.project_id": project_id,
            }
        }
    ]
    try:
        async with entities_col.watch(pipeline, full_document="updateLookup") as stream:
            async for change in stream:
                doc = change.get("fullDocument")
                if not doc:
                    continue
                hero_key = doc.get("media", {}).get("hero_thumb_key")
                await websocket.send_json(
                    {
                        "event": change["operationType"],
                        "code": doc["code"],
                        "status": doc["status"],
                        "department": doc["department"],
                        "hero_thumb_url": mint_presigned_url(hero_key),
                    }
                )
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        await websocket.close(code=1011, reason=str(exc))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("mvp-backend:app", host="0.0.0.0", port=8000, reload=True)
