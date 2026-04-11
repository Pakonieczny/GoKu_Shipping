/* netlify/functions/claudeRosterStageAB-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER — STAGE A/B VISUAL MATCHING — v6.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout).
   Returns 202 immediately. Writes result to Firebase when done.
   Frontend polls ai_asset_roster_pending.json to detect completion.

   Key change from v5: CSV-driven category pre-filtering.
   ─────────────────────────────────────────────────────────────────
   Flow:
     1. Read Phase 1 result from ai_asset_roster_phase1.json.
        Phase 1 includes rankedCategories (2 to 6) per 3D object,
        sorted by likelihoodPercent from highest to lowest.
     2. Read user reference images from ai_roster_ref_images.json.
     3. Read global CSV from game-generator-1/projects/BASE_Files/asset_3d_objects/
        reorganized_assets_manifest.csv → build assetName→category map.
     4. Scan ONLY the zip files whose asset_name maps to one of the
        rankedCategories / suggestedCategories for each requirement.
        If fewer than 2 valid CSV-backed categories remain, skip object search for that requirement
        rather than falling back to the full library.
     5. STAGE A — image-vs-image batch scan on the filtered asset pool.
        Particles: text description vs thumbnails (unchanged).
        3D Objects: user reference image vs filtered thumbnails.
     6. STAGE B — per-requirement final visual pick (unchanged).
     7. Assemble final roster, enforce limits, save pending.json.

   Global asset paths (shared across all projects):
     CSV:  game-generator-1/projects/BASE_Files/asset_3d_objects/reorganized_assets_manifest.csv
     Zips: game-generator-1/projects/BASE_Files/asset_3d_objects/{asset_name}.zip

   Request body: { projectPath, jobId }
   Response:     202 Accepted (background function — no body)
   ═══════════════════════════════════════════════════════════════════ */

const fetch  = require("node-fetch");
const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");

/* ─── Constants ──────────────────────────────────────────────────── */
const GLOBAL_ASSET_BASE    = "game-generator-1/projects/BASE_Files/asset_3d_objects";
const GLOBAL_ASSET_CSV     = `${GLOBAL_ASSET_BASE}/reorganized_assets_manifest.csv`;

const CLAUDE_MAX_RETRIES   = 5;
const CLAUDE_BASE_DELAY_MS = 1250;
const CLAUDE_MAX_DELAY_MS  = 12000;

const MAX_OBJ_ASSETS       = 50;
const MAX_PNG_ASSETS       = 50;
const IMAGES_PER_BATCH     = 50;
const MIN_SUGGESTED_CATS   = 2;
const MAX_SUGGESTED_CATS   = 6;   // hard cap — Phase 1 enforces this too
const MAX_AVATAR_ASSETS   = 20;
const AVATARS_ZIP_PRIMARY_PATH = `${GLOBAL_ASSET_BASE}/Avatars.zip`;
const AVATARS_ZIP_LEGACY_PATH  = "game-generator-1/projects/BASE_Files/avatar_assets/Avatars.zip";

function buildAvatarZipPathCandidates(requestedPath = "") {
  return [...new Set([
    requestedPath,
    AVATARS_ZIP_PRIMARY_PATH,
    AVATARS_ZIP_LEGACY_PATH
  ].filter(Boolean))];
}

/* ─── Retry helpers ──────────────────────────────────────────────── */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function computeRetryDelay(attempt) {
  return Math.min(
    CLAUDE_BASE_DELAY_MS * Math.pow(2, Math.max(0, attempt - 1)),
    CLAUDE_MAX_DELAY_MS
  ) + Math.floor(Math.random() * 700);
}

function isOverload(status, msg = "") {
  const m = String(msg).toLowerCase();
  if ([429, 500, 502, 503, 504, 529].includes(Number(status))) return true;
  if (
    m.includes("econnreset")     ||
    m.includes("econnrefused")   ||
    m.includes("etimedout")      ||
    m.includes("enotfound")      ||
    m.includes("socket hang up") ||
    m.includes("network error")  ||
    m.includes("fetch failed")
  ) return true;
  return m.includes("overloaded")        ||
         m.includes("rate limit")        ||
         m.includes("too many requests") ||
         m.includes("capacity")          ||
         m.includes("temporarily unavailable");
}

async function callClaude(apiKey, { model, maxTokens, system, userContent }) {
  const body = {
    model,
    max_tokens: maxTokens,
    system,
    messages: [{ role: "user", content: userContent }]
  };
  let last;
  for (let i = 1; i <= CLAUDE_MAX_RETRIES; i++) {
    try {
      const res = await fetch("https://api.anthropic.com/v1/messages", {
        method:  "POST",
        headers: {
          "Content-Type":      "application/json",
          "x-api-key":         apiKey,
          "anthropic-version": "2023-06-01"
        },
        body: JSON.stringify(body)
      });
      const raw  = await res.text();
      const data = raw ? JSON.parse(raw) : null;
      if (!res.ok) {
        const msg = data?.error?.message || `Claude error (${res.status})`;
        const err = Object.assign(new Error(msg), {
          status: res.status,
          isRetryableOverload: isOverload(res.status, msg)
        });
        throw err;
      }
      const text = data?.content?.find(b => b.type === "text")?.text;
      if (!text) throw new Error("Empty response from Claude");
      return { text, usage: data?.usage || null };
    } catch (err) {
      last = err;
      if (!err.isRetryableOverload && !isOverload(err.status, err.message)) throw err;
      if (i >= CLAUDE_MAX_RETRIES) throw err;
      await sleep(computeRetryDelay(i));
    }
  }
  throw last;
}

/* ─── CSV parsing ────────────────────────────────────────────────── */
function parseCsvRows(csvText) {
  const rows = [];
  let row = [];
  let field = '';
  let i = 0;
  let inQuotes = false;

  while (i < csvText.length) {
    const ch = csvText[i];

    if (inQuotes) {
      if (ch === '"') {
        if (csvText[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ',') {
      row.push(field);
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      i += 1;
      continue;
    }
    if (ch === '\r') {
      i += 1;
      continue;
    }

    field += ch;
    i += 1;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter(r => r.some(cell => String(cell || '').trim() !== ''));
}

// Returns { map: Map<assetName (lowercase), category>, categories: Set<category> }
function parseCsvIndex(csvText) {
  const rows = parseCsvRows(csvText);
  if (rows.length === 0) throw new Error('CSV is empty');

  const header = rows[0].map(h => h.trim().toLowerCase());
  const nameIdx = header.indexOf('asset_name');
  const catIdx  = header.indexOf('new_category');
  if (nameIdx === -1 || catIdx === -1) {
    throw new Error("CSV missing 'asset_name' or 'new_category' column");
  }

  const map = new Map();
  const categories = new Set();
  for (let i = 1; i < rows.length; i++) {
    const name = (rows[i][nameIdx] || '').trim().toLowerCase();
    const cat  = (rows[i][catIdx]  || '').trim();
    if (name && cat) {
      map.set(name, cat);
      categories.add(cat);
    }
  }

  return { map, categories };
}

function clampLikelihoodPercent(value, fallback = 50) {
  const n = Number(value);
  if (!Number.isFinite(n)) return Math.max(0, Math.min(100, Math.round(fallback)));
  return Math.max(0, Math.min(100, Math.round(n)));
}

function normalizeRequirementCategoryRanking(req = {}) {
  const normalized = [];
  const seen = new Set();
  const rankedSource = Array.isArray(req.rankedCategories) && req.rankedCategories.length > 0
    ? req.rankedCategories
    : Array.isArray(req.suggestedCategories)
      ? req.suggestedCategories.map((category, index) => ({
          category,
          likelihoodPercent: Math.max(1, 100 - (index * 5))
        }))
      : [];

  for (let index = 0; index < rankedSource.length; index++) {
    const entry = rankedSource[index];
    const category = String(
      typeof entry === 'string'
        ? entry
        : (entry?.category || entry?.name || '')
    ).trim().replace(/ /g, '_');
    if (!category) continue;

    const dedupeKey = category.toLowerCase();
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    normalized.push({
      category,
      likelihoodPercent: clampLikelihoodPercent(
        typeof entry === 'string' ? 100 - (index * 5) : entry?.likelihoodPercent,
        100 - (index * 5)
      )
    });
  }

  normalized.sort((a, b) => {
    if (b.likelihoodPercent !== a.likelihoodPercent) return b.likelihoodPercent - a.likelihoodPercent;
    return a.category.localeCompare(b.category);
  });

  return normalized.slice(0, MAX_SUGGESTED_CATS);
}

/* ─── Utilities ──────────────────────────────────────────────────── */
function stripFences(text) {
  let t = text
    .replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/\s*```$/i, "").trim();
  const a = t.indexOf("{"), b = t.lastIndexOf("}");
  if (a > 0 && b > a) t = t.substring(a, b + 1);
  return t.trim();
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}


function normalizeAvatarRole(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "avatar";
}

function parseAnimationsTxt(raw = "") {
  return Array.from(new Set(
    String(raw || "")
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean)
      .map(line => {
        const match = line.match(/^[-*]?\s*([^:]+?)(?:\s*:\s*(.*))?$/);
        return String(match?.[1] || line).trim();
      })
      .filter(Boolean)
  ));
}

function normalizeAnimationNeedToBuckets(need = '') {
  const lower = String(need || '').toLowerCase();
  if (!lower) return [];
  if (/idle|stand|breath/.test(lower)) return ['idle'];
  if (/move|walk|step/.test(lower)) return ['walk'];
  if (/run|sprint|jog/.test(lower)) return ['run'];
  if (/jump|hop|leap/.test(lower)) return ['jump'];
  if (/attack_or_action|attack|melee|strike|slash|swing|punch|kick/.test(lower)) return ['attack_melee', 'attack_ranged'];
  if (/shoot|fire|aim|ranged/.test(lower)) return ['attack_ranged'];
  if (/hurt|hit|damage|flinch|pain/.test(lower)) return ['hurt'];
  if (/death|die|dead|dying/.test(lower)) return ['death'];
  if (/reload/.test(lower)) return ['reload'];
  if (/crouch|duck/.test(lower)) return ['crouch'];
  if (/strafe|sidestep/.test(lower)) return ['strafe'];
  if (/celebrate|victory|cheer|taunt/.test(lower)) return ['celebrate'];
  if (/fall|airborne/.test(lower)) return ['fall'];
  if (/land|touchdown/.test(lower)) return ['land'];
  return [lower.replace(/[^a-z0-9]+/g, '_')];
}

function scoreAnimationCoverage(requirement = {}, clips = []) {
  const needs = Array.isArray(requirement.animationNeeds) ? requirement.animationNeeds : [];
  const BUCKET_PATTERNS = {
    idle: /idle|stand|breathing/i,
    walk: /walk|step/i,
    run: /run|sprint|jog/i,
    jump: /jump|leap|hop/i,
    attack_melee: /attack|slash|strike|swing|melee|punch|kick/i,
    attack_ranged: /shoot|fire|aim|ranged/i,
    hurt: /hurt|hit|damage|flinch|pain/i,
    death: /death|die|dying|dead/i,
    reload: /reload/i,
    crouch: /crouch|duck/i,
    strafe: /strafe|sidestep/i,
    celebrate: /celebrate|victory|cheer|taunt/i,
    fall: /fall|falling|airborne/i,
    land: /land|touchdown/i,
  };
  const normalizedBuckets = {};
  for (const clip of clips) {
    for (const [bucket, pattern] of Object.entries(BUCKET_PATTERNS)) {
      if (pattern.test(String(clip || ''))) {
        normalizedBuckets[bucket] = normalizedBuckets[bucket] || [];
        normalizedBuckets[bucket].push(clip);
      }
    }
  }
  if (needs.length === 0) {
    return {
      required: [],
      matched: [],
      missing: [],
      score: clips.length > 0 ? 1 : 0,
      coveragePercent: clips.length > 0 ? 100 : 0,
      normalizedBuckets
    };
  }
  const matched = needs.filter(need => {
    const buckets = normalizeAnimationNeedToBuckets(need);
    return buckets.some(bucket => Array.isArray(normalizedBuckets[bucket]) && normalizedBuckets[bucket].length > 0)
      || clips.some(clip => {
        const lowerClip = String(clip || '').toLowerCase();
        const lowerNeed = String(need || '').toLowerCase();
        return lowerClip.includes(lowerNeed) || lowerNeed.includes(lowerClip);
      });
  });
  return {
    required: needs,
    matched,
    missing: needs.filter(need => !matched.includes(need)),
    score: matched.length / Math.max(1, needs.length),
    coveragePercent: Math.round((matched.length / Math.max(1, needs.length)) * 100),
    normalizedBuckets
  };
}

function splitMatchTokens(value = '') {
  return String(value || '')
    .replace(/\u0000/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)
    .filter(token => !/^(mat|material|mesh|slot|default|obj|fbx|glb|gltf|mesh[0-9]+|[a-z]|[0-9]+)$/.test(token));
}

function scoreOrderedTokenAlignment(sharedTokens = [], leftTokens = [], rightTokens = []) {
  let score = 0;
  let rightCursor = -1;
  for (const token of sharedTokens) {
    const leftIndex = leftTokens.indexOf(token);
    const rightIndex = rightTokens.indexOf(token, rightCursor + 1);
    if (leftIndex >= 0 && rightIndex >= 0) {
      score += 5;
      rightCursor = rightIndex;
    }
  }
  return score;
}

function scoreTextureCandidates(materials = [], textureFileList = []) {
  const textureEntries = (Array.isArray(textureFileList) ? textureFileList : []).map((entryPath) => {
    const base = String(entryPath || '').split('/').pop() || String(entryPath || '');
    const lower = base.toLowerCase();
    const nameNoExt = base.replace(/\.[^.]+$/, '');
    return { entryPath, base, lower, nameNoExt, tokens: splitMatchTokens(nameNoExt) };
  });

  const contracts = (Array.isArray(materials) ? materials : []).map((material, index) => {
    const materialName = String(material?.name || `slot_${index}`);
    const materialLower = materialName.toLowerCase();
    const materialNameNoExt = materialName.replace(/\.[^.]+$/, '').toLowerCase();
    const materialTokens = splitMatchTokens(materialName);
    const ranked = textureEntries.map((texture) => {
      let score = 0;
      const reasons = [];
      const sharedTokens = materialTokens.filter(token => texture.tokens.includes(token));

      if (texture.nameNoExt.toLowerCase() === materialNameNoExt) {
        score += 100;
        reasons.push('exact full-name match');
      }
      if (sharedTokens.length > 0) {
        score += sharedTokens.length * 10;
        reasons.push(`shared tokens: ${sharedTokens.join(', ')}`);
      }
      const orderedScore = scoreOrderedTokenAlignment(sharedTokens, materialTokens, texture.tokens);
      if (orderedScore > 0) {
        score += orderedScore;
        reasons.push('ordered token alignment');
      }
      if (materialNameNoExt && (texture.nameNoExt.toLowerCase().includes(materialNameNoExt) || materialNameNoExt.includes(texture.nameNoExt.toLowerCase()))) {
        score += 8;
        reasons.push('substring containment');
      }
      if (/(diffuse|albedo|color|col|basecolor)/.test(texture.lower)) {
        score += 15;
        reasons.push('albedo/color suffix match');
      }
      if (/(thumbnail|preview|render|thumb)/.test(texture.lower)) {
        score -= 30;
        reasons.push('preview/thumbnail penalty');
      }
      if (sharedTokens.some(token => token.length <= 1 || /^[0-9]+$/.test(token))) {
        score -= 5;
        reasons.push('generic token penalty');
      }
      return {
        entryPath: texture.entryPath,
        base: texture.base,
        score,
        reason: reasons.join('; ') || 'no strong token evidence'
      };
    }).sort((a, b) => b.score - a.score || a.base.localeCompare(b.base));

    const best = ranked[0] || null;
    const second = ranked[1] || null;
    const scoreGap = best ? (best.score - (second?.score || 0)) : 0;
    let confidence = 'unresolved';
    let boundTexture = null;
    let ambiguous = false;
    if (best) {
      if (best.score >= 40) {
        confidence = 'high';
        boundTexture = best.entryPath;
      } else if (best.score >= 15 && scoreGap >= 10) {
        confidence = 'medium';
        boundTexture = best.entryPath;
      } else if (best.score < 15) {
        confidence = 'low';
      } else {
        ambiguous = true;
      }
    }
    return {
      materialName,
      slot: Number(material?.index ?? index),
      boundTexture,
      confidence,
      score: best?.score || 0,
      reason: best?.reason || 'no candidate textures available',
      secondBest: second?.entryPath || null,
      ambiguous
    };
  });

  const confidentMatches = contracts.filter(contract => contract.boundTexture && (contract.confidence === 'high' || contract.confidence === 'medium'));
  if (confidentMatches.length === 0 && contracts.length > 0) {
    const globalColormap = textureEntries.find(texture => /^colormap$/i.test(texture.nameNoExt));
    if (globalColormap) {
      return contracts.map(contract => ({
        ...contract,
        boundTexture: globalColormap.entryPath,
        confidence: 'medium',
        score: Math.max(contract.score, 15),
        reason: 'global colormap fallback because zero confident slot matches existed',
        ambiguous: false
      }));
    }
  }

  return contracts.map(contract => {
    if (contract.confidence === 'low' || contract.ambiguous) {
      return { ...contract, boundTexture: null, confidence: contract.confidence === 'low' ? 'low' : 'unresolved' };
    }
    return contract;
  });
}

let _threeFbxRuntimePromise = null;

async function getThreeFbxRuntime() {
  if (!_threeFbxRuntimePromise) {
    _threeFbxRuntimePromise = (async () => {
      const THREE = await import('three');
      const { FBXLoader } = await import('three/examples/jsm/loaders/FBXLoader.js');
      return { THREE, FBXLoader };
    })();
  }
  return _threeFbxRuntimePromise;
}

function nodeBufferToArrayBuffer(bufferLike) {
  if (bufferLike instanceof ArrayBuffer) return bufferLike;
  if (!Buffer.isBuffer(bufferLike)) {
    throw new Error('scanFbxBuffer expected a Node Buffer or ArrayBuffer');
  }
  return bufferLike.buffer.slice(bufferLike.byteOffset, bufferLike.byteOffset + bufferLike.byteLength);
}

function collectThreeMaterialNamesForAvatar(root) {
  const names = [];
  const seen = new Set();
  root?.traverse?.((node) => {
    const mats = Array.isArray(node?.material) ? node.material : (node?.material ? [node.material] : []);
    for (const mat of mats) {
      const label = String(mat?.name || mat?.type || 'UnnamedMaterial').trim() || 'UnnamedMaterial';
      if (seen.has(label)) continue;
      seen.add(label);
      names.push(label);
    }
  });
  return names;
}

function estimateMaterialSlotCountForAvatar(root) {
  let slotCount = 0;
  let sawMesh = false;
  root?.traverse?.((node) => {
    if (!node?.isMesh) return;
    sawMesh = true;
    const mats = Array.isArray(node.material) ? node.material : (node.material ? [node.material] : []);
    slotCount += Math.max(1, mats.length);
  });
  if (slotCount > 0) return slotCount;
  const uniqueMaterials = collectThreeMaterialNamesForAvatar(root).length;
  if (uniqueMaterials > 0) return uniqueMaterials;
  return sawMesh ? 1 : 0;
}

function detectDominantAxisForAvatar(size = {}) {
  const dims = [
    { axis: 'x', value: Math.abs(Number(size.x || 0)) },
    { axis: 'y', value: Math.abs(Number(size.y || 0)) },
    { axis: 'z', value: Math.abs(Number(size.z || 0)) }
  ].sort((a, b) => b.value - a.value);
  const dominantAxis = dims[0]?.axis || 'z';
  return {
    dominantAxis,
    forwardHint: dominantAxis === 'x' ? 'x' : 'z'
  };
}

function buildThreeFbxGeometryAnalysis(sceneOrRoot, sourceName = '', THREE = null) {
  if (!sceneOrRoot) return null;
  if (!THREE?.Box3) throw new Error('THREE.Box3 runtime unavailable for FBX analysis');

  sceneOrRoot.updateMatrixWorld?.(true);
  sceneOrRoot.traverse?.((node) => {
    if (node?.isMesh && typeof node.geometry?.computeBoundingBox === 'function') {
      node.geometry.computeBoundingBox();
    }
  });

  const box = new THREE.Box3().setFromObject(sceneOrRoot);
  const finite = [box.min?.x, box.min?.y, box.min?.z, box.max?.x, box.max?.y, box.max?.z].every(Number.isFinite);
  const min = finite ? {
    x: Number(box.min.x || 0),
    y: Number(box.min.y || 0),
    z: Number(box.min.z || 0)
  } : { x: 0, y: 0, z: 0 };
  const max = finite ? {
    x: Number(box.max.x || 0),
    y: Number(box.max.y || 0),
    z: Number(box.max.z || 0)
  } : { x: 0, y: 0, z: 0 };
  const size = {
    x: Number((max.x - min.x).toFixed(6)),
    y: Number((max.y - min.y).toFixed(6)),
    z: Number((max.z - min.z).toFixed(6))
  };
  const centroid = {
    x: Number((((min.x + max.x) / 2) || 0).toFixed(6)),
    y: Number((((min.y + max.y) / 2) || 0).toFixed(6)),
    z: Number((((min.z + max.z) / 2) || 0).toFixed(6))
  };

  let meshCount = 0;
  let vertexCount = 0;
  sceneOrRoot.traverse?.((node) => {
    if (!node?.isMesh) return;
    meshCount += 1;
    const posAttr = node.geometry?.attributes?.position;
    if (posAttr?.count) vertexCount += posAttr.count;
  });

  const slotCount = estimateMaterialSlotCountForAvatar(sceneOrRoot);
  const materialNames = collectThreeMaterialNamesForAvatar(sceneOrRoot);
  const maxDim = Math.max(Math.abs(size.x), Math.abs(size.y), Math.abs(size.z), 0);
  const normalizedToOneUnit = maxDim > 0 ? Number((1 / maxDim).toFixed(6)) : 1;
  const dominantAxisInfo = detectDominantAxisForAvatar(size);
  const floorY = Number((-min.y).toFixed(6));
  const ceilingY = Number((-max.y).toFixed(6));
  const centerY = Number((-centroid.y).toFixed(6));
  const centerOffsetX = Number((-centroid.x).toFixed(6));
  const centerOffsetZ = Number((-centroid.z).toFixed(6));

  return {
    sourceName,
    format: 'fbx',
    meshCount,
    slotCount,
    vertexCount,
    animationCount: Array.isArray(sceneOrRoot.animations) ? sceneOrRoot.animations.length : 0,
    materials: materialNames,
    boundingBox: {
      width: Number(size.x.toFixed(3)),
      height: Number(size.y.toFixed(3)),
      depth: Number(size.z.toFixed(3))
    },
    center: {
      x: Number(centroid.x.toFixed(3)),
      y: Number(centroid.y.toFixed(3)),
      z: Number(centroid.z.toFixed(3))
    },
    recommendedFloorYOffset: Number(floorY.toFixed(3)),
    geometry: {
      min: {
        x: Number(min.x.toFixed(6)),
        y: Number(min.y.toFixed(6)),
        z: Number(min.z.toFixed(6))
      },
      max: {
        x: Number(max.x.toFixed(6)),
        y: Number(max.y.toFixed(6)),
        z: Number(max.z.toFixed(6))
      },
      size,
      centroid
    },
    scale: {
      authoredUnit: 'unknown',
      unitToGameUnit: 1,
      normalizedToOneUnit,
      suggestedGameScale: normalizedToOneUnit,
      suggestedGameScaleVec: [normalizedToOneUnit, normalizedToOneUnit, normalizedToOneUnit],
      scaleWarning: (normalizedToOneUnit > 5 || normalizedToOneUnit < 0.1) ? 'LARGE SCALE CORRECTION NEEDED' : null
    },
    origin: {
      classification: 'unknown',
      biasY: floorY,
      biasX: centerOffsetX,
      biasZ: centerOffsetZ
    },
    placement: {
      floorY,
      ceilingY,
      centerY,
      centerOffsetX,
      centerOffsetZ,
      dominantAxis: dominantAxisInfo.dominantAxis,
      forwardHint: dominantAxisInfo.forwardHint
    },
    bounds: {
      width: Number(size.x.toFixed(3)),
      height: Number(size.y.toFixed(3)),
      depth: Number(size.z.toFixed(3))
    },
    floorOffset: Number(floorY.toFixed(3)),
    scaleHints: {
      normalizedToOneUnit,
      suggestedGameScale: normalizedToOneUnit,
      suggestedGameScaleVec: [normalizedToOneUnit, normalizedToOneUnit, normalizedToOneUnit]
    },
    multiMeshStructure: {
      isMultiMesh: meshCount > 1 || slotCount > 1,
      meshCount,
      slotCount,
      materialSlots: materialNames.map((materialName, index) => ({ slotIndex: index, materialName }))
    }
  };
}

async function scanFbxBuffer(fbxBuffer, sourceName = '') {
  const { THREE, FBXLoader } = await getThreeFbxRuntime();
  const loader = new FBXLoader();
  const arrayBuffer = nodeBufferToArrayBuffer(fbxBuffer);
  const parsed = loader.parse(arrayBuffer, '');
  if (!parsed) throw new Error(`FBXLoader.parse returned no scene for ${sourceName || 'buffer'}`);

  const geometry = buildThreeFbxGeometryAnalysis(parsed, sourceName, THREE);
  const materials = (geometry?.materials || []).map((name, index) => ({ name, index }));
  return {
    geometry,
    materials,
    meshCount: Number(geometry?.meshCount || 0),
    slotCount: Number(geometry?.slotCount || 0)
  };
}

function listAvatarTextureFiles(zip, folderPrefix) {
  const textures = [];
  for (const entryPath of Object.keys(zip.files)) {
    const entry = zip.files[entryPath];
    if (entry.dir || !entryPath.startsWith(folderPrefix)) continue;
    const base = entryPath.split('/').pop() || '';
    const lower = base.toLowerCase();
    if (base.startsWith('._') || entryPath.includes('__MACOSX')) continue;
    if ([".png",".jpg",".jpeg",".webp",".bmp",".tga"].some(ext => lower.endsWith(ext)) && !/thumbnail\./i.test(base)) {
      textures.push(entryPath);
    }
  }
  return textures.sort();
}

/* ─── Enforce hard selection limits ─────────────────────────────── */
function enforceHardLimits(roster) {
  if (!roster) return roster;
  if (Array.isArray(roster.objects3d) && roster.objects3d.length > MAX_OBJ_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming objects3d from ${roster.objects3d.length} to ${MAX_OBJ_ASSETS}`);
    roster.objects3d = roster.objects3d.slice(0, MAX_OBJ_ASSETS);
  }
  if (Array.isArray(roster.avatars) && roster.avatars.length > MAX_AVATAR_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming avatars from ${roster.avatars.length} to ${MAX_AVATAR_ASSETS}`);
    roster.avatars = roster.avatars.slice(0, MAX_AVATAR_ASSETS);
  }
  if (Array.isArray(roster.textureAssets) && roster.textureAssets.length > MAX_PNG_ASSETS) {
    console.warn(`[ROSTER-AB] Trimming textureAssets from ${roster.textureAssets.length} to ${MAX_PNG_ASSETS}`);
    roster.textureAssets = roster.textureAssets.slice(0, MAX_PNG_ASSETS);
  }
  if (roster.coverageSummary) {
    roster.coverageSummary.totalObjects3d  = (roster.objects3d    || []).length;
    roster.coverageSummary.totalAvatars    = (roster.avatars      || []).length;
    roster.coverageSummary.totalTextures   = (roster.textureAssets || []).length;
    roster.coverageSummary.limitsRespected =
      roster.coverageSummary.totalObjects3d <= MAX_OBJ_ASSETS &&
      roster.coverageSummary.totalAvatars   <= MAX_AVATAR_ASSETS &&
      roster.coverageSummary.totalTextures  <= MAX_PNG_ASSETS;
  }
  return roster;
}

/* ─── Stage A prompt: particle text-vs-image batch scan ─────────── */
function buildStageAParticlePrompt(requirements) {
  const reqList = requirements.map((r, i) =>
    `  ${i + 1}. ${r.name}: ${r.visualDescription}` +
    (r.behaviorDescription ? ` — ${r.behaviorDescription}` : "")
  ).join("\n");

  return `You are a game asset visual screener. You will be shown a batch of particle effect texture thumbnail images.
Your job is to identify which images are plausible visual candidates for any of the particle effect requirements listed below.
Cast a wide net — include anything that could plausibly match, even loosely, but still respect whether the requirement reads more like a burst / impact / spark versus a trail / smoke / lingering streak.

PARTICLE EFFECT REQUIREMENTS:
${reqList}

The images in this batch are numbered sequentially starting at 1.
For each image, list which requirement numbers (1-based) it could satisfy. Use an empty array if none.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesRequirements": [1, 3] },
    { "imageIndex": 2, "matchesRequirements": [] },
    { "imageIndex": 3, "matchesRequirements": [2] }
  ]
}`;
}

/* ─── Stage A prompt: 3D object image-vs-image batch scan ───────── */
function buildStageAObjectRefImagePrompt(requirementName, gameplayRole) {
  return `You are a game asset visual screener matching 3D object thumbnails against a user-provided reference image.

The FIRST image attached is the user's reference image for the requirement:
  Name: ${requirementName}
  Gameplay role: ${gameplayRole || "not specified"}

The remaining images (numbered 1, 2, 3... in your response) are candidate thumbnails from the 3D object library.
Your job: identify which library thumbnails are visually similar enough to the reference image to be a plausible match.
Cast a wide net — include anything that shares the general shape, style, or object category, but prefer candidates that look like final visible gameplay objects rather than generic placeholder geometry.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesReference": true },
    { "imageIndex": 2, "matchesReference": false },
    { "imageIndex": 3, "matchesReference": true }
  ]
}`;
}

/* ─── Stage B prompt: particle text-based final pick ────────────── */
function buildStageBParticlePrompt(requirementName, requirementDesc, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final asset selection for a game. You have been given thumbnail images of candidate particle texture assets. Pick the single best visual match for the requirement below.

REQUIREMENT:
Name: ${requirementName}
Description: ${requirementDesc}
Type: Particle Effect Texture

CANDIDATE THUMBNAILS (images attached in order):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.assetFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- Judge purely by visual appearance vs the requirement description.
- Consider shape silhouette, density, edge softness, color tone, and whether the texture reads more like a burst / impact / spark versus a trail / smoke / lingering streak.
- Pick exactly one winner. State which image number you chose and why.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What you saw in the thumbnail that matched the requirement"
}`;
}

/* ─── Stage B prompt: 3D object image-vs-image final pick ───────── */
function buildStageBObjectRefImagePrompt(requirementName, gameplayRole, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final 3D object asset selection. The FIRST image is the user's reference image showing what the object should look like. The remaining images are candidate library thumbnails.

REQUIREMENT:
Name: ${requirementName}
Gameplay role: ${gameplayRole || "not specified"}

CANDIDATE THUMBNAILS (images 2 onwards, numbered starting at 1 in your response):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.objFile} (${c.sourceZip})`).join("\n")}

SELECTION RULES:
- The reference image (first image) is the target appearance.
- Pick the candidate thumbnail that most closely resembles the reference in shape, silhouette, style, object category, and final in-game readability.
- Prefer richer authored objects over obvious placeholder or low-detail geometry when both satisfy the role.
- Pick exactly one winner. State which candidate image number (1-based, not counting the reference) you chose and why.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "What made this thumbnail most similar to the reference image"
}`;
}


function buildStageAAvatarRefImagePrompt(requirementName, gameplayRole, animationNeeds = []) {
  return `You are a game avatar visual screener matching avatar thumbnails against a user-provided reference image.

The FIRST image attached is the user's reference image for the avatar requirement:
  Name: ${requirementName}
  Gameplay role: ${gameplayRole || "not specified"}
  Animation needs: ${(animationNeeds || []).join(", ") || "not specified"}

The remaining images (numbered 1, 2, 3... in your response) are candidate avatar thumbnails from Avatars.zip.
Your job: identify which avatar thumbnails are visually similar enough to the reference image to be plausible matches for this character role.
Cast a wide net, but prefer candidates that clearly read as final characters rather than props.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "matches": [
    { "imageIndex": 1, "matchesReference": true },
    { "imageIndex": 2, "matchesReference": false }
  ]
}`;
}

function buildStageBAvatarRefImagePrompt(requirementName, gameplayRole, animationNeeds, candidates, gameInterpretation) {
  return `GAME CONTEXT:
${gameInterpretation}

You are making the final avatar asset selection. The FIRST image is the user's reference image. The remaining images are candidate avatar thumbnails.

REQUIREMENT:
Name: ${requirementName}
Gameplay role: ${gameplayRole || "not specified"}
Animation needs: ${(animationNeeds || []).join(", ") || "not specified"}

CANDIDATE THUMBNAILS (images 2 onwards, numbered starting at 1 in your response):
${candidates.map((c, i) => `  Image ${i + 1}: ${c.assetName} (${c.sourceZip}) | clips: ${(c.animationClips || []).join(", ") || "none"}`).join("\n")}

SELECTION RULES:
- The reference image is the target appearance.
- Prefer candidates that best match the role silhouette, costume/type, and likely animation usefulness.
- Animation coverage matters. Break ties in favor of higher clip coverage for the requested role.
- Pick exactly one winner.

Respond ONLY with a valid JSON object. No markdown, no fences, no preamble.

{
  "requirementName": "${requirementName}",
  "imageNumberChosen": 1,
  "visualSelectionRationale": "Why this avatar is the best fit"
}`;
}

/* ═══════════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let bucket      = null;
  let jobId       = null;

  const err400 = msg => ({ statusCode: 400, body: msg });

  try {
    if (!event.body) return { statusCode: 400, body: "" };

    const body = JSON.parse(event.body);
    jobId = body.jobId;
    projectPath = body.projectPath;
    if (!projectPath || !jobId) return { statusCode: 400, body: "" };

    const apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error("Missing ANTHROPIC_API_KEY");

    bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-AB] Starting Stage A/B for project ${projectPath}, job ${jobId}`);

    // ── 1. Load Phase 1 result ───────────────────────────────────────────
    const phase1File = bucket.file(`${projectPath}/ai_asset_roster_phase1.json`);
    const [p1Exists] = await phase1File.exists();
    if (!p1Exists) return err400("ai_asset_roster_phase1.json not found. Run Phase 1 first.");
    const [p1Content] = await phase1File.download();
    const p1Payload   = JSON.parse(p1Content.toString());
    const { phase1 }  = p1Payload;
    if (!phase1) return err400("No phase1 data in ai_asset_roster_phase1.json");

    const particleReqs       = phase1.particleEffects || [];
    const objectReqs         = phase1.objects3d || [];
    const avatarReqs         = phase1.avatarRequirements || [];
    const gameInterpretation = phase1.gameInterpretationSummary || "";
    const requestedAvatarZipPath = p1Payload.avatarPipeline?.zipPath || '';
    const avatarZipPathCandidates = buildAvatarZipPathCandidates(requestedAvatarZipPath);
    if (!avatarZipPathCandidates.length) return err400('Missing avatarPipeline.zipPath in ai_asset_roster_phase1.json');
    let resolvedAvatarZipPath = requestedAvatarZipPath || AVATARS_ZIP_PRIMARY_PATH;

    console.log(`[ROSTER-AB] Phase 1 loaded: ${particleReqs.length} particle req(s), ${objectReqs.length} object req(s), ${avatarReqs.length} avatar req(s)`);

    // ── 2. Load user reference images ───────────────────────────────────
    const refImagesFile = bucket.file(`${projectPath}/ai_roster_ref_images.json`);
    const [refExists]   = await refImagesFile.exists();
    if (!refExists) return err400("ai_roster_ref_images.json not found. Frontend must upload user reference images first.");
    const [refContent]      = await refImagesFile.download();
    const refPayload = JSON.parse(refContent.toString());
    const userRefImages = Array.isArray(refPayload.items)
      ? refPayload.items
      : (Array.isArray(refPayload.objects) ? refPayload.objects.map(item => ({ ...item, requirementType: 'object3d' })) : []);

    const refImageByName = new Map();
    for (const img of userRefImages) {
      if (img.requirementName && img.b64 && img.mimeType) {
        refImageByName.set(img.requirementName.toLowerCase(), img);
      }
    }
    console.log(`[ROSTER-AB] User reference images loaded: ${refImageByName.size} requirement(s) have reference images`);

    // ── 3. Load global CSV → asset_name → category map ──────────────────
    console.log(`[ROSTER-AB] Loading global asset CSV from ${GLOBAL_ASSET_CSV}`);
    const csvFile = bucket.file(GLOBAL_ASSET_CSV);
    const [csvExists] = await csvFile.exists();
    if (!csvExists) throw new Error(`Global asset CSV not found at ${GLOBAL_ASSET_CSV}`);
    const [csvBuffer] = await csvFile.download();
    const { map: assetCategoryMap, categories: knownCategories } = parseCsvIndex(csvBuffer.toString("utf8"));
    console.log(`[ROSTER-AB] CSV loaded: ${assetCategoryMap.size} asset entries`);

    // ── 4. Build per-requirement allowed category sets ───────────────────
    // Map<requirementName, { allowedCats:Set<category>, rankedCategories:Array<{category, likelihoodPercent}> }>
    // An empty allowedCats set means "skip object search".
    const reqCategoryFilter = new Map();
    for (const req of objectReqs) {
      const ranked = normalizeRequirementCategoryRanking(req);
      const validRanked = ranked.filter(entry => {
        const known = knownCategories.has(entry.category);
        if (!known) {
          console.warn(
            `[ROSTER-AB] Req "${req.name}": unknown category "${entry.category}" ` +
            `(${entry.likelihoodPercent}%) — ignoring`
          );
        }
        return known;
      }).slice(0, MAX_SUGGESTED_CATS);

      if (validRanked.length >= 1) {
        const allowedCats = new Set(validRanked.map(entry => entry.category));
        reqCategoryFilter.set(req.name, { allowedCats, rankedCategories: validRanked });
        console.log(
          `[ROSTER-AB] Req "${req.name}": filtering to ${validRanked.length} ranked category(s): ` +
          `${validRanked.map(entry => `${entry.category} (${entry.likelihoodPercent}%)`).join(", ")}`
        );
      } else {
        reqCategoryFilter.set(req.name, { allowedCats: new Set(), rankedCategories: [] });
        console.warn(`[ROSTER-AB] Req "${req.name}": no valid ranked categories — skipping object search for this requirement`);
      }
    }

    // ── 5. Scan particle zip files (project-local, unchanged) ───────────
    const particleAssets = []; // { assetFile, b64, mimeType, sourceZip }
    {
      const particlePrefix = `${projectPath}/asset_particle_textures/`;
      let particleFiles;
      try {
        [particleFiles] = await bucket.getFiles({ prefix: particlePrefix });
      } catch (e) {
        console.warn(`[ROSTER-AB] Could not list particle folder: ${e.message}`);
        particleFiles = [];
      }
      const particleZips = (particleFiles || []).filter(f => f.name.toLowerCase().endsWith(".zip"));
      console.log(`[ROSTER-AB] Particle zips found: ${particleZips.length}`);

      for (const zipFile of particleZips) {
        const sourceZip = zipFile.name.split("/").pop();
        try {
          const [zipBuffer] = await zipFile.download();
          const zip = await JSZip.loadAsync(zipBuffer);
          let added = 0;
          for (const entryPath of Object.keys(zip.files)) {
            if (zip.files[entryPath].dir) continue;
            const base  = entryPath.split("/").pop();
            const lower = base.toLowerCase();
            if (base.startsWith("._")) continue;
            if (![".png", ".jpg", ".jpeg", ".webp"].some(e => lower.endsWith(e))) continue;
            const blob     = await zip.files[entryPath].async("nodebuffer");
            const mimeType = lower.endsWith(".png") ? "image/png" : "image/jpeg";
            particleAssets.push({ assetFile: base, b64: blob.toString("base64"), mimeType, sourceZip });
            added++;
          }
          console.log(`[ROSTER-AB] Particle zip ${sourceZip}: ${added} asset(s) indexed`);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not process particle zip ${sourceZip}: ${e.message}`);
        }
      }
    }

    // ── 6. Scan global 3D object mega-zips, tagged with CSV category ────
    //
    // Zip structure (derived from CSV new_category column):
    //   {TopLevel}.zip / {SubCategory} / {asset_name} / {asset_name}.obj
    //                                                  / {asset_name}.jpg  ← thumbnail
    //                                                  / colormap.jpg      ← texture (locked here)
    //
    // COLORMAP LOCK: Any file whose name contains "colormap" (case-insensitive) is
    // unconditionally treated as that object's texture and locked into the roster as
    // colormapEntryPath (full zip path) at index time. This is the ONE place in the
    // entire pipeline where the obj↔texture match is established. Extract uses the
    // locked colormapEntryPath directly — no re-discovery, no classification logic.
    //
    // CSV new_category = "Architecture_Modular/Floors_Stairs_Pillars"
    //   → zip file:      Architecture_Modular.zip
    //   → internal path: Floors_Stairs_Pillars/{asset_name}/
    //
    // Top-level zip names are derived dynamically from the CSV — adding a
    // 5th zip requires no code changes, just updating the CSV and uploading.
    //
    // Strategy: load each mega-zip ONCE, index ALL assets inside it tagged
    // with their full CSV category. Stage A filters the in-memory array
    // per-requirement — no repeat zip downloads per requirement.
    //
    // objectAssets: { objFile, objEntryPath, thumbFile, colormapFile, colormapEntryPath,
    //                 colormapConfidence, b64, mimeType, sourceZip, assetName, category }

    console.log(`[ROSTER-AB] Scanning global 3D object mega-zips from ${GLOBAL_ASSET_BASE}/`);
    const objectAssets = [];
    {
      // Derive unique top-level zip names from CSV categories dynamically.
      // "Architecture_Modular/Floors_Stairs_Pillars" → "Architecture_Modular"
      const topLevelZipNames = new Set();
      for (const cat of assetCategoryMap.values()) {
        const topLevel = cat.split("/")[0];
        if (topLevel) topLevelZipNames.add(topLevel);
      }
      console.log(`[ROSTER-AB] Top-level zips derived from CSV: ${[...topLevelZipNames].join(", ")}`);

      for (const zipName of topLevelZipNames) {
        const zipPath = `${GLOBAL_ASSET_BASE}/${zipName}.zip`;
        const zipFile = bucket.file(zipPath);
        const [zipExists] = await zipFile.exists();
        if (!zipExists) {
          console.warn(`[ROSTER-AB] Mega-zip not found: ${zipPath} — skipping`);
          continue;
        }

        console.log(`[ROSTER-AB] Loading mega-zip: ${zipName}.zip`);
        let zip;
        try {
          const [zipBuffer] = await zipFile.download();
          zip = await JSZip.loadAsync(zipBuffer);
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not load ${zipName}.zip: ${e.message} — skipping`);
          continue;
        }

        // Group zip entries by "SubCategory/asset_name" folder key.
        // Internal path: {SubCategory}/{asset_name}/{filename}
        // Map< "SubCategory/asset_name" → { subCategory, assetFolder, objEntry, thumbEntry, colormapEntry } >
        const assetFolderMap = new Map();

        // Detect whether the zip has a redundant root folder matching the zip name.
        // e.g. Architecture_Modular.zip may contain:
        //   Architecture_Modular/Modular_Blocks_Panels/fountain-center/file  ← extra level
        // OR the expected:
        //   Modular_Blocks_Panels/fountain-center/file                        ← direct
        // We detect this by checking if parts[0] matches zipName (case-insensitive).
        // If so, we shift the index offset by 1.
        const zipNameLower = zipName.toLowerCase();
        let depthOffset = 0;
        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const p = entryPath.split("/");
          if (p.length >= 1 && p[0].toLowerCase() === zipNameLower) {
            depthOffset = 1;
          }
          break; // only need to check first file
        }
        if (depthOffset > 0) {
          console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip has redundant root folder — adjusting depth offset`);
        }

        for (const entryPath of Object.keys(zip.files)) {
          if (zip.files[entryPath].dir) continue;
          const parts = entryPath.split("/");
          if (parts.length < 3 + depthOffset) continue; // need SubCategory/asset_name/file
          const subCategory = parts[0 + depthOffset];
          const assetFolder = parts[1 + depthOffset];
          const fileName    = parts[parts.length - 1];
          const fileLower   = fileName.toLowerCase();
          if (fileName.startsWith("._")) continue;

          const folderKey = `${subCategory}/${assetFolder}`;
          if (!assetFolderMap.has(folderKey)) {
            assetFolderMap.set(folderKey, { subCategory, assetFolder, objEntry: null, thumbEntry: null, colormapEntry: null });
          }
          const entry = assetFolderMap.get(folderKey);

          if (fileLower.endsWith(".obj") && !entry.objEntry) {
            entry.objEntry = { entryPath, fileName };
          } else if ([".png", ".jpg", ".jpeg", ".webp"].some(e => fileLower.endsWith(e))) {
            if (fileLower.includes("colormap")) {
              // "colormap" anywhere in the filename = this object's texture. Locked. No other logic applies.
              if (!entry.colormapEntry) {
                entry.colormapEntry = { entryPath, fileName };
              }
            } else if (!entry.thumbEntry) {
              entry.thumbEntry = { entryPath, fileName, fileLower };
            }
          }
        }

        // Build objectAssets from folder map
        let added = 0;
        for (const [folderKey, entry] of assetFolderMap) {
          if (!entry.objEntry) continue;
          if (!entry.thumbEntry) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: no thumbnail — skipping`);
            continue;
          }

          // Verify asset exists in CSV
          const assetNameLower = entry.assetFolder.toLowerCase();
          const csvCategory    = assetCategoryMap.get(assetNameLower);
          if (!csvCategory) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: "${entry.assetFolder}" not in CSV — skipping`);
            continue;
          }

          try {
            const blob = await zip.files[entry.thumbEntry.entryPath].async("nodebuffer");
            const b64  = blob.toString("base64");
            if (!b64) continue;
            const mimeType = entry.thumbEntry.fileLower.endsWith(".png") ? "image/png" : "image/jpeg";
            if (!entry.colormapEntry) {
              console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: no colormap found — asset will have no texture`);
            }
            objectAssets.push({
              objFile:            entry.objEntry.fileName,
              objEntryPath:       entry.objEntry.entryPath,       // full zip path — locked at index time
              thumbFile:          entry.thumbEntry.fileName,
              colormapFile:       entry.colormapEntry?.fileName  || null,  // locked at index time
              colormapEntryPath:  entry.colormapEntry?.entryPath || null,  // full zip path — locked at index time
              colormapConfidence: entry.colormapEntry            ? "HIGH" : "NONE",
              b64,
              mimeType,
              sourceZip: `${zipName}.zip`,
              assetName: entry.assetFolder,
              category:  csvCategory          // canonical category from CSV
            });
            added++;
          } catch (e) {
            console.warn(`[ROSTER-AB] ${zipName}.zip/${folderKey}: thumbnail read failed — ${e.message}`);
          }
        }

        console.log(`[ROSTER-AB] Mega-zip ${zipName}.zip: ${added} asset(s) indexed`);
      }
    }

    const avatarAssets = [];
    {
      let avatarZip = null;
      for (const candidatePath of avatarZipPathCandidates) {
        const avatarZipFile = bucket.file(candidatePath);
        const [avatarZipExists] = await avatarZipFile.exists();
        if (!avatarZipExists) continue;
        console.log(`[ROSTER-AB] Loading avatar library from ${candidatePath}`);
        try {
          const [avatarZipBuffer] = await avatarZipFile.download();
          avatarZip = await JSZip.loadAsync(avatarZipBuffer);
          resolvedAvatarZipPath = candidatePath;
          break;
        } catch (e) {
          console.warn(`[ROSTER-AB] Could not load avatar library ${candidatePath}: ${e.message}`);
        }
      }

      if (avatarZip) {
        const folderMap = new Map();
        for (const entryPath of Object.keys(avatarZip.files)) {
          const entry = avatarZip.files[entryPath];
          if (entry.dir || entryPath.includes('__MACOSX')) continue;
          const base = entryPath.split('/').pop() || '';
          if (base.startsWith('._')) continue;
          const parts = entryPath.split('/').filter(Boolean);
          if (parts.length < 2) continue;
          const folderKey = parts.slice(0, -1).join('/');
          if (!folderMap.has(folderKey)) {
            folderMap.set(folderKey, { folderKey, folderName: parts[parts.length - 2], files: [] });
          }
          folderMap.get(folderKey).files.push(entryPath);
        }

        for (const folder of folderMap.values()) {
          const fbxEntryPath = folder.files.find(file => /\.fbx$/i.test(file));
          const thumbnailEntryPath = folder.files.find(file => /thumbnail\.(png|jpg|jpeg|webp)$/i.test(file));
          if (!fbxEntryPath || !thumbnailEntryPath) continue;
          try {
            const thumbBuffer = await avatarZip.files[thumbnailEntryPath].async('nodebuffer');
            const thumbLower = thumbnailEntryPath.toLowerCase();
            const mimeType = thumbLower.endsWith('.png') ? 'image/png' : 'image/jpeg';
            const animationManifestPath = folder.files.find(file => /animations\.txt$/i.test(file)) || null;
            const rawAnimations = animationManifestPath
              ? await avatarZip.files[animationManifestPath].async('text')
              : '';
            const animationClips = parseAnimationsTxt(rawAnimations);
            let fbxGeometry = null;
            let fbxMaterials = [];
            let fbxMeshCount = 0;
            let fbxSlotCount = 0;
            try {
              const fbxBuffer = await avatarZip.files[fbxEntryPath].async('nodebuffer');
              const scanResult = await scanFbxBuffer(fbxBuffer, fbxEntryPath);
              if (scanResult) {
                fbxGeometry = scanResult.geometry || null;
                fbxMaterials = scanResult.materials || [];
                fbxMeshCount = scanResult.meshCount || 0;
                fbxSlotCount = scanResult.slotCount || fbxMeshCount;
              }
            } catch (e) {
              console.warn(`[ROSTER-AB] Avatar FBX scan failed for ${fbxEntryPath}: ${e.message}`);
            }
            avatarAssets.push({
              assetName: fbxEntryPath.split('/').pop(),
              fbxEntryPath,
              thumbnailEntryPath,
              thumbnailFile: thumbnailEntryPath.split('/').pop(),
              textureFiles: listAvatarTextureFiles(avatarZip, `${folder.folderKey}/`),
              animationManifestPath,
              rawAnimations,
              animationClips,
              geometryAnalysis: fbxGeometry,
              materials: fbxMaterials,
              materialAssignments: fbxMaterials.map((m, i) => ({ slot: i, materialName: m.name })),
              meshCount: fbxMeshCount,
              slotCount: fbxSlotCount,
              b64: thumbBuffer.toString('base64'),
              mimeType,
              sourceZip: resolvedAvatarZipPath.split('/').pop() || 'Avatars.zip',
              avatarFolder: folder.folderName
            });
          } catch (e) {
            console.warn(`[ROSTER-AB] Avatar folder ${folder.folderKey}: thumbnail read failed — ${e.message}`);
          }
        }
      } else {
        console.warn(`[ROSTER-AB] Avatar library not found in any expected path: ${avatarZipPathCandidates.join(', ')}`);
      }
    }

    console.log(`[ROSTER-AB] Asset library ready: ${particleAssets.length} particle textures, ${objectAssets.length} 3D objects, ${avatarAssets.length} avatars`);

    // ── 7. Stage A — Visual Library Scan ────────────────────────────────
    const particleCandidates = new Map(particleReqs.map(r => [r.name, []]));
    const objectCandidates   = new Map(objectReqs.map(r   => [r.name, []]));
    const avatarCandidates   = new Map(avatarReqs.map(r   => [r.name, []]));

    // Stage A: particles (unchanged — no category filter needed)
    async function runStageAParticleBatches() {
      if (particleReqs.length === 0 || particleAssets.length === 0) return;
      const batches = chunkArray(particleAssets, IMAGES_PER_BATCH);
      console.log(`[ROSTER-AB] Stage A particles: ${particleAssets.length} assets → ${batches.length} batch(es)`);

      for (let b = 0; b < batches.length; b++) {
        const batch       = batches[b];
        const imageBlocks = batch.map(asset => ({
          type:   "image",
          source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
        }));

        let batchResult;
        try {
          batchResult = await callClaude(apiKey, {
            model:       "claude-sonnet-4-20250514",
            maxTokens:   2000,
            system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
            userContent: [
              { type: "text", text: buildStageAParticlePrompt(particleReqs) },
              ...imageBlocks
            ]
          });
        } catch (e) {
          console.warn(`[ROSTER-AB] Stage A particle batch ${b + 1} failed: ${e.message} — skipping`);
          continue;
        }

        let parsed;
        try { parsed = JSON.parse(stripFences(batchResult.text)); }
        catch (e) {
          console.warn(`[ROSTER-AB] Stage A particle batch ${b + 1} parse failed — skipping`);
          continue;
        }

        for (const match of (parsed.matches || [])) {
          const imgIdx = (match.imageIndex || 1) - 1;
          const asset  = batch[imgIdx];
          if (!asset) continue;
          for (const reqIdx of (match.matchesRequirements || [])) {
            const req = particleReqs[reqIdx - 1];
            if (!req) continue;
            const candidates = particleCandidates.get(req.name);
            if (!candidates) continue;
            if (!candidates.some(c => c.assetFile === asset.assetFile)) {
              candidates.push(asset);
            }
          }
        }
      }
    }

    // Stage A: 3D objects — category-filtered image-vs-image
    async function runStageAObjectsImageVsImage() {
      if (objectReqs.length === 0 || objectAssets.length === 0) return;

      for (const req of objectReqs) {
        const refImg = refImageByName.get(req.name.toLowerCase());
        if (!refImg) {
          console.warn(`[ROSTER-AB] No reference image for object "${req.name}" — will be unmatched`);
          continue;
        }

        // Apply category filter — never fall back to the full library when categories fail.
        const categoryFilter = reqCategoryFilter.get(req.name) || { allowedCats: new Set(), rankedCategories: [] };
        const allowedCats = categoryFilter.allowedCats;
        if (allowedCats.size === 0) {
          console.warn(`[ROSTER-AB] Stage A object "${req.name}": fewer than ${MIN_SUGGESTED_CATS} valid CSV-backed ranked categories — skipping search`);
          continue;
        }

        const filteredAssets = objectAssets.filter(a => allowedCats.has(a.category));

        console.log(
          `[ROSTER-AB] Stage A object "${req.name}": ` +
          `${filteredAssets.length} assets after category filter ` +
          `(${categoryFilter.rankedCategories.map(entry => `${entry.category} (${entry.likelihoodPercent}%)`).join(", ")})`
        );

        if (filteredAssets.length === 0) {
          console.warn(`[ROSTER-AB] Stage A object "${req.name}": 0 assets in searched categories — skipping search`);
          continue;
        }

        const refBlock = {
          type:   "image",
          source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
        };

        const batches    = chunkArray(filteredAssets, IMAGES_PER_BATCH);
        const candidates = objectCandidates.get(req.name);
        console.log(`[ROSTER-AB] Stage A object "${req.name}": ${filteredAssets.length} assets → ${batches.length} batch(es)`);

        for (let b = 0; b < batches.length; b++) {
          const batch       = batches[b];
          const thumbBlocks = batch.map(asset => ({
            type:   "image",
            source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
          }));

          let batchResult;
          try {
            batchResult = await callClaude(apiKey, {
              model:       "claude-sonnet-4-20250514",
              maxTokens:   2000,
              system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
              userContent: [
                { type: "text", text: buildStageAObjectRefImagePrompt(req.name, req.gameplayRole) },
                refBlock,
                ...thumbBlocks
              ]
            });
          } catch (e) {
            console.warn(`[ROSTER-AB] Stage A object "${req.name}" batch ${b + 1} failed: ${e.message} — skipping`);
            continue;
          }

          let parsed;
          try { parsed = JSON.parse(stripFences(batchResult.text)); }
          catch (e) {
            console.warn(`[ROSTER-AB] Stage A object "${req.name}" batch ${b + 1} parse failed — skipping`);
            continue;
          }

          for (const match of (parsed.matches || [])) {
            if (!match.matchesReference) continue;
            const imgIdx = (match.imageIndex || 1) - 1;
            const asset  = batch[imgIdx];
            if (!asset) continue;
            if (!candidates.some(c => c.objFile === asset.objFile)) {
              candidates.push(asset);
            }
          }
        }

        console.log(`[ROSTER-AB] Stage A object "${req.name}": ${candidates.length} candidate(s) found`);
      }
    }

    async function runStageAAvatarImageVsImage() {
      if (avatarReqs.length === 0 || avatarAssets.length === 0) return;
      for (const req of avatarReqs) {
        const refImg = refImageByName.get(req.name.toLowerCase());
        if (!refImg) {
          console.warn(`[ROSTER-AB] Stage A avatar "${req.name}": no reference image — skipping search`);
          continue;
        }

        const refBlock = {
          type:   "image",
          source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
        };

        const batches = chunkArray(avatarAssets, IMAGES_PER_BATCH);
        const candidates = avatarCandidates.get(req.name);
        console.log(`[ROSTER-AB] Stage A avatar "${req.name}": ${avatarAssets.length} assets → ${batches.length} batch(es)`);

        for (let b = 0; b < batches.length; b++) {
          const batch = batches[b];
          const thumbBlocks = batch.map(asset => ({
            type:   "image",
            source: { type: "base64", media_type: asset.mimeType, data: asset.b64 }
          }));

          let batchResult;
          try {
            batchResult = await callClaude(apiKey, {
              model:       "claude-sonnet-4-20250514",
              maxTokens:   2000,
              system:      "You are a game asset visual screener. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
              userContent: [
                { type: "text", text: buildStageAAvatarRefImagePrompt(req.name, req.gameplayRole, req.animationNeeds || []) },
                refBlock,
                ...thumbBlocks
              ]
            });
          } catch (e) {
            console.warn(`[ROSTER-AB] Stage A avatar "${req.name}" batch ${b + 1} failed: ${e.message} — skipping`);
            continue;
          }

          let parsed;
          try { parsed = JSON.parse(stripFences(batchResult.text)); }
          catch (e) {
            console.warn(`[ROSTER-AB] Stage A avatar "${req.name}" batch ${b + 1} parse failed — skipping`);
            continue;
          }

          for (const match of (parsed.matches || [])) {
            if (!match.matchesReference) continue;
            const imgIdx = (match.imageIndex || 1) - 1;
            const asset  = batch[imgIdx];
            if (!asset) continue;
            if (!candidates.some(c => c.assetName === asset.assetName && c.fbxEntryPath === asset.fbxEntryPath)) {
              candidates.push(asset);
            }
          }
        }

        console.log(`[ROSTER-AB] Stage A avatar "${req.name}": ${candidates.length} candidate(s) found`);
      }
    }

    // Run particle, object, and avatar Stage A scans concurrently
    await Promise.all([
      runStageAParticleBatches(),
      runStageAObjectsImageVsImage(),
      runStageAAvatarImageVsImage()
    ]);

    console.log("[ROSTER-AB] Stage A complete");

    // ── 8. Stage B — Per-Requirement Final Visual Pick ───────────────────
    console.log("[ROSTER-AB] Stage B: per-requirement final visual selection...");

    async function runStageBParticle(req) {
      const candidates = particleCandidates.get(req.name) || [];
      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Stage B particle: no candidates for "${req.name}" — unmatched`);
        return null;
      }
      const imageBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));
      const desc = req.visualDescription + (req.behaviorDescription ? ` — ${req.behaviorDescription}` : "");
      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent: [
            { type: "text", text: buildStageBParticlePrompt(req.name, desc, candidates, gameInterpretation) },
            ...imageBlocks
          ]
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Stage B particle failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}`, colormapFile: null };
      }
      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }
      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return { requirementName: req.name, selectedAsset: candidates[chosenIdx], visualSelectionRationale: parsed.visualSelectionRationale || "", colormapFile: null };
    }

    async function runStageBObject(req) {
      const candidates = objectCandidates.get(req.name) || [];
      const refImg     = refImageByName.get(req.name.toLowerCase());

      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Stage B object: no candidates for "${req.name}" — unmatched`);
        return null;
      }

      const refBlock = refImg ? {
        type:   "image",
        source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
      } : null;

      const thumbBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));

      const userContent = refBlock
        ? [{ type: "text", text: buildStageBObjectRefImagePrompt(req.name, req.gameplayRole, candidates, gameInterpretation) }, refBlock, ...thumbBlocks]
        : [{ type: "text", text: buildStageBObjectRefImagePrompt(req.name, req.gameplayRole, candidates, gameInterpretation) }, ...thumbBlocks];

      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Stage B object failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}` };
      }

      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return {
        requirementName:          req.name,
        selectedAsset:            candidates[chosenIdx],
        visualSelectionRationale: parsed.visualSelectionRationale || ""
      };
    }

    async function runStageBAvatar(req) {
      const candidates = avatarCandidates.get(req.name) || [];
      const refImg = refImageByName.get(req.name.toLowerCase());
      if (candidates.length === 0) {
        console.warn(`[ROSTER-AB] Stage B avatar: no candidates for "${req.name}" — unmatched`);
        return null;
      }

      const refBlock = refImg ? {
        type:   "image",
        source: { type: "base64", media_type: refImg.mimeType, data: refImg.b64 }
      } : null;
      const thumbBlocks = candidates.map(c => ({
        type:   "image",
        source: { type: "base64", media_type: c.mimeType, data: c.b64 }
      }));
      const userContent = refBlock
        ? [{ type: "text", text: buildStageBAvatarRefImagePrompt(req.name, req.gameplayRole, req.animationNeeds || [], candidates, gameInterpretation) }, refBlock, ...thumbBlocks]
        : [{ type: "text", text: buildStageBAvatarRefImagePrompt(req.name, req.gameplayRole, req.animationNeeds || [], candidates, gameInterpretation) }, ...thumbBlocks];

      let result;
      try {
        result = await callClaude(apiKey, {
          model:       "claude-sonnet-4-20250514",
          maxTokens:   1000,
          system:      "You are a visual asset selection specialist. Respond only with a valid JSON object. No markdown, no fences, no preamble.",
          userContent
        });
      } catch (e) {
        console.warn(`[ROSTER-AB] Stage B avatar failed for "${req.name}": ${e.message} — using first candidate`);
        return { requirementName: req.name, selectedAsset: candidates[0], visualSelectionRationale: `Fallback: ${e.message}` };
      }

      let parsed;
      try { parsed = JSON.parse(stripFences(result.text)); }
      catch (e) { parsed = { imageNumberChosen: 1, visualSelectionRationale: "Fallback: parse error" }; }

      const chosenIdx = Math.min((parsed.imageNumberChosen || 1) - 1, candidates.length - 1);
      return {
        requirementName: req.name,
        selectedAsset: candidates[chosenIdx],
        visualSelectionRationale: parsed.visualSelectionRationale || ""
      };
    }

    const [particleResults, objectResults, avatarResults] = await Promise.all([
      Promise.all(particleReqs.map(r => runStageBParticle(r))),
      Promise.all(objectReqs.map(r   => runStageBObject(r))),
      Promise.all(avatarReqs.map(r   => runStageBAvatar(r)))
    ]);

    console.log(
      `[ROSTER-AB] Stage B complete: ${particleResults.filter(Boolean).length} particle selections, ` +
      `${objectResults.filter(Boolean).length} object selections, ` +
      `${avatarResults.filter(Boolean).length} avatar selections`
    );

    // ── 9. Assemble final roster ─────────────────────────────────────────
    function assembleParticleAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      return {
        assetName:            asset.assetFile,
        sourceZip:            asset.sourceZip,
        intendedUsage:        `Particle effect: ${stageBResult.requirementName}`,
        particleEffectTarget: stageBResult.requirementName,
        matchedRequirement:   stageBResult.requirementName,
        selectionRationale:   stageBResult.visualSelectionRationale,
        thumbnailB64:         asset.b64,
        thumbnailMime:        asset.mimeType
      };
    }

    function assembleObjectAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      const p1    = phase1Req || {};
      return {
        assetName:           asset.objFile,
        objEntryPath:        asset.objEntryPath        || null,  // locked zip path from index time
        colormapFile:        asset.colormapFile        || null,  // locked at index time
        colormapEntryPath:   asset.colormapEntryPath   || null,  // locked zip path from index time
        colormapConfidence:  asset.colormapConfidence  || "NONE",
        thumbFile:           asset.thumbFile,
        sourceZip:           asset.sourceZip,
        category:            asset.category            || null,
        intendedRole:        p1.gameplayRole || p1.visualDescription || stageBResult.requirementName || "",
        matchedRequirement:  stageBResult.requirementName,
        selectionRationale:  stageBResult.visualSelectionRationale,
        thumbnailB64:        asset.b64,
        thumbnailMime:       asset.mimeType
      };
    }

    function assembleAvatarAsset(stageBResult, phase1Req) {
      if (!stageBResult) return null;
      const asset = stageBResult.selectedAsset;
      const p1 = phase1Req || {};
      const coverage = scoreAnimationCoverage(p1, asset.animationClips || []);
      const textureBindingContract = scoreTextureCandidates(
        asset.materials || [],
        asset.textureFiles || []
      );
      return {
        assetName: asset.assetName,
        fbxEntryPath: asset.fbxEntryPath || null,
        thumbnailEntryPath: asset.thumbnailEntryPath || null,
        thumbnailFile: asset.thumbnailFile || null,
        textureFiles: asset.textureFiles || [],
        textureBindingContract,
        animationManifestPath: asset.animationManifestPath || null,
        rawAnimations: asset.rawAnimations || '',
        animationClips: asset.animationClips || [],
        normalizedAnimations: coverage.normalizedBuckets || {},
        animationCoverage: coverage,
        geometryAnalysis: asset.geometryAnalysis || null,
        materials: asset.materials || [],
        materialAssignments: asset.materialAssignments || [],
        meshCount: asset.meshCount || 0,
        slotCount: asset.slotCount || 0,
        avatarRole: normalizeAvatarRole(p1.gameplayRole || stageBResult.requirementName),
        intendedRole: p1.gameplayRole || stageBResult.requirementName || "",
        matchedRequirement: stageBResult.requirementName,
        selectionRationale: stageBResult.visualSelectionRationale,
        textureStyle: p1.textureStyle || "",
        importance: p1.importance || '',
        selectionPriority: p1.selectionPriority || null,
        characterType: p1.characterType || '',
        gameplayFunction: p1.gameplayFunction || '',
        sourceZip: asset.sourceZip,
        thumbnailB64: asset.b64,
        thumbnailMime: asset.mimeType
      };
    }

    const phase1ParticleMap = new Map(particleReqs.map(r => [r.name, r]));
    const phase1ObjectMap   = new Map(objectReqs.map(r   => [r.name, r]));
    const phase1AvatarMap   = new Map(avatarReqs.map(r   => [r.name, r]));

    const textureAssets = particleResults
      .filter(Boolean)
      .map(r => assembleParticleAsset(r, phase1ParticleMap.get(r.requirementName)))
      .filter(Boolean);

    const objects3d = objectResults
      .filter(Boolean)
      .map(r => assembleObjectAsset(r, phase1ObjectMap.get(r.requirementName)))
      .filter(Boolean);

    const avatars = avatarResults
      .filter(Boolean)
      .map(r => assembleAvatarAsset(r, phase1AvatarMap.get(r.requirementName)))
      .filter(Boolean);

    const matchedParticleNames = new Set(textureAssets.map(a => a.matchedRequirement));
    const matchedObjectNames   = new Set(objects3d.map(a => a.matchedRequirement));
    const matchedAvatarNames   = new Set(avatars.map(a => a.matchedRequirement));

    const unmatchedRequirements = [
      ...particleReqs.filter(r => !matchedParticleNames.has(r.name)).map(r => ({
        requirementName: r.name, type: "particle_effect", reason: "No visual candidates found in Stage A"
      })),
      ...objectReqs.filter(r => !matchedObjectNames.has(r.name)).map(r => {
        const filterInfo = reqCategoryFilter.get(r.name) || { allowedCats: new Set(), rankedCategories: [] };
        return {
          requirementName: r.name,
          type: "object_3d",
          reason: (filterInfo.allowedCats.size < MIN_SUGGESTED_CATS)
            ? `Fewer than ${MIN_SUGGESTED_CATS} valid CSV-backed ranked categories were available for this requirement; Stage A object search was skipped`
            : "No visual candidates found in the searched categories during Stage A",
          categoriesSearched: filterInfo.rankedCategories.map(entry => entry.category),
          rankedCategoriesSearched: filterInfo.rankedCategories
        };
      }),
      ...avatarReqs.filter(r => !matchedAvatarNames.has(r.name)).map(r => ({
        requirementName: r.name,
        type: "avatar",
        reason: "No visual candidates found in Avatars.zip during Stage A/B"
      }))
    ];

    const roster = {
      documentTitle:             "Game-Specific Asset Roster",
      gameInterpretationSummary: gameInterpretation,
      objects3d,
      avatars,
      textureAssets,
      unmatchedRequirements,
      coverageSummary: {
        totalObjects3d:  objects3d.length,
        totalAvatars:    avatars.length,
        totalTextures:   textureAssets.length,
        totalUnmatched:  unmatchedRequirements.length,
        limitsRespected: objects3d.length <= MAX_OBJ_ASSETS && avatars.length <= MAX_AVATAR_ASSETS && textureAssets.length <= MAX_PNG_ASSETS,
        coverageNotes:   `${objects3d.length} objects, ${avatars.length} avatars, and ${textureAssets.length} particle textures selected.`
      },
      visualDirectionNotes: {}
    };

    roster._phase1Analysis = phase1;
    enforceHardLimits(roster);

    roster._meta = {
      jobId,
      generatedAt:         Date.now(),
      totalObjectAssets:   objectAssets.length,
      totalAvatarAssets:   avatarAssets.length,
      totalParticleAssets: particleAssets.length,
      refImagesUsed:       refImageByName.size,
      csvEntriesLoaded:    assetCategoryMap.size,
      avatarZipPath:        resolvedAvatarZipPath,
      approved:            false
    };

    // ── 10. Save pending roster to Firebase ──────────────────────────────
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    console.log(
      `[ROSTER-AB] Complete. Objects: ${objects3d.length}, ` +
      `Avatars: ${avatars.length}, ` +
      `Textures: ${textureAssets.length}, ` +
      `Unmatched: ${unmatchedRequirements.length}, ` +
      `RefImages used: ${refImageByName.size}, ` +
      `CSV entries: ${assetCategoryMap.size}`
    );

    return { statusCode: 202, body: "" };

  } catch (error) {
    console.error("[ROSTER-AB] Unhandled error:", error);
    if (bucket && projectPath) {
      try {
        await bucket.file(`${projectPath}/ai_asset_roster_error.json`).save(
          JSON.stringify({ error: error.message, failedAt: Date.now(), stage: "stageAB", jobId: jobId || null }),
          { contentType: "application/json", resumable: false }
        );
      } catch (e) { /* non-fatal */ }
    }
    return { statusCode: 202, body: "" };
  }
};
