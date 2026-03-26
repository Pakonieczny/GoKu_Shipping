/* netlify/functions/claudeRosterExtract-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER EXTRACTION & FIREBASE STAGING — v2.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout)
   called after the user approves the game-specific Asset Roster.

   Flow:
     1. Load the approved roster from ai_asset_roster_pending.json
     2. For each selected asset, find the matching .zip — searches project-local particle/3D folders
        first, then falls back to the global projects/BASE_Files/asset_3d_objects/ mega-zips.
        (matched by sourceRosterDocument name → same base name .zip)
     3. Extract only the approved files from each zip (parallel uploads)
     4. Upload extracted files to a game-specific staged folder:
        ${projectPath}/staged_assets/${jobId}/
     5. Save ai_asset_roster_approved.json with staged file paths
     6. assets.json registration is handled by the frontend in two steps
        after this function returns:
          a. copyRosterAssetsToModels() — copies staged files into models/
          b. syncAssetsJson() — scans models/ and rebuilds assets.json;
             approved 3D objects register as children of the Models folder (key "15"),
             approved particle textures register at root level with their own assigned numeric keys.
        This function does NOT write assets.json. "staged_roster" is not
        a real manifest key — it was a stale reference and has been removed.
     7. Return { success:true, stagedAssets, stagedFolder, extractionLog }

   Request body:  { projectPath, jobId }
   Response body: { success:true, stagedAssets, stagedFolder, extractionLog }
               |  { success:false, error: "..." }

   NOTE: Renamed to *-background.js so Netlify gives this function a
   15-minute execution window instead of the default 10-second limit.
   ═══════════════════════════════════════════════════════════════════ */

const admin  = require("./firebaseAdmin");
const JSZip  = require("jszip");

/* ─── Helpers ────────────────────────────────────────────────── */
function err400(msg) { return { statusCode: 400, body: JSON.stringify({ success: false, error: msg }) }; }

/* "Nature Pack.docx" → "Nature Pack.zip" */
function zipNameFromRoster(rosterDocName = "") {
  return rosterDocName.replace(/\.docx$/i, ".zip");
}

/* Normalize for fuzzy filename matching:
   strips everything except alphanumerics, dots, dashes, underscores.
   "Tree Trunk.obj" → "treetrunk.obj"
   Used for both the roster asset name and the zip entry name so
   spaces / casing differences do not cause silent misses.           */
function normalizeAssetName(name = "") {
  return name.toLowerCase().replace(/[^a-z0-9.\-_]/g, "");
}

/* Zip content classifier for staged 3D assets */
function getZipEntryBaseName(name = "") {
  const base = String(name || "").split("/").pop() || String(name || "");
  return base.replace(/\.[^.]+$/, "");
}

function detectMimeType(filename = "") {
  const ext = String(filename || "").split(".").pop().toLowerCase();
  return ext === "png"                    ? "image/png"
       : ext === "jpg" || ext === "jpeg" ? "image/jpeg"
       : ext === "webp"                   ? "image/webp"
       : ext === "bmp"                    ? "image/bmp"
       : ext === "tga"                    ? "image/x-targa"
       : ext === "obj"                    ? "text/plain"
       : ext === "glb" || ext === "gltf" ? "model/gltf-binary"
       : "application/octet-stream";
}

/* classifyZipContents — classifies a pack zip that contains multiple object subfolders.
   Each 3D object lives in its own subfolder: ObjectName/ObjectName.obj, ObjectName/colormap.png, ObjectName/thumb.png
   We classify per-folder so that mesh basename matching is scoped correctly within each object's folder,
   not across the entire pack (which would incorrectly flag same-named colormaps as thumbnails).
   Returns a Map: normalizedObjBaseName → { meshFile, colormapResolved, hasMtlFile }
   Also returns a legacy top-level colormapResolved (first found) for backwards-compat callers. */
function classifyZipContents(zip) {
  const meshExtensions  = new Set([".obj", ".fbx", ".glb", ".gltf", ".c3b"]);
  const imageExtensions = new Set([".jpg", ".jpeg", ".png", ".webp", ".tga", ".bmp"]);
  const explicitColorNames = new Set([
    "colormap", "color_map", "albedo", "albedo_map",
    "diffuse", "diffuse_map", "basecolor", "base_color",
    "texture", "tex", "color"
  ]);
  const confidenceRank = { HIGH: 3, MEDIUM: 2, LOW: 1, NONE: 0 };

  // Group all entries by their immediate parent folder (or "" for root-level files)
  const byFolder = new Map(); // folderPath → { meshFiles: [], imageFiles: [], hasMtlFile: false }
  const allMeshFiles  = [];
  const allImageFiles = [];
  let globalHasMtlFile = false;

  for (const entryPath of Object.keys(zip.files || {})) {
    const entry = zip.files[entryPath];
    if (!entry || entry.dir || entryPath.includes("__MACOSX")) continue;

    const parts    = entryPath.split("/");
    const fileName = parts[parts.length - 1];
    // folder key: everything except the filename; root files use ""
    const folderKey = parts.length > 1 ? parts.slice(0, -1).join("/") : "";

    const lowerFileName = fileName.toLowerCase();
    const ext      = lowerFileName.includes(".") ? lowerFileName.slice(lowerFileName.lastIndexOf(".")) : "";
    const baseName = getZipEntryBaseName(fileName).toLowerCase();

    if (!byFolder.has(folderKey)) byFolder.set(folderKey, { meshFiles: [], imageFiles: [], hasMtlFile: false });
    const bucket = byFolder.get(folderKey);

    if (meshExtensions.has(ext)) {
      const rec = { entryPath, fileName, baseName, ext };
      bucket.meshFiles.push(rec);
      allMeshFiles.push(rec);
    } else if (imageExtensions.has(ext)) {
      const rec = { entryPath, fileName, baseName, ext };
      bucket.imageFiles.push(rec);
      allImageFiles.push(rec);
    } else if (ext === ".mtl") {
      bucket.hasMtlFile = true;
      globalHasMtlFile = true;
    }
  }

  /* Classify images within a single folder scope.
     Rules (in priority order):
       1. thumb/preview in name → THUMBNAIL (highest priority, always)
       2. Explicit colormap name (colormap, albedo, diffuse …) → COLORMAP HIGH
       3. basename matches a mesh basename in the SAME folder → THUMBNAIL
          (e.g. TreeTrunk/TreeTrunk.png is a preview/thumbnail of TreeTrunk.obj)
       4. Only one image left unidentified → COLORMAP MEDIUM
       5. Last image standing → COLORMAP LOW                                      */
  function classifyImagesInScope(imageFiles, meshFiles) {
    const meshBaseNamesLocal = new Set(meshFiles.map(f => f.baseName));
    const detections = imageFiles.map((image) => {
      if (/(thumb|preview)/i.test(image.baseName)) {
        return { ...image, role: "THUMBNAIL",  confidence: "HIGH",   detectionRule: "name=thumb-or-preview" };
      }
      if (explicitColorNames.has(image.baseName)) {
        return { ...image, role: "COLORMAP",   confidence: "HIGH",   detectionRule: `name=${image.baseName}` };
      }
      if (meshBaseNamesLocal.has(image.baseName)) {
        // Same base name as the mesh in this folder → thumbnail (preview image, not colormap)
        return { ...image, role: "THUMBNAIL",  confidence: "HIGH",   detectionRule: "mesh-basename-match" };
      }
      return { ...image, role: "UNKNOWN", confidence: "NONE", detectionRule: "none" };
    });

    let colormapCandidates = detections.filter(i => i.role === "COLORMAP");
    let unidentified       = detections.filter(i => i.role === "UNKNOWN");

    if (colormapCandidates.length === 0 && imageFiles.length === 1 && unidentified.length === 1) {
      unidentified[0].role = "COLORMAP"; unidentified[0].confidence = "MEDIUM"; unidentified[0].detectionRule = "only-image";
      colormapCandidates = [unidentified[0]]; unidentified = [];
    }
    if (colormapCandidates.length === 0 && unidentified.length === 1) {
      unidentified[0].role = "COLORMAP"; unidentified[0].confidence = "LOW"; unidentified[0].detectionRule = "last-unidentified-image";
      colormapCandidates = [unidentified[0]];
    }

    colormapCandidates.sort((a, b) => {
      const rankDiff = (confidenceRank[b.confidence] || 0) - (confidenceRank[a.confidence] || 0);
      return rankDiff || a.fileName.localeCompare(b.fileName);
    });

    return { detections, colormapResolved: colormapCandidates[0] || null };
  }

  // Build per-object-folder classification map: normalizedObjBaseName → classification result
  const perObjectClassification = new Map();
  let firstColormapResolved = null;

  for (const [folderKey, bucket] of byFolder.entries()) {
    if (bucket.meshFiles.length === 0) continue; // image-only folder (thumbnails dir, etc.) — skip
    const result = classifyImagesInScope(bucket.imageFiles, bucket.meshFiles);
    for (const mesh of bucket.meshFiles) {
      perObjectClassification.set(mesh.baseName, {
        meshFile:         mesh,
        colormapResolved: result.colormapResolved,
        hasMtlFile:       bucket.hasMtlFile,
        imageDetections:  result.detections
      });
    }
    if (!firstColormapResolved && result.colormapResolved) {
      firstColormapResolved = result.colormapResolved;
    }
  }

  // Fallback: if no per-folder mesh found but there are root-level files, classify them globally
  if (perObjectClassification.size === 0 && allMeshFiles.length > 0) {
    const fallback = classifyImagesInScope(allImageFiles, allMeshFiles);
    for (const mesh of allMeshFiles) {
      perObjectClassification.set(mesh.baseName, {
        meshFile:         mesh,
        colormapResolved: fallback.colormapResolved,
        hasMtlFile:       globalHasMtlFile,
        imageDetections:  fallback.detections
      });
    }
    firstColormapResolved = fallback.colormapResolved;
  }

  return {
    meshFiles:               allMeshFiles,
    imageFiles:              allImageFiles,
    imageDetections:         [],           // legacy field — use perObjectClassification instead
    colormapResolved:        firstColormapResolved, // legacy: first found; use perObjectClassification for accuracy
    hasMtlFile:              globalHasMtlFile,
    perObjectClassification  // Map<objBaseName, { meshFile, colormapResolved, hasMtlFile, imageDetections }>
  };
}


/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  let projectPath = null;
  let jobId = null;

  try {
    if (!event.body) return err400("Missing request body");

    const body = JSON.parse(event.body);
    projectPath = body.projectPath;
    jobId = body.jobId;
    if (!projectPath) return err400("Missing projectPath");
    if (!jobId)       return err400("Missing jobId");

    const bucket = admin.storage().bucket(
      process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
    );

    console.log(`[ROSTER-EXTRACT] Starting extraction — project: ${projectPath}, job: ${jobId}`);

    // ── 1. Load the pending roster ───────────────────────────────────
    const pendingFile = bucket.file(`${projectPath}/ai_asset_roster_pending.json`);
    const [pendingExists] = await pendingFile.exists();
    if (!pendingExists) return err400("ai_asset_roster_pending.json not found. Run roster generation first.");

    const [pendingContent] = await pendingFile.download();
    const roster = JSON.parse(pendingContent.toString());

    const objects3d   = Array.isArray(roster.objects3d)       ? roster.objects3d       : [];
    const textures    = Array.isArray(roster.textureAssets)    ? roster.textureAssets    : [];
    const allSelected = [...objects3d, ...textures];

    if (allSelected.length === 0) {
      return err400("No assets selected in the roster. Cannot proceed with extraction.");
    }

    // ── 2. Group selected assets by source zip ───────────────────────
    const byZip = new Map(); // zipName → [asset objects]
    for (const asset of allSelected) {
      const zipName = asset.sourceZip || zipNameFromRoster(asset.sourceRosterDocument || "");
      if (!zipName || zipName === ".zip") {
        console.warn(`[ROSTER-EXTRACT] Asset "${asset.assetName}" has no valid sourceZip or sourceRosterDocument — skipping.`);
        continue;
      }
      if (!byZip.has(zipName)) byZip.set(zipName, []);
      byZip.get(zipName).push(asset);
    }

    // ── 3. List available zip files ─────────────────────────────────
    // Search order:
    //   1. ${projectPath}/asset_particle_textures/  — project-local particle zips
    //   2. ${projectPath}/asset_3d_objects/          — project-local 3D zips (legacy / future)
    //   3. projects/BASE_Files/asset_3d_objects/    — global mega-zips (Architecture_Modular.zip etc.)
    //
    // Project-local paths are checked first so a project-specific override
    // can shadow a global zip of the same name if needed.
    // Later entries do NOT overwrite earlier ones in availableZips — first match wins.
    const GLOBAL_ASSET_BASE = "projects/BASE_Files/asset_3d_objects";
    const availableZips = new Map(); // lowercased base filename → bucket File reference
    const zipSearchFolders = [
      `${projectPath}/asset_particle_textures/`,
      `${projectPath}/asset_3d_objects/`,
      `${GLOBAL_ASSET_BASE}/`
    ];
    for (const folder of zipSearchFolders) {
      let folderFiles;
      try {
        [folderFiles] = await bucket.getFiles({ prefix: folder });
      } catch (e) {
        console.warn(`[ROSTER-EXTRACT] Could not list folder ${folder}: ${e.message}`);
        continue;
      }
      for (const f of folderFiles || []) {
        const base = f.name.split("/").pop();
        if (!base || !base.toLowerCase().endsWith(".zip")) continue;
        const key = base.toLowerCase();
        if (!availableZips.has(key)) {
          // First match wins — project-local overrides global if names collide
          availableZips.set(key, f);
        }
      }
    }
    console.log(`[ROSTER-EXTRACT] Zip index built: ${availableZips.size} zip(s) found across all folders`);

    // ── 4. Extract and stage assets ──────────────────────────────────
    const stagedFolderPath = `${projectPath}/staged_assets/${jobId}`;
    const stagedAssets  = [];
    const extractionLog = [];

    for (const [zipName, assets] of byZip.entries()) {
      const zipFile = availableZips.get(zipName.toLowerCase());
      if (!zipFile) {
        console.warn(`[ROSTER-EXTRACT] Zip not found in any search folder (project-local or global): ${zipName}`);
        extractionLog.push({ zipName, status: "missing", assetCount: assets.length });
        continue;
      }

      // Download and parse the zip
      const [zipBuffer] = await zipFile.download();
      let zip;
      try {
        zip = await JSZip.loadAsync(zipBuffer);
      } catch (e) {
        console.warn(`[ROSTER-EXTRACT] Could not parse zip ${zipName}: ${e.message}`);
        extractionLog.push({ zipName, status: "parse_error", error: e.message });
        continue;
      }

      // Build normalized lookup: normalizedBaseName → full zip entry path
      const zipEntries = new Map();
      for (const entryPath of Object.keys(zip.files)) {
        const entry = zip.files[entryPath];
        if (entry.dir) continue;
        const baseName = entryPath.split("/").pop();
        zipEntries.set(normalizeAssetName(baseName), entryPath);
      }

      // Upload all approved assets from this zip in parallel
      const zipClassification = classifyZipContents(zip);
      const uploadTasks = assets.map(async (asset) => {
        const normalizedTarget = normalizeAssetName(asset.assetName);
        const entryPath = zipEntries.get(normalizedTarget)
                       || zipEntries.get(normalizedTarget + '.obj');

        if (!entryPath) {
          const availableSample = [...zipEntries.keys()].slice(0, 10).join(", ");
          const msg = `"${asset.assetName}" (normalized: "${normalizedTarget}") not found in ${zipName}. ` +
                      `Sample available: [${availableSample}]`;
          console.warn(`[ROSTER-EXTRACT] ${msg}`);
          extractionLog.push({ zipName, asset: asset.assetName, status: "not_in_zip", detail: msg });
          return null;
        }

        try {
          const fileData = await zip.files[entryPath].async("nodebuffer");
          const stagedPath = `${stagedFolderPath}/${asset.assetName}`;
          await bucket.file(stagedPath).save(fileData, {
            contentType: detectMimeType(asset.assetName),
            resumable: false
          });

          const is3dAsset = /\.(obj|fbx|glb|gltf|c3b)$/i.test(asset.assetName || "");
          let colormapFile = asset.colormapFile || null;
          let colormapConfidence = asset.colormapConfidence || "NONE";
          let colormapStagedPath = null;
          let colormapDetectionRule = asset.colormapFile ? "roster-field" : "none";

          if (is3dAsset) {
            let resolvedColormap = null;
            if (asset.colormapFile) {
              const explicitEntryPath = zipEntries.get(normalizeAssetName(asset.colormapFile));
              if (explicitEntryPath) {
                resolvedColormap = {
                  entryPath: explicitEntryPath,
                  fileName: explicitEntryPath.split("/").pop() || asset.colormapFile,
                  confidence: asset.colormapConfidence || "HIGH",
                  detectionRule: "roster-field"
                };
              } else {
                console.warn(`[ROSTER-EXTRACT] Colormap "${asset.colormapFile}" not found in ${zipName}`);
                extractionLog.push({
                  zipName,
                  asset: asset.assetName,
                  status: "colormap_missing",
                  detail: `Declared colormap "${asset.colormapFile}" not found`
                });
              }
            }

            // Per-object classification: look up by this asset's own base name first,
            // then fall back to the first colormap found anywhere in the zip.
            // This prevents cross-contamination where Object A gets Object B's colormap.
            if (!resolvedColormap) {
              const assetBaseName = getZipEntryBaseName(asset.assetName).toLowerCase();
              const perObjResult = zipClassification.perObjectClassification.get(assetBaseName);
              const candidate = perObjResult?.colormapResolved || zipClassification.colormapResolved;
              if (candidate) {
                resolvedColormap = {
                  entryPath: candidate.entryPath,
                  fileName: candidate.fileName,
                  confidence: candidate.confidence || "LOW",
                  detectionRule: candidate.detectionRule || "auto"
                };
                console.log(`[ROSTER-EXTRACT] Colormap auto-resolved for ${asset.assetName} from ${zipName}: ${resolvedColormap.fileName} (rule: ${resolvedColormap.detectionRule})`);
              }
            }

            if (resolvedColormap) {
              const colormapBuffer = await zip.files[resolvedColormap.entryPath].async("nodebuffer");
              colormapFile = resolvedColormap.fileName;
              colormapConfidence = resolvedColormap.confidence || "LOW";
              colormapDetectionRule = resolvedColormap.detectionRule || "auto";
              colormapStagedPath = `${stagedFolderPath}/${resolvedColormap.fileName}`;
              await bucket.file(colormapStagedPath).save(colormapBuffer, {
                contentType: detectMimeType(resolvedColormap.fileName),
                resumable: false
              });
            } else {
              extractionLog.push({
                zipName,
                asset: asset.assetName,
                status: "colormap_not_found",
                detail: `No colormap found in ${zipName}`
              });
            }
          }

          return {
            assetName:            asset.assetName,
            sourceRosterDocument: asset.sourceRosterDocument,
            stagedPath,
            colormapFile,
            colormapStagedPath,
            colormapConfidence,
            colormapDetectionRule,
            intendedRole:         asset.intendedRole || asset.intendedUsage || "",
            selectionRationale:   asset.selectionRationale || ""
          };
        } catch (e) {
          console.warn(`[ROSTER-EXTRACT] Upload failed for ${asset.assetName}: ${e.message}`);
          extractionLog.push({ zipName, asset: asset.assetName, status: "upload_error", error: e.message });
          return null;
        }
      });

      const results   = await Promise.all(uploadTasks);
      const succeeded = results.filter(Boolean);
      stagedAssets.push(...succeeded);

      extractionLog.push({ zipName, status: "ok", extracted: succeeded.length, attempted: assets.length });
      console.log(`[ROSTER-EXTRACT] ${zipName}: ${succeeded.length}/${assets.length} asset(s) staged`);
    }

    // ── 5. Save approved roster with staged paths ────────────────────
    const stagedIndex = new Map(
      stagedAssets
        .filter(asset => asset?.assetName)
        .map(asset => [normalizeAssetName(asset.assetName), asset])
    );

    const enrichApprovedAsset = (asset) => {
      const stagedMeta = stagedIndex.get(normalizeAssetName(asset?.assetName || ""));
      if (!stagedMeta) return asset;
      return {
        ...asset,
        stagedPath: stagedMeta.stagedPath || asset.stagedPath || null,
        colormapFile: stagedMeta.colormapFile || asset.colormapFile || null,
        colormapStagedPath: stagedMeta.colormapStagedPath || asset.colormapStagedPath || null,
        colormapConfidence: stagedMeta.colormapConfidence || asset.colormapConfidence || "NONE",
        colormapDetectionRule: stagedMeta.colormapDetectionRule || asset.colormapDetectionRule || "none"
      };
    };

    const approvedRoster = {
      ...roster,
      objects3d: (roster.objects3d || []).map(enrichApprovedAsset),
      textureAssets: (roster.textureAssets || []).map(enrichApprovedAsset),
      _meta: {
        ...roster._meta,
        approved:         true,
        approvedAt:       Date.now(),
        stagedFolder:     stagedFolderPath,
        stagedAssetCount: stagedAssets.length,
        extractionLog
      },
      stagedAssets
    };

    await bucket.file(`${projectPath}/ai_asset_roster_approved.json`).save(
      JSON.stringify(approvedRoster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    // Mark pending as approved (UI reference only)
    roster._meta = { ...roster._meta, approved: true, approvedAt: Date.now() };
    await bucket.file(`${projectPath}/ai_asset_roster_pending.json`).save(
      JSON.stringify(roster, null, 2),
      { contentType: "application/json", resumable: false }
    );

    // ── 6. assets.json is NOT written here ───────────────────────────
    // The frontend handles manifest registration in two steps after this
    // function returns: copyRosterAssetsToModels() copies staged files into
    // models/, then syncAssetsJson() rebuilds assets.json — approved 3D objects
    // register as children of the Models folder (key "15"), while approved
    // particle textures register at root level with their own assigned numeric keys.

    // ── 7. Write result sentinel — frontend polls for this file ──────
    // Background functions return 202 immediately with no body. The frontend
    // polls ai_asset_roster_extract_result.json until it appears with a
    // completedAt timestamp matching this jobId, then reads the staged assets.
    const resultPayload = {
      success:      true,
      jobId,
      completedAt:  Date.now(),
      stagedAssets,
      stagedFolder: stagedFolderPath,
      extractionLog
    };
    await bucket.file(`${projectPath}/ai_asset_roster_extract_result.json`)
      .save(JSON.stringify(resultPayload, null, 2), { contentType: "application/json", resumable: false });

    console.log(`[ROSTER-EXTRACT] Complete. ${stagedAssets.length} asset(s) staged to ${stagedFolderPath}`);

    // Background function — response body is ignored by Netlify, but return cleanly.
    return { statusCode: 200, body: "" };

  } catch (error) {
    console.error("[ROSTER-EXTRACT] Unhandled error:", error);

    // Write error sentinel so the frontend poller surfaces the failure immediately
    // instead of timing out after 15 minutes.
    try {
      if (projectPath) {
        const bucket = admin.storage().bucket(
          process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app"
        );
        await bucket.file(`${projectPath}/ai_asset_roster_extract_error.json`)
          .save(JSON.stringify({ success: false, jobId, error: error.message, failedAt: Date.now() }),
                { contentType: "application/json", resumable: false });
      }
    } catch (writeErr) {
      console.warn("[ROSTER-EXTRACT] Could not write error sentinel:", writeErr.message);
    }

    return { statusCode: 500, body: "" };
  }
};

