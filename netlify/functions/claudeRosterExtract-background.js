/* netlify/functions/claudeRosterExtract-background.js */
/* ═══════════════════════════════════════════════════════════════════
   ASSET ROSTER EXTRACTION & FIREBASE STAGING — v2.0
   ─────────────────────────────────────────────────────────────────
   Background Netlify function (suffix -background = 15-min timeout)
   called after the user approves the game-specific Asset Roster.

   Flow:
     1. Load the approved roster from ai_asset_roster_pending.json
     2. For each selected asset, find the matching .zip in asset_packs/
        (matched by sourceRosterDocument name → same base name .zip)
     3. Extract only the approved files from each zip (parallel uploads)
     4. Upload extracted files to a game-specific staged folder:
        ${projectPath}/staged_assets/${jobId}/
     5. Save ai_asset_roster_approved.json with staged file paths
     6. assets.json registration is handled by the frontend in three steps
        after this function returns:
          a. copyRosterAssetsToModels() — copies staged files into models/
          b. syncAssetsJson() — scans models/ and rebuilds assets.json;
             primitive .obj files land in key "0" (.primitives folder),
             textures at root level or key "15" (Models folder).
          c. hardenPrimitivesInManifest() — targeted pass that moves any
             roster .obj entries into key "0" if not already there.
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

/* ═══════════════════════════════════════════════════════════════ */
exports.handler = async (event) => {
  try {
    if (!event.body) return err400("Missing request body");

    const { projectPath, jobId } = JSON.parse(event.body);
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

    const primitives  = Array.isArray(roster.primitiveObjects) ? roster.primitiveObjects : [];
    const textures    = Array.isArray(roster.textureAssets)    ? roster.textureAssets    : [];
    const allSelected = [...primitives, ...textures];

    if (allSelected.length === 0) {
      return err400("No assets selected in the roster. Cannot proceed with extraction.");
    }

    // ── 2. Group selected assets by source zip ───────────────────────
    const byZip = new Map(); // zipName → [asset objects]
    for (const asset of allSelected) {
      const zipName = zipNameFromRoster(asset.sourceRosterDocument || "");
      if (!zipName || zipName === ".zip") {
        console.warn(`[ROSTER-EXTRACT] Asset "${asset.assetName}" has no valid sourceRosterDocument — skipping.`);
        continue;
      }
      if (!byZip.has(zipName)) byZip.set(zipName, []);
      byZip.get(zipName).push(asset);
    }

    // ── 3. List available zip files in asset_packs/ ──────────────────
    const packsPrefix = `${projectPath}/asset_packs/`;
    const [packFiles] = await bucket.getFiles({ prefix: packsPrefix });
    const availableZips = new Map(); // lowercased base filename → bucket File reference
    for (const f of packFiles || []) {
      const base = f.name.split("/").pop();
      if (base) availableZips.set(base.toLowerCase(), f);
    }

    // ── 4. Extract and stage assets ──────────────────────────────────
    const stagedFolderPath = `${projectPath}/staged_assets/${jobId}`;
    const stagedAssets  = [];
    const extractionLog = [];

    for (const [zipName, assets] of byZip.entries()) {
      const zipFile = availableZips.get(zipName.toLowerCase());
      if (!zipFile) {
        console.warn(`[ROSTER-EXTRACT] Zip not found in asset_packs/: ${zipName}`);
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
      const uploadTasks = assets.map(async (asset) => {
        const normalizedTarget = normalizeAssetName(asset.assetName);
        const entryPath = zipEntries.get(normalizedTarget);

        if (!entryPath) {
          const availableSample = [...zipEntries.keys()].slice(0, 10).join(", ");
          const msg = `"${asset.assetName}" (normalized: "${normalizedTarget}") not found in ${zipName}. ` +
                      `Sample available: [${availableSample}]`;
          console.warn(`[ROSTER-EXTRACT] ${msg}`);
          extractionLog.push({ zipName, asset: asset.assetName, status: "not_in_zip", detail: msg });
          return null;
        }

        try {
          const fileData  = await zip.files[entryPath].async("nodebuffer");
          const ext       = asset.assetName.split(".").pop().toLowerCase();
          const mimeType  = ext === "png"                   ? "image/png"
                          : ext === "jpg" || ext === "jpeg" ? "image/jpeg"
                          : ext === "webp"                  ? "image/webp"
                          : ext === "obj"                   ? "text/plain"
                          : ext === "glb" || ext === "gltf" ? "model/gltf-binary"
                          : "application/octet-stream";

          const stagedPath = `${stagedFolderPath}/${asset.assetName}`;
          await bucket.file(stagedPath).save(fileData, { contentType: mimeType, resumable: false });

          return {
            assetName:            asset.assetName,
            sourceRosterDocument: asset.sourceRosterDocument,
            stagedPath,
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
    const approvedRoster = {
      ...roster,
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
    // The frontend handles manifest registration in three steps after this
    // function returns: copyRosterAssetsToModels() copies staged files into
    // models/, syncAssetsJson() rebuilds assets.json from models/ (primitives
    // -> key "0", textures -> key "15"), then hardenPrimitivesInManifest()
    // ensures .obj files are in the .primitives folder (key "0").

    console.log(`[ROSTER-EXTRACT] Complete. ${stagedAssets.length} asset(s) staged to ${stagedFolderPath}`);

    return {
      statusCode: 200,
      body: JSON.stringify({
        success: true,
        stagedAssets,
        stagedFolder: stagedFolderPath,
        extractionLog
      })
    };

  } catch (error) {
    console.error("[ROSTER-EXTRACT] Unhandled error:", error);
    return { statusCode: 500, body: JSON.stringify({ success: false, error: error.message }) };
  }
};
