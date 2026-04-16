export function createAssetsTreeEntitiesCompiler(deps = {}) {
  const {
    ROOT,
    storage,
    ref,
    listAll,
    getDownloadURL,
    uploadString,
    deleteObject,
    loadJsonFromStoragePath,
    fetchStorageTextSafe,
    syncAssetsJson,
    annotateApprovedRosterWithManifestKeys,
    analyzeDirectThreeAsset,
    getCachedModelAnalysis,
    cacheModelAnalysis,
    refreshJsonUi = () => {},
    getCurrentProject = () => null
  } = deps;

  if (!ROOT) throw new Error('AssetsTreeEntitiesCompiler requires ROOT.');
  if (!storage) throw new Error('AssetsTreeEntitiesCompiler requires storage.');
  if (typeof ref !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires ref().');
  if (typeof listAll !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires listAll().');
  if (typeof getDownloadURL !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires getDownloadURL().');
  if (typeof uploadString !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires uploadString().');
  if (typeof deleteObject !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires deleteObject().');
  if (typeof loadJsonFromStoragePath !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires loadJsonFromStoragePath().');
  if (typeof fetchStorageTextSafe !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires fetchStorageTextSafe().');
  if (typeof syncAssetsJson !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires syncAssetsJson().');
  if (typeof annotateApprovedRosterWithManifestKeys !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires annotateApprovedRosterWithManifestKeys().');
  if (typeof analyzeDirectThreeAsset !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires analyzeDirectThreeAsset().');
  if (typeof getCachedModelAnalysis !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires getCachedModelAnalysis().');
  if (typeof cacheModelAnalysis !== 'function') throw new Error('AssetsTreeEntitiesCompiler requires cacheModelAnalysis().');

  const DEFAULT_GROUP_MAT = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1];
  const FORBIDDEN_LEGACY_PRIMITIVE_MODEL_KEYS = new Set(['17', '18', '21', '34', '35']);
  const FORBIDDEN_LEGACY_PRIMITIVE_MODEL_TITLES = new Set(['cube.obj', 'cylinder.obj', 'sphere.obj', 'plane.obj', 'planevertical.obj']);


  function createDefaultSceneIntent() {
    return {
      settings: { rootGroupTitle: 'Scene Root' },
      groups: [],
      objects: [],
      rigidbodies: []
    };
  }

  function cloneJsonSafe(value, fallback = null) {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (_) {
      return fallback;
    }
  }

  function assertNoForbiddenLegacyPrimitiveModelEntries(manifestEntries = [], label = 'manifest') {
    const offenders = [];
    for (const entry of flattenAssetManifestEntries(Array.isArray(manifestEntries) ? manifestEntries : [])) {
      if (!entry || typeof entry !== 'object') continue;
      const key = String(entry.key || '').trim();
      const titleLower = String(entry.title || '').trim().toLowerCase();
      if (FORBIDDEN_LEGACY_PRIMITIVE_MODEL_KEYS.has(key) || FORBIDDEN_LEGACY_PRIMITIVE_MODEL_TITLES.has(titleLower)) {
        offenders.push(`${key || '(missing key)'}:${entry.title || '(missing title)'}`);
      }
    }
    if (offenders.length > 0) {
      throw new Error(`${label} contains forbidden legacy primitive model entries (${offenders.join(', ')}). Use only .primitives keys 4-14.`);
    }
    return true;
  }

  function normalizeNumericVector(value, fallback) {
    if (!Array.isArray(value) || value.length !== fallback.length) return fallback.slice();
    return fallback.map((defaultValue, index) => {
      const num = Number(value[index]);
      return Number.isFinite(num) ? num : defaultValue;
    });
  }

  function flattenAssetManifestEntries(entries) {
    const flat = [];
    for (const entry of Array.isArray(entries) ? entries : []) {
      if (!entry || typeof entry !== 'object') continue;
      flat.push(entry);
      if (Array.isArray(entry.children) && entry.children.length > 0) {
        flat.push(...flattenAssetManifestEntries(entry.children));
      }
    }
    return flat;
  }

  function buildManifestLookup(entries = []) {
    const byKey = new Map();
    const byTitleLower = new Map();
    const flat = flattenAssetManifestEntries(entries);
    for (const entry of flat) {
      if (!entry || typeof entry !== 'object') continue;
      const key = entry.key != null ? String(entry.key) : '';
      const titleLower = String(entry.title || '').toLowerCase();
      if (key) byKey.set(key, entry);
      if (titleLower) byTitleLower.set(titleLower, entry);
    }
    return { byKey, byTitleLower, flat };
  }

  function buildApprovedRosterLookup(roster = null) {
    const byManifestKey = new Map();
    const byAssetNameLower = new Map();
    if (!roster || typeof roster !== 'object') return { byManifestKey, byAssetNameLower };

    const addAsset = (asset) => {
      if (!asset || typeof asset !== 'object') return;
      const manifestKey = asset.manifestKey != null ? String(asset.manifestKey) : '';
      const assetNameLower = String(asset.assetName || '').toLowerCase();
      if (manifestKey) byManifestKey.set(manifestKey, asset);
      if (assetNameLower) byAssetNameLower.set(assetNameLower, asset);
    };

    (Array.isArray(roster.objects3d) ? roster.objects3d : []).forEach(addAsset);
    (Array.isArray(roster.avatars) ? roster.avatars : []).forEach(addAsset);
    (Array.isArray(roster.textureAssets) ? roster.textureAssets : []).forEach(addAsset);
    (Array.isArray(roster.stagedAssets) ? roster.stagedAssets : []).forEach(addAsset);

    return { byManifestKey, byAssetNameLower };
  }

  // After renameModelFilesToKeys runs, scan facts produced by getOrBuildExtendedModelAnalysis
  // have sourceName equal to the numeric manifest key (e.g. "129") because the file in
  // models/ is stored under that numeric name. Manifest entries, however, retain their
  // original title (e.g. "coin-gold.obj"). Without an alias, resolveScanFactForAsset
  // would never find a matching scan fact for a renamed asset, leaving extent / meshCount /
  // slotCount fields empty in the compiled package.
  //
  // Pass the manifest lookup so we can register, for each scan fact whose sourceName is a
  // numeric manifest key, additional aliases under the manifest entry's title (with and
  // without extension). This keeps lookup-by-title semantics intact for both pre-rename
  // (filename-named) and post-rename (numeric-keyed) scan facts.
  function buildScanFactsLookup(scanFacts = [], manifestLookup = null) {
    const bySourceNameLower = new Map();
    if (!Array.isArray(scanFacts)) return bySourceNameLower;
    for (const fact of scanFacts) {
      if (!fact || typeof fact !== 'object') continue;
      const sourceNameLower = String(fact.sourceName || fact.embeddedPath || '').split('/').pop().toLowerCase();
      if (sourceNameLower) bySourceNameLower.set(sourceNameLower, fact);
      const sourceNameNoExt = sourceNameLower.replace(/\.[^.]+$/, '');
      if (sourceNameNoExt && sourceNameNoExt !== sourceNameLower) {
        bySourceNameLower.set(sourceNameNoExt, fact);
      }

      // Numeric-keyed scan fact (post-rename): alias by the manifest entry's title so
      // resolveScanFactForAsset can find it via assetEntry.title lookup.
      if (manifestLookup && manifestLookup.byKey && /^\d+$/.test(sourceNameLower)) {
        const manifestEntry = manifestLookup.byKey.get(sourceNameLower);
        const titleLower = String(manifestEntry?.title || '').toLowerCase();
        if (titleLower) {
          if (!bySourceNameLower.has(titleLower)) bySourceNameLower.set(titleLower, fact);
          const titleNoExt = titleLower.replace(/\.[^.]+$/, '');
          if (titleNoExt && titleNoExt !== titleLower && !bySourceNameLower.has(titleNoExt)) {
            bySourceNameLower.set(titleNoExt, fact);
          }
        }
      }
    }
    return bySourceNameLower;
  }

  function resolveScanFactForAsset(scanLookup, assetEntry = null) {
    const assetTitleLower = String(assetEntry?.title || '').toLowerCase();
    const assetTitleNoExt = assetTitleLower.replace(/\.[^.]+$/, '');
    return scanLookup.get(assetTitleLower)
      || scanLookup.get(assetTitleNoExt)
      || null;
  }

  async function checkApprovedRosterExists(projectName = getCurrentProject()) {
    if (!projectName) return false;
    try {
      await loadJsonFromStoragePath(`${ROOT}/${projectName}/ai_asset_roster_approved.json`);
      return true;
    } catch (_) {
      return false;
    }
  }

  function buildTreeIndex(nodes = []) {
    const byKey = new Map();
    const rootOrder = [];
    const childOrderByKey = new Map();

    const visit = (node, isRoot = false) => {
      if (!node || typeof node !== 'object') return;
      const key = String(node.key || '').trim();
      if (!key) return;
      byKey.set(key, node);
      if (isRoot) rootOrder.push(key);
      const childKeys = Array.isArray(node.children)
        ? node.children.map(child => String(child?.key || '').trim()).filter(Boolean)
        : [];
      childOrderByKey.set(key, childKeys);
      for (const child of Array.isArray(node.children) ? node.children : []) visit(child, false);
    };

    for (const node of Array.isArray(nodes) ? nodes : []) visit(node, true);
    return { byKey, rootOrder, childOrderByKey };
  }

  function stableOrderByPreferredKeys(items = [], preferredKeys = []) {
    const preferredIndex = new Map();
    (Array.isArray(preferredKeys) ? preferredKeys : []).forEach((key, index) => {
      const normalized = String(key || '').trim();
      if (normalized) preferredIndex.set(normalized, index);
    });

    return items
      .map((item, index) => {
        const key = String(item?.key || '').trim();
        const preferred = preferredIndex.has(key) ? preferredIndex.get(key) : Number.MAX_SAFE_INTEGER;
        return { item, index, preferred };
      })
      .sort((a, b) => (a.preferred - b.preferred) || (a.index - b.index))
      .map(entry => entry.item);
  }

  function orderEntityKeysByExisting(newEntities = {}, existingEntities = {}) {
    const preferred = Object.keys(existingEntities && typeof existingEntities === 'object' ? existingEntities : {});
    const preferredIndex = new Map(preferred.map((key, index) => [String(key), index]));
    return Object.keys(newEntities || {})
      .map((key, index) => ({
        key,
        index,
        preferred: preferredIndex.has(String(key)) ? preferredIndex.get(String(key)) : Number.MAX_SAFE_INTEGER
      }))
      .sort((a, b) => (a.preferred - b.preferred) || (a.index - b.index))
      .map(entry => entry.key);
  }

  function buildExistingMaterialIndex(existingAssets = []) {
    const materialByKey = new Map();
    const materialOrder = [];
    for (const entry of Array.isArray(existingAssets) ? existingAssets : []) {
      if (!entry || entry.type !== 'material') continue;
      const key = String(entry.key || '').trim();
      if (!key) continue;
      materialByKey.set(key, entry);
      materialOrder.push(key);
    }
    return { materialByKey, materialOrder };
  }

  function buildExistingObjectMaterialKeyIndex(existingEntities = {}, existingAssets = []) {
    const { materialByKey } = buildExistingMaterialIndex(existingAssets);
    const byObjectKey = new Map();
    for (const [entityKey, entity] of Object.entries(existingEntities && typeof existingEntities === 'object' ? existingEntities : {})) {
      if (!entity || entity.type !== 'object') continue;
      const materialKey = String(entity?.data?.['0']?.material_file || '').trim();
      if (materialKey && materialByKey.has(materialKey)) byObjectKey.set(String(entityKey), materialKey);
    }
    return byObjectKey;
  }

  function buildExistingHtmlTemplateIndex(existingEntities = {}) {
    const bySrcKey = new Map();
    const order = [];
    for (const [entityKey, entity] of Object.entries(existingEntities && typeof existingEntities === 'object' ? existingEntities : {})) {
      if (!entity || entity.type !== 'HTMLTemplate') continue;
      const src = String(entity.src || '').trim();
      if (!src) continue;
      bySrcKey.set(src, {
        key: String(entityKey),
        entity: cloneJsonSafe(entity, {})
      });
      order.push(String(entityKey));
    }
    return { bySrcKey, order };
  }

  function buildExistingRigidbodyIndex(existingTree = []) {
    const byObjectKey = new Map();
    const visit = (node) => {
      if (!node || typeof node !== 'object') return;
      const key = String(node.key || '').trim();
      if (!key) return;
      if (node.type === 'object') {
        const rigidbodyChild = (Array.isArray(node.children) ? node.children : []).find(child => child?.type === 'RigidBody');
        if (rigidbodyChild?.key != null) {
          byObjectKey.set(key, {
            key: String(rigidbodyChild.key),
            title: String(rigidbodyChild.title || 'New RigidBody')
          });
        }
      }
      for (const child of Array.isArray(node.children) ? node.children : []) visit(child);
    };
    for (const root of Array.isArray(existingTree) ? existingTree : []) visit(root);
    return byObjectKey;
  }

  async function readExistingCompiledJsonPackage(projectName = getCurrentProject()) {
    if (!projectName) return { assets: [], tree: [], entities: {} };

    const loadSafe = async (relativePath, fallback) => {
      try {
        const parsed = await loadJsonFromStoragePath(`${ROOT}/${projectName}/${relativePath}`);
        return parsed == null ? fallback : parsed;
      } catch (_) {
        return fallback;
      }
    };

    const [assets, tree, entities] = await Promise.all([
      loadSafe('json/assets.json', []),
      loadSafe('json/tree.json', []),
      loadSafe('json/entities.json', {})
    ]);

    return {
      assets: Array.isArray(assets) ? assets : [],
      tree: Array.isArray(tree) ? tree : [],
      entities: entities && typeof entities === 'object' && !Array.isArray(entities) ? entities : {}
    };
  }

  
  async function readSceneIntentJsonOrDefault(projectName = getCurrentProject()) {
    if (!projectName) return createDefaultSceneIntent();
    try {
      const parsed = await loadJsonFromStoragePath(`${ROOT}/${projectName}/json/scene_intent.json`);
      if (!parsed || typeof parsed !== 'object') return createDefaultSceneIntent();
      return {
        settings: {
          rootGroupTitle: parsed?.settings?.rootGroupTitle || 'Scene Root'
        },
        groups: Array.isArray(parsed?.groups) ? parsed.groups : [],
        objects: Array.isArray(parsed?.objects) ? parsed.objects : [],
        rigidbodies: Array.isArray(parsed?.rigidbodies) ? parsed.rigidbodies : []
      };
    } catch (_) {
      return createDefaultSceneIntent();
    }
  }

  function createDefaultMaterialPayload({ templateMaterial = null, albedoRatio = null, albedoTexture = undefined, extent = [1, 1, 1] } = {}) {
    const template = templateMaterial && typeof templateMaterial === 'object' ? templateMaterial : {};
    const resolvedAlbedoTexture = albedoTexture !== undefined
      ? (albedoTexture || null)
      : (Object.prototype.hasOwnProperty.call(template, 'albedo_texture') ? template.albedo_texture : null);
    const resolvedAlbedoVideo = Object.prototype.hasOwnProperty.call(template, 'albedo_video')
      ? template.albedo_video
      : (resolvedAlbedoTexture ? '' : null);
    const resolvedExtent = Array.isArray(extent) && extent.length === 3
      ? extent.slice()
      : (Array.isArray(template.extent) && template.extent.length === 3 ? template.extent.slice() : [1, 1, 1]);

    return {
      albedo_ratio: Array.isArray(albedoRatio) && albedoRatio.length === 3
        ? albedoRatio.slice()
        : (Array.isArray(template.albedo_ratio) && template.albedo_ratio.length === 3 ? template.albedo_ratio.slice() : [255, 255, 255]),
      albedo_texture: resolvedAlbedoTexture,
      albedo_video: resolvedAlbedoVideo,
      ambient_ratio: Array.isArray(template.ambient_ratio) && template.ambient_ratio.length === 3 ? template.ambient_ratio.slice() : [255, 255, 255],
      ambient_texture: Object.prototype.hasOwnProperty.call(template, 'ambient_texture') ? template.ambient_texture : null,
      ambient_video: Object.prototype.hasOwnProperty.call(template, 'ambient_video') ? template.ambient_video : null,
      ao_ratio: Number.isFinite(Number(template.ao_ratio)) ? Number(template.ao_ratio) : 1,
      ao_texture: Object.prototype.hasOwnProperty.call(template, 'ao_texture') ? template.ao_texture : null,
      ao_texture_channel: typeof template.ao_texture_channel === 'string' ? template.ao_texture_channel : 'r',
      diffuse_ibl_ratio: Array.isArray(template.diffuse_ibl_ratio) && template.diffuse_ibl_ratio.length === 3 ? template.diffuse_ibl_ratio.slice() : [255, 255, 255],
      diffuse_ratio: Array.isArray(template.diffuse_ratio) && template.diffuse_ratio.length === 3 ? template.diffuse_ratio.slice() : [255, 255, 255],
      diffuse_texture: Object.prototype.hasOwnProperty.call(template, 'diffuse_texture') ? template.diffuse_texture : null,
      emissive_ratio: Array.isArray(template.emissive_ratio) && template.emissive_ratio.length === 3 ? template.emissive_ratio.slice() : [0, 0, 0],
      emissive_texture: Object.prototype.hasOwnProperty.call(template, 'emissive_texture') ? template.emissive_texture : null,
      extent: resolvedExtent,
      metalness_ratio: Number.isFinite(Number(template.metalness_ratio)) ? Number(template.metalness_ratio) : 1,
      metalness_texture: Object.prototype.hasOwnProperty.call(template, 'metalness_texture') ? template.metalness_texture : null,
      metalness_texture_channel: typeof template.metalness_texture_channel === 'string' ? template.metalness_texture_channel : 'r',
      normal_ratio: Number.isFinite(Number(template.normal_ratio)) ? Number(template.normal_ratio) : 1,
      normal_texture: Object.prototype.hasOwnProperty.call(template, 'normal_texture') ? template.normal_texture : null,
      opacity_ratio: Number.isFinite(Number(template.opacity_ratio)) ? Number(template.opacity_ratio) : 1,
      opacity_texture: Object.prototype.hasOwnProperty.call(template, 'opacity_texture') ? template.opacity_texture : null,
      opacity_texture_channel: typeof template.opacity_texture_channel === 'string' ? template.opacity_texture_channel : 'a',
      roughness_ratio: Number.isFinite(Number(template.roughness_ratio)) ? Number(template.roughness_ratio) : 1,
      roughness_texture: Object.prototype.hasOwnProperty.call(template, 'roughness_texture') ? template.roughness_texture : null,
      roughness_texture_channel: typeof template.roughness_texture_channel === 'string' ? template.roughness_texture_channel : 'r',
      specular_ibl_ratio: Array.isArray(template.specular_ibl_ratio) && template.specular_ibl_ratio.length === 3 ? template.specular_ibl_ratio.slice() : [255, 255, 255],
      specular_pbr_ratio: Array.isArray(template.specular_pbr_ratio) && template.specular_pbr_ratio.length === 3 ? template.specular_pbr_ratio.slice() : [255, 255, 255],
      specular_power: Number.isFinite(Number(template.specular_power)) ? Number(template.specular_power) : 32,
      specular_ratio: Array.isArray(template.specular_ratio) && template.specular_ratio.length === 3 ? template.specular_ratio.slice() : [255, 255, 255],
      specular_texture: Object.prototype.hasOwnProperty.call(template, 'specular_texture') ? template.specular_texture : null,
      use_alpha_channel: Boolean(template.use_alpha_channel),
      use_pbr: Object.prototype.hasOwnProperty.call(template, 'use_pbr') ? Boolean(template.use_pbr) : true,
      uv_animation: Number.isFinite(Number(template.uv_animation)) ? Number(template.uv_animation) : -1
    };
  }

  function createDefaultEntityMaterialSlot({ templateSlot = null, materialKey = '', albedoRatio = null } = {}) {
    const template = templateSlot && typeof templateSlot === 'object' ? templateSlot : {};
    return {
      albedo_ratio: Array.isArray(albedoRatio) && albedoRatio.length === 3
        ? albedoRatio.slice()
        : (Array.isArray(template.albedo_ratio) && template.albedo_ratio.length === 3 ? template.albedo_ratio.slice() : [255, 255, 255]),
      albedo_texture: typeof template.albedo_texture === 'string' ? template.albedo_texture : '',
      albedo_video: typeof template.albedo_video === 'string' ? template.albedo_video : '',
      ambient_ratio: Array.isArray(template.ambient_ratio) && template.ambient_ratio.length === 3 ? template.ambient_ratio.slice() : [255, 255, 255],
      ambient_texture: typeof template.ambient_texture === 'string' ? template.ambient_texture : '',
      ambient_video: typeof template.ambient_video === 'string' ? template.ambient_video : '',
      ao_ratio: Number.isFinite(Number(template.ao_ratio)) ? Number(template.ao_ratio) : 1,
      ao_texture: typeof template.ao_texture === 'string' ? template.ao_texture : '',
      ao_texture_channel: typeof template.ao_texture_channel === 'string' ? template.ao_texture_channel : 'r',
      diffuse_ibl_ratio: Array.isArray(template.diffuse_ibl_ratio) && template.diffuse_ibl_ratio.length === 3 ? template.diffuse_ibl_ratio.slice() : [255, 255, 255],
      diffuse_ratio: Array.isArray(template.diffuse_ratio) && template.diffuse_ratio.length === 3 ? template.diffuse_ratio.slice() : [255, 255, 255],
      diffuse_texture: typeof template.diffuse_texture === 'string' ? template.diffuse_texture : '',
      emissive_ratio: Array.isArray(template.emissive_ratio) && template.emissive_ratio.length === 3 ? template.emissive_ratio.slice() : [0, 0, 0],
      emissive_texture: typeof template.emissive_texture === 'string' ? template.emissive_texture : '',
      extent: Array.isArray(template.extent) && template.extent.length === 3 ? template.extent.slice() : [0, 0, 0],
      metalness_ratio: Number.isFinite(Number(template.metalness_ratio)) ? Number(template.metalness_ratio) : 1,
      metalness_texture: typeof template.metalness_texture === 'string' ? template.metalness_texture : '',
      metalness_texture_channel: typeof template.metalness_texture_channel === 'string' ? template.metalness_texture_channel : 'r',
      normal_ratio: Number.isFinite(Number(template.normal_ratio)) ? Number(template.normal_ratio) : 1,
      normal_texture: typeof template.normal_texture === 'string' ? template.normal_texture : '',
      opacity_ratio: Number.isFinite(Number(template.opacity_ratio)) ? Number(template.opacity_ratio) : 1,
      opacity_texture: typeof template.opacity_texture === 'string' ? template.opacity_texture : '',
      opacity_texture_channel: typeof template.opacity_texture_channel === 'string' ? template.opacity_texture_channel : 'a',
      roughness_ratio: Number.isFinite(Number(template.roughness_ratio)) ? Number(template.roughness_ratio) : 1,
      roughness_texture: typeof template.roughness_texture === 'string' ? template.roughness_texture : '',
      roughness_texture_channel: typeof template.roughness_texture_channel === 'string' ? template.roughness_texture_channel : 'r',
      specular_ibl_ratio: Array.isArray(template.specular_ibl_ratio) && template.specular_ibl_ratio.length === 3 ? template.specular_ibl_ratio.slice() : [255, 255, 255],
      specular_pbr_ratio: Array.isArray(template.specular_pbr_ratio) && template.specular_pbr_ratio.length === 3 ? template.specular_pbr_ratio.slice() : [255, 255, 255],
      specular_power: Number.isFinite(Number(template.specular_power)) ? Number(template.specular_power) : 32,
      specular_ratio: Array.isArray(template.specular_ratio) && template.specular_ratio.length === 3 ? template.specular_ratio.slice() : [255, 255, 255],
      specular_texture: typeof template.specular_texture === 'string' ? template.specular_texture : '',
      use_alpha_channel: Boolean(template.use_alpha_channel),
      use_pbr: Object.prototype.hasOwnProperty.call(template, 'use_pbr') ? Boolean(template.use_pbr) : true,
      uv_animation: Number.isFinite(Number(template.uv_animation)) ? Number(template.uv_animation) : 0,
      material_file: materialKey || (typeof template.material_file === 'string' ? template.material_file : '')
    };
  }

  function collectSceneIntentGroupKeys(sceneIntent = null) {
    const keys = new Set();
    for (const group of Array.isArray(sceneIntent?.groups) ? sceneIntent.groups : []) {
      const key = String(group?.key || '').trim();
      if (key) keys.add(key);
    }
    return keys;
  }

  function expandSceneIntentObjects(sceneIntent = null) {
    const expanded = [];
    for (const objectSpec of Array.isArray(sceneIntent?.objects) ? sceneIntent.objects : []) {
      if (!objectSpec || typeof objectSpec !== 'object') continue;
      const baseKey = String(objectSpec.key || '').trim();
      if (!baseKey) continue;
      const count = Math.max(1, Number.parseInt(objectSpec.count, 10) || 1);
      for (let index = 0; index < count; index += 1) {
        const sceneNodeKey = index === 0 ? baseKey : `${baseKey}__${index + 1}`;
        expanded.push({
          ...cloneJsonSafe(objectSpec, {}),
          key: sceneNodeKey,
          sourceKey: baseKey,
          cloneIndex: index,
          count
        });
      }
    }
    return expanded;
  }

  function validateSceneIntent(sceneIntent, manifestEntries = []) {
    assertNoForbiddenLegacyPrimitiveModelEntries(manifestEntries, 'bootstrap assets manifest');
    const manifestLookup = buildManifestLookup(manifestEntries);
    const groupKeys = collectSceneIntentGroupKeys(sceneIntent);
    const errors = [];
    const claimedSceneKeys = new Map();

    const claimSceneKey = (key, label) => {
      const normalizedKey = String(key || '').trim();
      if (!normalizedKey) return;
      const existingLabel = claimedSceneKeys.get(normalizedKey);
      if (existingLabel) {
        errors.push(`scene_intent key collision: ${label} uses key "${normalizedKey}", already claimed by ${existingLabel}.`);
        return;
      }
      claimedSceneKeys.set(normalizedKey, label);
    };

    for (const group of Array.isArray(sceneIntent?.groups) ? sceneIntent.groups : []) {
      const key = String(group?.key || '').trim();
      const parent = group?.parent == null ? null : String(group.parent).trim();
      if (!key) {
        errors.push('scene_intent.groups contains an entry with a missing key.');
        continue;
      }
      claimSceneKey(key, `scene_intent.groups[${JSON.stringify(key)}]`);
      if (parent && !groupKeys.has(parent)) {
        errors.push(`scene_intent.groups "${key}" references missing parent "${parent}".`);
      }
    }

    for (const objectSpec of Array.isArray(sceneIntent?.objects) ? sceneIntent.objects : []) {
      const key = String(objectSpec?.key || '').trim();
      const assetKey = String(objectSpec?.assetKey || '').trim();
      const parent = objectSpec?.parent == null ? null : String(objectSpec.parent).trim();
      const count = Math.max(1, Number.parseInt(objectSpec?.count, 10) || 1);

      if (!key) errors.push('scene_intent.objects contains an entry with a missing key.');
      if (!assetKey || !manifestLookup.byKey.has(assetKey)) {
        errors.push(`scene_intent object "${key || '(missing key)'}" references unknown assetKey "${assetKey}".`);
      }
      const assetEntry = manifestLookup.byKey.get(assetKey) || null;
      const assetTitleLower = String(assetEntry?.title || '').trim().toLowerCase();
      if (FORBIDDEN_LEGACY_PRIMITIVE_MODEL_KEYS.has(assetKey) || FORBIDDEN_LEGACY_PRIMITIVE_MODEL_TITLES.has(assetTitleLower)) {
        errors.push(`scene_intent object "${key || '(missing key)'}" references forbidden legacy primitive model assetKey "${assetKey}" (${assetEntry?.title || 'unknown title'}). Use only .primitives keys 4-14.`);
      }
      if (parent && !groupKeys.has(parent)) {
        errors.push(`scene_intent object "${key || '(missing key)'}" references missing parent group "${parent}".`);
      }
      if (!(count >= 1)) {
        errors.push(`scene_intent object "${key || '(missing key)'}" must have count >= 1.`);
      }
      if (key) {
        for (let index = 0; index < count; index += 1) {
          const expandedKey = index === 0 ? key : `${key}__${index + 1}`;
          claimSceneKey(expandedKey, `scene_intent.objects[${JSON.stringify(key)}]`);
        }
      }
    }

    for (const rigidbodySpec of Array.isArray(sceneIntent?.rigidbodies) ? sceneIntent.rigidbodies : []) {
      const key = String(rigidbodySpec?.key || '').trim();
      const parent = rigidbodySpec?.parent == null ? null : String(rigidbodySpec.parent).trim();
      if (!key) errors.push('scene_intent.rigidbodies contains an entry with a missing key.');
      if (parent && !groupKeys.has(parent)) {
        errors.push(`scene_intent rigidbody "${key || '(missing key)'}" references missing parent group "${parent}".`);
      }
      if (key) claimSceneKey(key, `scene_intent.rigidbodies[${JSON.stringify(key)}]`);
    }

    if (errors.length > 0) {
      throw new Error(errors.join(' '));
    }

    return true;
  }

  async function getOrBuildExtendedModelAnalysis(projectName = getCurrentProject(), options = {}) {
    const { forceRescan = false, allowedAssetNames = null } = options;
    if (!projectName) return [];

    if (!forceRescan) {
      const cached = getCachedModelAnalysis(projectName);
      if (cached.length > 0) return cached;
    }

    const modelsRef = ref(storage, `${ROOT}/${projectName}/models`);
    const listing = await listAll(modelsRef).catch(() => ({ items: [] }));
    const allowedNamesLower = allowedAssetNames instanceof Set
      ? new Set(Array.from(allowedAssetNames).map(name => String(name || '').toLowerCase()))
      : null;

    // After renameModelFilesToKeys runs, 3D files in models/ are stored under their
    // numeric manifest key (e.g. "129") rather than their original filename — they
    // have no extension and are pure digits. The frontend rename helper records the
    // original mime type for each renamed key in window._rosterKeyToMimeType. Use
    // that map to recover the loader type for numeric-keyed files; without this the
    // extension filter below silently skips every renamed roster 3D asset, leaving
    // scanFacts empty and breaking downstream geometry/texture contracts.
    const keyToMimeType = (typeof window !== 'undefined' && window._rosterKeyToMimeType instanceof Map)
      ? window._rosterKeyToMimeType
      : null;

    const analyses = [];
    for (const item of listing.items) {
      const lowerName = String(item.name || '').toLowerCase();
      if (item.name === '2' || item.name === '23') continue;

      // Reserved Cherry3D runtime artifacts: numeric-extensionless files NOT registered
      // as a renamed roster 3D asset in the mime-type map. Genuine renamed roster files
      // (also numeric-extensionless) are handled below via keyToMimeType lookup.
      const isNumericExtensionless = !/\./.test(item.name) && /^\d+$/.test(item.name);
      const renamedMime = (isNumericExtensionless && keyToMimeType) ? keyToMimeType.get(item.name) : null;
      if (isNumericExtensionless && !renamedMime) continue;

      const has3dExt = ['.obj', '.glb', '.gltf', '.fbx'].some(ext => lowerName.endsWith(ext));
      if (!has3dExt && !renamedMime) continue;
      if (allowedNamesLower && !allowedNamesLower.has(lowerName)) continue;

      try {
        const url = await getDownloadURL(item);
        const resolvedType = renamedMime
          || (lowerName.endsWith('.obj') ? 'model/obj'
            : lowerName.endsWith('.fbx') ? 'model/fbx'
            : 'model/gltf-binary');
        const analysis = await analyzeDirectThreeAsset({
          name: item.name,
          path: `models/${item.name}`,
          url,
          type: resolvedType
        });
        if (analysis) analyses.push(analysis);
      } catch (error) {
        console.warn(`getOrBuildExtendedModelAnalysis: failed to analyze ${item.name}`, error);
      }
    }

    if (analyses.length > 0) {
      cacheModelAnalysis(projectName, analyses);
    }

    return analyses;
  }

  function deriveCompiledMaterialTitle(sceneObject, assetEntry) {
    const objectTitle = String(sceneObject?.title || '').trim();
    if (objectTitle) return objectTitle;
    const assetTitle = String(assetEntry?.title || '').trim();
    if (assetTitle) return assetTitle.replace(/\.[^.]+$/, '');
    return `Material ${sceneObject?.sourceKey || sceneObject?.key || 'Object'}`;
  }

  function nextAvailableNumericKey(usedKeys, startAt = 1) {
    let next = Math.max(1, Number(startAt) || 1);
    while (usedKeys.has(String(next))) next += 1;
    usedKeys.add(String(next));
    return String(next);
  }

  function buildCanonicalAssetsJson(input = {}) {
    const {
      bootstrapAssets = [],
      annotatedRoster = null,
      sceneIntent = null,
      scanFacts = [],
      existingAssets = [],
      existingEntities = {}
    } = input;

    const baseEntries = cloneJsonSafe(
      (Array.isArray(bootstrapAssets) ? bootstrapAssets : []).filter(entry => entry?.type !== 'material'),
      []
    );
    const usedKeys = new Set(
      flattenAssetManifestEntries(baseEntries)
        .map(entry => entry?.key != null ? String(entry.key) : '')
        .filter(Boolean)
    );
    const highestExistingKey = Array.from(usedKeys).reduce((max, key) => {
      const num = Number.parseInt(key, 10);
      return Number.isFinite(num) ? Math.max(max, num) : max;
    }, 0);

    const manifestLookup = buildManifestLookup(baseEntries);
    const rosterLookup = buildApprovedRosterLookup(annotatedRoster);
    const scanLookup = buildScanFactsLookup(scanFacts, manifestLookup);
    const expandedObjects = expandSceneIntentObjects(sceneIntent);
    const materialKeyBySourceKey = new Map();
    const materialEntriesByKey = new Map();
    const { materialByKey: existingMaterialByKey, materialOrder: existingMaterialOrder } = buildExistingMaterialIndex(existingAssets);
    const existingObjectMaterialKeyByObjectKey = buildExistingObjectMaterialKeyIndex(existingEntities, existingAssets);

    for (const sceneObject of expandedObjects) {
      const sourceKey = String(sceneObject.sourceKey || sceneObject.key || '');
      if (materialKeyBySourceKey.has(sourceKey)) continue;

      const assetEntry = manifestLookup.byKey.get(String(sceneObject.assetKey || '')) || null;
      const rosterAsset =
        rosterLookup.byManifestKey.get(String(sceneObject.assetKey || '')) ||
        rosterLookup.byAssetNameLower.get(String(assetEntry?.title || '').toLowerCase()) ||
        null;
      const scanFact = resolveScanFactForAsset(scanLookup, assetEntry);
      const albedoRatio = Array.isArray(sceneObject?.style?.albedo_ratio) ? sceneObject.style.albedo_ratio : null;
      const colormapKey = rosterAsset?.colormapManifestKey || null;
      const extent = [
        Number(scanFact?.boundingBox?.width || 1),
        Number(scanFact?.boundingBox?.height || 1),
        Number(scanFact?.boundingBox?.depth || 1)
      ].map(value => Number.isFinite(value) && value > 0 ? Number(value.toFixed(3)) : 1);

      const existingMaterialKey = String(existingObjectMaterialKeyByObjectKey.get(String(sceneObject.key)) || existingObjectMaterialKeyByObjectKey.get(sourceKey) || '').trim();
      let materialKey = '';
      if (existingMaterialKey && !usedKeys.has(existingMaterialKey)) {
        materialKey = existingMaterialKey;
        usedKeys.add(materialKey);
      } else {
        materialKey = nextAvailableNumericKey(usedKeys, highestExistingKey + materialEntriesByKey.size + 1);
      }

      const existingMaterialEntry = existingMaterialByKey.get(materialKey) || null;
      const materialTitle = String(existingMaterialEntry?.title || deriveCompiledMaterialTitle(sceneObject, assetEntry));
      materialEntriesByKey.set(materialKey, {
        key: materialKey,
        type: 'material',
        title: materialTitle,
        children: [],
        material: createDefaultMaterialPayload({
          templateMaterial: existingMaterialEntry?.material || null,
          albedoRatio,
          albedoTexture: colormapKey ? String(colormapKey) : undefined,
          extent
        })
      });
      materialKeyBySourceKey.set(sourceKey, materialKey);
    }

    const orderedMaterialEntries = stableOrderByPreferredKeys(
      Array.from(materialEntriesByKey.values()),
      existingMaterialOrder
    );

    return { assets: [...baseEntries, ...orderedMaterialEntries], materialKeyBySourceKey };
  }

  function buildCanonicalTreeJson(input = {}) {
    const { sceneIntent = null, existingTree = [], rigidbodyPlan = null } = input;
    const groups = Array.isArray(sceneIntent?.groups) ? sceneIntent.groups : [];
    const objects = expandSceneIntentObjects(sceneIntent);
    const rootNodes = [];
    const groupNodeByKey = new Map();
    const existingTreeIndex = buildTreeIndex(existingTree);

    for (const group of groups) {
      const key = String(group?.key || '').trim();
      if (!key) continue;
      groupNodeByKey.set(key, {
        key,
        title: String(group?.title || key),
        visible: group?.visible !== false,
        children: [],
        type: 'object-group'
      });
    }

    for (const group of groups) {
      const key = String(group?.key || '').trim();
      if (!key) continue;
      const parentKey = group?.parent == null ? null : String(group.parent).trim();
      const node = groupNodeByKey.get(key);
      if (parentKey && groupNodeByKey.has(parentKey)) {
        groupNodeByKey.get(parentKey).children.push(node);
      } else {
        rootNodes.push(node);
      }
    }

    for (const objectSpec of objects) {
      const objectNode = {
        key: String(objectSpec.key),
        title: String(objectSpec.title || objectSpec.key),
        visible: objectSpec.visible !== false,
        children: [],
        type: 'object',
        id: String(objectSpec.assetKey || '')
      };

      const rigidbodyDescriptor = rigidbodyPlan instanceof Map ? rigidbodyPlan.get(String(objectSpec.key)) : null;
      if (objectSpec?.rigidbody?.enabled && rigidbodyDescriptor) {
        objectNode.children.push({
          key: rigidbodyDescriptor.key,
          title: rigidbodyDescriptor.title,
          visible: true,
          children: [],
          type: 'RigidBody'
        });
      }

      const parentKey = objectSpec?.parent == null ? null : String(objectSpec.parent).trim();
      if (parentKey && groupNodeByKey.has(parentKey)) {
        groupNodeByKey.get(parentKey).children.push(objectNode);
      } else {
        rootNodes.push(objectNode);
      }
    }

    for (const rigidbodySpec of Array.isArray(sceneIntent?.rigidbodies) ? sceneIntent.rigidbodies : []) {
      const rigidbodyNode = {
        key: String(rigidbodySpec.key),
        title: String(rigidbodySpec.title || rigidbodySpec.key),
        visible: rigidbodySpec.visible !== false,
        children: [],
        type: 'RigidBody'
      };
      const parentKey = rigidbodySpec?.parent == null ? null : String(rigidbodySpec.parent).trim();
      if (parentKey && groupNodeByKey.has(parentKey)) {
        groupNodeByKey.get(parentKey).children.push(rigidbodyNode);
      } else {
        rootNodes.push(rigidbodyNode);
      }
    }

    for (const [groupKey, groupNode] of groupNodeByKey.entries()) {
      const preferredChildKeys = existingTreeIndex.childOrderByKey.get(groupKey) || [];
      groupNode.children = stableOrderByPreferredKeys(groupNode.children, preferredChildKeys);
    }
    for (const rootNode of rootNodes) {
      const preferredChildKeys = existingTreeIndex.childOrderByKey.get(String(rootNode?.key || '')) || [];
      if (Array.isArray(rootNode?.children) && rootNode.children.length > 0) {
        rootNode.children = stableOrderByPreferredKeys(rootNode.children, preferredChildKeys);
      }
    }

    return stableOrderByPreferredKeys(rootNodes, existingTreeIndex.rootOrder);
  }

  function buildCanonicalEntitiesJson(input = {}) {
    const {
      sceneIntent = null,
      assets = [],
      scanFacts = [],
      materialKeyBySourceKey = new Map(),
      existingEntities = {},
      existingTree = [],
      rigidbodyPlan = null
    } = input;
    const sceneEntities = {};
    const manifestLookup = buildManifestLookup(assets);
    const scanLookup = buildScanFactsLookup(scanFacts, manifestLookup);

    for (const group of Array.isArray(sceneIntent?.groups) ? sceneIntent.groups : []) {
      const key = String(group?.key || '').trim();
      if (!key) continue;
      const existingGroup = existingEntities?.[key] && typeof existingEntities[key] === 'object'
        ? existingEntities[key] : null;
      sceneEntities[key] = {
        type: 'object-group',
        position: normalizeNumericVector(existingGroup?.position, [0, 0, 0]),
        rotate:   normalizeNumericVector(existingGroup?.rotate,   [0, 0, 0]),
        scale:    normalizeNumericVector(existingGroup?.scale,     [1, 1, 1]),
        visible: group?.visible !== false,
        key,
        groupMat: Array.isArray(existingGroup?.groupMat) && existingGroup.groupMat.length === 16
          ? existingGroup.groupMat.slice()
          : DEFAULT_GROUP_MAT.slice()
      };
    }

    for (const objectSpec of expandSceneIntentObjects(sceneIntent)) {
      const key = String(objectSpec.key);
      const sourceKey = String(objectSpec.sourceKey || objectSpec.key);
      const assetEntry = manifestLookup.byKey.get(String(objectSpec.assetKey || '')) || null;
      const scanFact = resolveScanFactForAsset(scanLookup, assetEntry);
      const existingEntity = existingEntities?.[key] && typeof existingEntities[key] === 'object' ? existingEntities[key] : null;
      const existingSlotKeys = existingEntity?.data && typeof existingEntity.data === 'object'
        ? Object.keys(existingEntity.data).filter(slot => /^\d+$/.test(slot)).sort((a, b) => Number(a) - Number(b))
        : [];
      const computedSlotCount = Math.max(1, Number.parseInt(scanFact?.slotCount, 10) || Number.parseInt(scanFact?.meshCount, 10) || 1);
      const slotCount = Math.max(computedSlotCount, existingSlotKeys.length || 0, 1);
      const materialKey = String(materialKeyBySourceKey.get(sourceKey) || '');
      const albedoRatio = Array.isArray(objectSpec?.style?.albedo_ratio) ? objectSpec.style.albedo_ratio : null;
      const templateSlot = existingEntity?.data?.['0'] && typeof existingEntity.data['0'] === 'object'
        ? existingEntity.data['0']
        : null;

      const data = {};
      for (let slot = 0; slot < slotCount; slot += 1) {
        const slotKey = String(slot);
        const existingSlot = existingEntity?.data?.[slotKey] && typeof existingEntity.data[slotKey] === 'object'
          ? existingEntity.data[slotKey]
          : templateSlot;
        data[slotKey] = createDefaultEntityMaterialSlot({
          templateSlot: existingSlot,
          materialKey,
          albedoRatio
        });
      }

      const entity = {
        type: 'object',
        position: normalizeNumericVector(objectSpec?.transform?.position, [0, 0, 0]),
        rotate: normalizeNumericVector(objectSpec?.transform?.rotate, [0, 0, 0]),
        scale: normalizeNumericVector(objectSpec?.transform?.scale, [1, 1, 1]),
        groupMat: DEFAULT_GROUP_MAT.slice(),
        data,
        hud: existingEntity?.hud === true,
        show_shadow: Object.prototype.hasOwnProperty.call(existingEntity || {}, 'show_shadow') ? Boolean(existingEntity.show_shadow) : true,
        cast_shadow: Object.prototype.hasOwnProperty.call(existingEntity || {}, 'cast_shadow') ? Boolean(existingEntity.cast_shadow) : true,
        visible: objectSpec.visible !== false,
        frame: Array.isArray(existingEntity?.frame) && existingEntity.frame.length === 2 ? existingEntity.frame.slice() : ['0', 0],
        key,
        autoscale: Number.isFinite(Number(existingEntity?.autoscale)) ? Number(existingEntity.autoscale) : 1,
        pivot: Array.isArray(existingEntity?.pivot) && existingEntity.pivot.length === 3 ? existingEntity.pivot.slice() : [0, 0, 0]
      };
      if (Object.prototype.hasOwnProperty.call(existingEntity || {}, 'render_back_faces') || Object.prototype.hasOwnProperty.call(objectSpec || {}, 'render_back_faces')) {
        entity.render_back_faces = Object.prototype.hasOwnProperty.call(objectSpec || {}, 'render_back_faces')
          ? Boolean(objectSpec.render_back_faces)
          : Boolean(existingEntity?.render_back_faces);
      }
      sceneEntities[key] = entity;

      const rigidbodyDescriptor = rigidbodyPlan instanceof Map ? rigidbodyPlan.get(key) : null;
      if (objectSpec?.rigidbody?.enabled && rigidbodyDescriptor) {
        const existingRigidbodyEntity = existingEntities?.[rigidbodyDescriptor.key] && typeof existingEntities[rigidbodyDescriptor.key] === 'object'
          ? existingEntities[rigidbodyDescriptor.key]
          : null;
        const rigidbodySpec = objectSpec?.rigidbody && typeof objectSpec.rigidbody === 'object' ? objectSpec.rigidbody : {};
        const rigidbodyEntity = {
          type: 'RigidBody',
          position: normalizeNumericVector(rigidbodySpec.position, Array.isArray(existingRigidbodyEntity?.position) ? existingRigidbodyEntity.position : [0, 0, 0]),
          rotate: normalizeNumericVector(rigidbodySpec.rotate, Array.isArray(existingRigidbodyEntity?.rotate) ? existingRigidbodyEntity.rotate : [0, 0, 0]),
          scale: normalizeNumericVector(rigidbodySpec.scale, Array.isArray(existingRigidbodyEntity?.scale) ? existingRigidbodyEntity.scale : [1, 1, 1]),
          visible: Object.prototype.hasOwnProperty.call(rigidbodySpec, 'visible') ? Boolean(rigidbodySpec.visible) : (existingRigidbodyEntity?.visible !== false),
          shape_type: String(rigidbodySpec.shape_type || existingRigidbodyEntity?.shape_type || 'box'),
          groupMat: Array.isArray(existingRigidbodyEntity?.groupMat) && existingRigidbodyEntity.groupMat.length === 16
            ? existingRigidbodyEntity.groupMat.slice()
            : DEFAULT_GROUP_MAT.slice(),
          key: rigidbodyDescriptor.key
        };
        const motionType = rigidbodySpec.motion_type || existingRigidbodyEntity?.motion_type;
        if (motionType != null && motionType !== '') rigidbodyEntity.motion_type = String(motionType);
        const massValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'mass') ? rigidbodySpec.mass : existingRigidbodyEntity?.mass;
        if (massValue != null && massValue !== '' && Number.isFinite(Number(massValue))) rigidbodyEntity.mass = Number(massValue);
        const frictionValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'friction') ? rigidbodySpec.friction : existingRigidbodyEntity?.friction;
        if (frictionValue != null && frictionValue !== '' && Number.isFinite(Number(frictionValue))) rigidbodyEntity.friction = Number(frictionValue);
        const ghostValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'ghost') ? rigidbodySpec.ghost : existingRigidbodyEntity?.ghost;
        if (ghostValue != null) rigidbodyEntity.ghost = Boolean(ghostValue);
        sceneEntities[rigidbodyDescriptor.key] = rigidbodyEntity;
      }
    }

    for (const rigidbodySpec of Array.isArray(sceneIntent?.rigidbodies) ? sceneIntent.rigidbodies : []) {
      const key = String(rigidbodySpec.key || '').trim();
      if (!key) continue;
      const existingRigidbodyEntity = existingEntities?.[key] && typeof existingEntities[key] === 'object'
        ? existingEntities[key]
        : null;
      const standaloneRigidbody = {
        type: 'RigidBody',
        position: normalizeNumericVector(rigidbodySpec.position, Array.isArray(existingRigidbodyEntity?.position) ? existingRigidbodyEntity.position : [0, 0, 0]),
        rotate: normalizeNumericVector(rigidbodySpec.rotate, Array.isArray(existingRigidbodyEntity?.rotate) ? existingRigidbodyEntity.rotate : [0, 0, 0]),
        scale: normalizeNumericVector(rigidbodySpec.scale, Array.isArray(existingRigidbodyEntity?.scale) ? existingRigidbodyEntity.scale : [1, 1, 1]),
        visible: Object.prototype.hasOwnProperty.call(rigidbodySpec, 'visible') ? Boolean(rigidbodySpec.visible) : (existingRigidbodyEntity?.visible !== false),
        shape_type: String(rigidbodySpec.shape_type || existingRigidbodyEntity?.shape_type || 'box'),
        groupMat: Array.isArray(existingRigidbodyEntity?.groupMat) && existingRigidbodyEntity.groupMat.length === 16
          ? existingRigidbodyEntity.groupMat.slice()
          : DEFAULT_GROUP_MAT.slice(),
        key
      };
      const motionType = rigidbodySpec.motion_type || existingRigidbodyEntity?.motion_type;
      if (motionType != null && motionType !== '') standaloneRigidbody.motion_type = String(motionType);
      const massValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'mass') ? rigidbodySpec.mass : existingRigidbodyEntity?.mass;
      if (massValue != null && massValue !== '' && Number.isFinite(Number(massValue))) standaloneRigidbody.mass = Number(massValue);
      const frictionValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'friction') ? rigidbodySpec.friction : existingRigidbodyEntity?.friction;
      if (frictionValue != null && frictionValue !== '' && Number.isFinite(Number(frictionValue))) standaloneRigidbody.friction = Number(frictionValue);
      const ghostValue = Object.prototype.hasOwnProperty.call(rigidbodySpec, 'ghost') ? rigidbodySpec.ghost : existingRigidbodyEntity?.ghost;
      if (ghostValue != null) standaloneRigidbody.ghost = Boolean(ghostValue);
      sceneEntities[key] = standaloneRigidbody;
    }

    const { bySrcKey: existingHtmlTemplates } = buildExistingHtmlTemplateIndex(existingEntities);
    const usedKeys = new Set(Object.keys(sceneEntities));
    const htmlAssets = (Array.isArray(assets) ? assets : []).filter(entry => entry?.type === 'HTML' || String(entry?.extension || '').toLowerCase() === 'html');
    for (const assetEntry of htmlAssets) {
      const srcKey = String(assetEntry?.key || '').trim();
      if (!srcKey) continue;
      const existingTemplate = existingHtmlTemplates.get(srcKey) || null;
      let templateKey = '';
      if (existingTemplate?.key && !usedKeys.has(existingTemplate.key)) {
        templateKey = existingTemplate.key;
        usedKeys.add(templateKey);
      } else {
        templateKey = nextAvailableNumericKey(usedKeys, 1);
      }
      sceneEntities[templateKey] = {
        key: templateKey,
        src: srcKey,
        type: 'HTMLTemplate'
      };
    }

    const orderedEntities = {};
    for (const entityKey of orderEntityKeysByExisting(sceneEntities, existingEntities)) {
      orderedEntities[entityKey] = sceneEntities[entityKey];
    }
    return orderedEntities;
  }

  function buildCanonicalJsonPackage(input = {}) {
    const { bootstrapAssets = [], sceneIntent = null, existingTree = [], existingEntities = {} } = input;
    validateSceneIntent(sceneIntent, bootstrapAssets);

    const sceneAndReservedKeys = new Set();
    for (const group of Array.isArray(sceneIntent?.groups) ? sceneIntent.groups : []) {
      const key = String(group?.key || '').trim();
      if (key) sceneAndReservedKeys.add(key);
    }
    for (const objectSpec of expandSceneIntentObjects(sceneIntent)) {
      const key = String(objectSpec?.key || '').trim();
      if (key) sceneAndReservedKeys.add(key);
    }
    for (const rigidbodySpec of Array.isArray(sceneIntent?.rigidbodies) ? sceneIntent.rigidbodies : []) {
      const key = String(rigidbodySpec?.key || '').trim();
      if (key) sceneAndReservedKeys.add(key);
    }
    const existingRigidbodyByObjectKey = buildExistingRigidbodyIndex(existingTree);
    const rigidbodyPlan = new Map();
    const rigidbodyKeyPool = new Set(sceneAndReservedKeys);
    for (const objectSpec of expandSceneIntentObjects(sceneIntent)) {
      if (!objectSpec?.rigidbody?.enabled) continue;
      const objectKey = String(objectSpec.key);
      const existingDescriptor = existingRigidbodyByObjectKey.get(objectKey) || null;
      const explicitKey = String(objectSpec?.rigidbody?.key || '').trim();
      let rigidbodyKey = explicitKey || (existingDescriptor?.key && !String(existingDescriptor.key).includes('__rb') ? String(existingDescriptor.key) : '');
      if (!rigidbodyKey || rigidbodyKeyPool.has(rigidbodyKey)) {
        if (existingDescriptor?.key && !String(existingDescriptor.key).includes('__rb') && !rigidbodyPlan.has(objectKey) && !rigidbodyKeyPool.has(String(existingDescriptor.key))) {
          rigidbodyKey = String(existingDescriptor.key);
          rigidbodyKeyPool.add(rigidbodyKey);
        } else {
          rigidbodyKey = nextAvailableNumericKey(rigidbodyKeyPool, 1);
        }
      } else {
        rigidbodyKeyPool.add(rigidbodyKey);
      }
      const rigidbodyTitle = String(
        objectSpec?.rigidbody?.treeTitle ||
        objectSpec?.rigidbody?.title ||
        existingDescriptor?.title ||
        'New RigidBody'
      );
      rigidbodyPlan.set(objectKey, { key: rigidbodyKey, title: rigidbodyTitle });
    }

    const compiledAssets = buildCanonicalAssetsJson(input);
    assertNoForbiddenLegacyPrimitiveModelEntries(compiledAssets.assets, 'compiled assets.json');
    const tree = buildCanonicalTreeJson({ sceneIntent, existingTree, rigidbodyPlan });
    const entities = buildCanonicalEntitiesJson({
      sceneIntent,
      assets: compiledAssets.assets,
      scanFacts: input.scanFacts || [],
      materialKeyBySourceKey: compiledAssets.materialKeyBySourceKey,
      existingEntities,
      existingTree,
      rigidbodyPlan
    });

    return { assets: compiledAssets.assets, tree, entities };
  }

  async function writeCompiledJsonPackageAtomically(projectName, compiled = {}) {
    if (!projectName) throw new Error('writeCompiledJsonPackageAtomically requires projectName.');

    const payloadMap = {
      'json/assets.json': JSON.stringify(compiled.assets || []),
      'json/tree.json': JSON.stringify(compiled.tree || []),
      'json/entities.json': JSON.stringify(compiled.entities || {})
    };

    for (const [path, payload] of Object.entries(payloadMap)) {
      try {
        JSON.parse(payload);
      } catch (error) {
        throw new Error(`Compiler produced invalid ${path}: ${error.message}`);
      }
    }

    const previousPayloads = new Map();
    for (const path of Object.keys(payloadMap)) {
      try {
        const livePayload = await fetchStorageTextSafe(ref(storage, `${ROOT}/${projectName}/${path}`));
        previousPayloads.set(path, livePayload.text);
      } catch (_) {
        previousPayloads.set(path, null);
      }
    }

    const tempRefs = new Map();
    const uploadedTemps = [];
    const uploadedFinals = [];
    try {
      try {
        for (const [path, payload] of Object.entries(payloadMap)) {
          const tempRef = ref(storage, `${ROOT}/${projectName}/.compiler_tmp/${path.replace(/\//g, '__')}`);
          tempRefs.set(path, tempRef);
          await uploadString(tempRef, payload);
          uploadedTemps.push(path);
        }
      } catch (tempError) {
        for (const path of uploadedTemps.slice().reverse()) {
          const tempRef = tempRefs.get(path);
          if (!tempRef) continue;
          try {
            await deleteObject(tempRef);
          } catch (cleanupError) {
            console.error(`Temp cleanup failed for ${path}`, cleanupError);
          }
        }
        throw tempError;
      }

      for (const [path, payload] of Object.entries(payloadMap)) {
        await uploadString(ref(storage, `${ROOT}/${projectName}/${path}`), payload);
        uploadedFinals.push(path);
      }
    } catch (error) {
      for (const path of uploadedFinals.reverse()) {
        const targetRef = ref(storage, `${ROOT}/${projectName}/${path}`);
        const previousPayload = previousPayloads.get(path);
        try {
          if (previousPayload == null) {
            await deleteObject(targetRef);
          } else {
            await uploadString(targetRef, previousPayload);
          }
        } catch (rollbackError) {
          console.error(`Rollback failed for ${path}`, rollbackError);
        }
      }
      throw error;
    }

    return payloadMap;
  }

  async function compileProjectJsonPackage(projectName = getCurrentProject(), options = {}) {
    const {
      forceRescan = false,
      preAnnotatedRoster,
      degradedOutputAccepted = false
    } = options;
    if (!projectName) throw new Error('compileProjectJsonPackage requires projectName.');

    const bootstrapAssets = await syncAssetsJson(projectName);
    const scanFacts = await getOrBuildExtendedModelAnalysis(projectName, { forceRescan });

    let annotatedRoster = typeof preAnnotatedRoster === 'undefined'
      ? await annotateApprovedRosterWithManifestKeys(projectName, bootstrapAssets, scanFacts).catch(async (err) => {
          console.error('[COMPILER] annotateApprovedRosterWithManifestKeys failed:', err?.message || err);
          return null;
        })
      : preAnnotatedRoster;

    if (typeof preAnnotatedRoster === 'undefined') {
      const approvedRosterExists = await checkApprovedRosterExists(projectName);
      if (approvedRosterExists && annotatedRoster === null && !degradedOutputAccepted) {
        const error = new Error(
          'Approved roster annotation failed. Asset texture and geometry contracts could not be resolved. '
          + 'Geometry and texture contracts will show NOT AVAILABLE in tranche prompts, and colormap keys will be absent from the compiled package.'
        );
        error.name = 'ROSTER_ANNOTATION_FAILED';
        error.code = 'ROSTER_ANNOTATION_FAILED';
        throw error;
      }
    }

    const sceneIntent = await readSceneIntentJsonOrDefault(projectName);
    const existingCompiledPackage = await readExistingCompiledJsonPackage(projectName);
    const compiled = buildCanonicalJsonPackage({
      bootstrapAssets,
      annotatedRoster,
      sceneIntent,
      scanFacts,
      existingAssets: existingCompiledPackage.assets,
      existingTree: existingCompiledPackage.tree,
      existingEntities: existingCompiledPackage.entities
    });

    await writeCompiledJsonPackageAtomically(projectName, compiled);
    await refreshJsonUi();

    return {
      bootstrapAssets,
      scanFacts,
      annotatedRoster,
      sceneIntent,
      existingCompiledPackage,
      compiled,
      degradedOutputAccepted: Boolean(degradedOutputAccepted)
    };
  }

  return {
    DEFAULT_GROUP_MAT: DEFAULT_GROUP_MAT.slice(),
    createDefaultSceneIntent,
    validateSceneIntent,
    buildCanonicalAssetsJson,
    buildCanonicalTreeJson,
    buildCanonicalEntitiesJson,
    buildCanonicalJsonPackage,
    writeCompiledJsonPackageAtomically,
    compileProjectJsonPackage
  };
}
