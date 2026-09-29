"use strict";

// Review-tab Redo failed on every slot with
//   "input_storage_path not found: listing-generator-1/Charm_Maker/New_Charms/<charm>.png"
// The collector moves a used charm from New_Charms to the Used pool right after
// a batch is collected. The page remembers each charm's reference answer by the
// charm's NAME, and the remembered answer carried the path of whoever asked
// first (the batch submit, under New_Charms), so a later Redo in the same tab
// asked the server for a file that had already moved. Two fixes are checked:
//   1. the page hands every caller its OWN resolved path for the original file
//      (derived copies stay shared), and a Redo re-checks the path it sends;
//   2. the server finds the same charm file in the other pool when the saved
//      path names a folder the charm has left.
// No network: storage, the image decoder and the bucket are stubs.
// Usage: node tests/listing-batch/redo-charm-path.cjs [background.js] [page.html]

const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");

const server = fs.readFileSync(process.argv[2] || "netlify/functions/geminiImageProxy-background.js", "utf8");
const page = fs.readFileSync(process.argv[3] || "Listing_Generator_1.html", "utf8");
const cut = (src, from, to) => {
  const start = src.indexOf(from);
  const end = src.indexOf(to, start);
  assert(start > 0 && end > start, `found ${from.trim().split("\n")[0]}`);
  return src.slice(start, end);
};

const NEW = "listing-generator-1/Charm_Maker/New_Charms/1788997466383_542_Sun_Cloud_Necklaces.png";
const USED = "listing-generator-1/Charm_Maker/Used_Necklace_Charm_Pool/1788997466383_542_Sun_Cloud_Necklaces.png";
const NEW_EAR = "listing-generator-1/Charm_Maker/New_Charms_Earrings/1788000000000_7_Star_Studs.png";
const USED_EAR = "listing-generator-1/Charm_Maker/Used_Earring_Charm_Pool/1788000000000_7_Star_Studs.png";

// ---- server: storagePathToBuffer ------------------------------------------
async function serverReads() {
  const src = cut(server, "// A charm is identified by its file name.", "function safeErr(err) {");
  const run = (files) => {
    const looked = [];
    const bucket = { file: (name) => ({
      exists: async () => { looked.push(name); return [files.has(name)]; },
      getMetadata: async () => [{ contentType: "image/png" }],
      download: async () => [Buffer.from(name)],
    }) };
    const warnings = [];
    const ctx = vm.createContext({ getBucket: () => bucket, Buffer, console: { warn: (m) => warnings.push(m) } });
    vm.runInContext(`${src}; this.read = storagePathToBuffer;`, ctx);
    return { read: (p) => ctx.read(p), looked, warnings };
  };
  const text = async (w, p) => (await w.read(p)).buffer.toString();

  // the path is right: nothing else is looked at
  let w = run(new Set([NEW]));
  assert.equal(await text(w, NEW), NEW);
  assert.deepEqual(w.looked, [NEW]);

  // batch_collect moved it: New_Charms path, file now in the Used pool
  w = run(new Set([USED]));
  assert.equal(await text(w, NEW), USED, "reads the charm from the Used pool");
  assert.equal(w.warnings.length, 1);

  // charm_restore moved it back: Used path, file now in New_Charms
  w = run(new Set([NEW]));
  assert.equal(await text(w, USED), NEW, "reads the charm from the active pool");

  // earrings behave the same way
  w = run(new Set([USED_EAR]));
  assert.equal(await text(w, NEW_EAR), USED_EAR);

  // gone everywhere: the error still names the path that was asked for
  w = run(new Set());
  await assert.rejects(() => w.read(NEW), { message: `input_storage_path not found: ${NEW}` });

  // not a charm-pool file: never searched for elsewhere
  const ref = "listing-generator-1/Beady_Necklace/Primary_Models/ref.png";
  w = run(new Set([USED]));
  await assert.rejects(() => w.read(ref), { message: `input_storage_path not found: ${ref}` });
  assert.deepEqual(w.looked, [ref]);

  // a file inside a sub-folder of a pool is not "the charm's file name"
  const deep = "listing-generator-1/Charm_Maker/New_Charms/sub/x.png";
  w = run(new Set(["listing-generator-1/Charm_Maker/Used_Necklace_Charm_Pool/sub/x.png"]));
  await assert.rejects(() => w.read(deep), { message: `input_storage_path not found: ${deep}` });

  // the allow-list still applies
  await assert.rejects(() => run(new Set()).read("somewhere/else.png"), /not allowed/);
}

// ---- page: _resolveCharmReference and charmPathThatExists ----------------
async function pageMemo() {
  const src = cut(page, "async function _resolveCharmReference(charmPath, allowHoopRepair) {", "const GRAVITY_PROMPT_MARKER");
  const world = (opts = {}) => {
    const stored = new Set(opts.stored || []);
    const state = { hang: opts.hang ?? { angle: 0, pivot: "hoop", lever: 1, cx: 1, cy: 1 }, decodeFails: !!opts.decodeFails };
    const ctx = vm.createContext({
      _gravityMemo: new Map(),
      _gravityKey: (s) => String(s.length),
      GRAVITY_CACHE_DIR: "listing-generator-1/_gravity_cache",
      HOOP_REPAIR_DIR: "listing-generator-1/_gravity_cache/_hooped",
      GRAVITY_MIN_DEGREES: 3,
      storage: {},
      ref: (_s, p) => ({ fullPath: p }),
      getMetadata: async (r) => { if (!stored.has(r.fullPath)) throw new Error("object-not-found"); return {}; },
      uploadBytesResumable: async (r) => { stored.add(r.fullPath); },
      _loadCharmBitmap: async () => { if (state.decodeFails) throw new Error("decode"); return { close() {} }; },
      analyseCharmHang: () => state.hang,
      softenHangAngle: (deg) => deg,
      renderRotatedCharm: () => ({ toBlob: (cb) => cb({}) }),
      hoopIntegratedCharmPath: async () => null,
      Promise, Map, Math, console: { log() {}, warn() {} },
    });
    vm.runInContext(`${src}; this.resolve = _resolveCharmReference;`, ctx);
    const exists = cut(page, "async function charmPathThatExists(path, resolvedPath) {", "    /**\n     * Resolves the original charm");
    vm.runInContext(`${exists}; this.exists = charmPathThatExists;`, ctx);
    return { ...ctx, stored, state };
  };

  // 1. hangs true: every caller is answered with the path it asked about
  let w = world();
  const first = await w.resolve(NEW, true);   // the batch submit, charm under New_Charms
  assert.deepEqual({ ...first }, { base: NEW, rotated: NEW });
  const redo = await w.resolve(USED, true);   // a later Redo, charm now in the Used pool
  assert.deepEqual({ ...redo }, { base: USED, rotated: USED }, "Redo is not sent the stale New_Charms path");

  // 2. the charm needs turning: the cached rotated copy is shared, the
  //    original is still the caller's own path
  w = world({ hang: { angle: 20 * Math.PI / 180, pivot: "hoop", lever: 1, cx: 1, cy: 1 } });
  const turnedFirst = await w.resolve(NEW, false);
  assert.equal(turnedFirst.base, NEW);
  assert(turnedFirst.rotated.startsWith("listing-generator-1/_gravity_cache/"), "rotated copy is the cache file");
  const turnedRedo = await w.resolve(USED, false);
  assert.equal(turnedRedo.base, USED, "original follows the caller");
  assert.equal(turnedRedo.rotated, turnedFirst.rotated, "the cached rotated copy is reused");

  // 3. the decoder failed the first time: the answer falls back to the
  //    original, and a later caller still gets ITS path
  w = world({ decodeFails: true });
  assert.deepEqual({ ...(await w.resolve(NEW, true)) }, { base: NEW, rotated: NEW });
  assert.deepEqual({ ...(await w.resolve(USED, true)) }, { base: USED, rotated: USED });

  // 4. Redo re-checks what it is about to send
  w = world({ stored: [USED] });
  assert.equal(await w.exists(USED, USED), USED, "same file: no lookup needed");
  assert.equal(await w.exists("listing-generator-1/_gravity_cache/gone", USED), USED, "a vanished cache file falls back");
  w.stored.add("listing-generator-1/_gravity_cache/here");
  assert.equal(await w.exists("listing-generator-1/_gravity_cache/here", USED), "listing-generator-1/_gravity_cache/here");
}

(async () => {
  await serverReads();
  console.log("server: a charm that moved between pools is still read; other paths behave as before");
  await pageMemo();
  console.log("page: every caller gets its own path for the original charm; derived copies stay shared; Redo re-checks its path");
})().catch((e) => { console.error(e); process.exit(1); });
