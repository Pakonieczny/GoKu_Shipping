"use strict";

// Redo of a Review slot must only ever be composed with the set's OWN charm.
// Bug (Beady_Necklace Set_513, 2026-09-29): the set was still being finished by
// a stall-restart batch and had no manifest.json yet. Redo could not find a
// charm record, fell back to "the latest file in New_Charms" and composed two
// slots with two unrelated charms (a disc and a basket) next to the set's own.
// Checked here, with no network and no image calls:
//   page   - resolveOriginalCharm never falls back to another charm: no
//            manifest, an unreadable manifest, a split vote or a charm that is
//            in no folder all stop with a message; a recorded charm that moved
//            between pools is still found; a polluted per-slot record cannot
//            outvote the set's sourceCharm or the majority.
//   server - an edits request into a saved set whose manifest names a different
//            charm is refused before any image is read or generated (so a tab
//            opened before the page guard shipped is covered too).
// Usage: node tests/listing-batch/redo-own-charm.cjs [background.js] [page.html]

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

const ROOT = "listing-generator-1";
const POOL = `${ROOT}/Charm_Maker`;
const SET = `${ROOT}/Beady_Necklace/Ready_To_List/Set_513`;
const BIRD = "1788990000000_11_Bird_Necklaces.png";
const DISC = "1788991111111_22_Round_Disc_Necklaces.png";
const BASKET = "1788997696920_629_Wicker_Basket_Necklaces.png";
const inNew = (n) => `${POOL}/New_Charms/${n}`;
const inUsed = (n) => `${POOL}/Used_Necklace_Charm_Pool/${n}`;

// ---- page: resolveOriginalCharm -------------------------------------------
async function pageResolver() {
  const src = cut(page, "async function resolveOriginalCharm(setPath, slotIndex, fallbackFolderPath) {", "// Function to retrieve top N latest files from storage");
  const world = ({ manifest, files = [], manifestError }) => {
    const stored = new Set(files);
    const calls = { latest: 0 };
    const ctx = vm.createContext({
      ROOT,
      Error, JSON, Map, Set, Array, String, RegExp, TextDecoder,
      ensureStorageSignedIn: async () => {},
      storage: {},
      ref: (_s, p) => ({ fullPath: p, name: p.split("/").pop() }),
      getBytes: async (r) => {
        if (!r.fullPath.endsWith("/manifest.json")) throw new Error("unexpected read " + r.fullPath);
        if (manifestError) throw manifestError;
        if (!manifest) { const e = new Error("Firebase Storage: Object does not exist."); e.code = "storage/object-not-found"; throw e; }
        return new TextEncoder().encode(JSON.stringify(manifest));
      },
      getMetadata: async (r) => { if (!stored.has(r.fullPath)) throw new Error("object-not-found"); return {}; },
      getDownloadURL: async (r) => "https://fake/" + r.fullPath,
      listAll: async (r) => {
        const dir = r.fullPath.replace(/\/$/, "") + "/";
        const items = [...stored].filter((p) => p.startsWith(dir) && !p.slice(dir.length).includes("/"))
          .map((p) => ({ name: p.split("/").pop(), fullPath: p }));
        return { items, prefixes: [] };
      },
      newCharmsPath: () => `${POOL}/New_Charms`,
      newCharmsEarringsPath: () => `${POOL}/New_Charms_Earrings`,
      charmMakerSourcePath: () => `${POOL}/New_Charms`,
      // the removed fallback: any use of it fails the test
      listLatestFile: async () => { calls.latest++; return { fullPath: inNew(BASKET), name: BASKET, url: "x" }; },
      console: { log() {}, warn() {} },
    });
    vm.runInContext(`${src}; this.resolve = resolveOriginalCharm;`, ctx);
    return { resolve: (slot = 0) => ctx.resolve(SET, slot, `${POOL}/New_Charms`), calls };
  };
  const slots = (names) => names.map((n, i) => ({ slot: i + 1, type: n ? "gen" : "copy", newCharm: n ? inNew(n) : null }));
  // the newest charm in New_Charms is a different one: the old fallback would have taken it
  const decoys = [inNew(BASKET), inNew(DISC)];

  // 1. the recorded charm is where it was
  let w = world({ manifest: { sourceCharm: inNew(BIRD), sourceCharmName: BIRD, slots: slots([BIRD, BIRD]) }, files: [inNew(BIRD), ...decoys] });
  assert.equal((await w.resolve()).fullPath, inNew(BIRD));

  // 2. batch_collect moved it to the Used pool: found by file name, not the newest file
  w = world({ manifest: { sourceCharm: inNew(BIRD), sourceCharmName: BIRD, slots: slots([BIRD, BIRD]) }, files: [inUsed(BIRD), ...decoys] });
  assert.equal((await w.resolve()).fullPath, inUsed(BIRD));

  // 3. NO manifest yet (the Set_513 case): Redo stops, the newest charm is never used
  w = world({ manifest: null, files: [inNew(BIRD), ...decoys] });
  await assert.rejects(() => w.resolve(0), /no record of its charm yet/);
  assert.equal(w.calls.latest, 0, "no latest-file fallback");

  // 4. manifest could not be read (network): stops, says so
  w = world({ manifestError: new Error("network down"), files: decoys });
  await assert.rejects(() => w.resolve(0), /could not be read just now/);

  // 5. the charm is in no folder: stops instead of taking a different charm
  w = world({ manifest: { sourceCharm: inNew(BIRD), sourceCharmName: BIRD, slots: slots([BIRD]) }, files: decoys });
  await assert.rejects(() => w.resolve(0), new RegExp(`own charm \\(${BIRD.replace(/\./g, "\\.")}\\) could not be found`));
  assert.equal(w.calls.latest, 0);

  // 6. manifest with no charm at all (no sourceCharm, no slot charms): stops
  w = world({ manifest: { slots: slots([null, null]) }, files: decoys });
  await assert.rejects(() => w.resolve(0), /no record of its charm/);

  // 7. earlier wrong Redos left a wrong charm in slots 1 and 3, and no sourceCharm was
  //    recorded: the charm most slots share (the set's own) wins for EVERY slot
  const polluted = { slots: slots([DISC, BIRD, BASKET, BIRD, BIRD, null, BIRD, null]) };
  w = world({ manifest: polluted, files: [inNew(BIRD), inNew(DISC), inNew(BASKET)] });
  for (const slot of [0, 1, 2, 3, 4, 6]) assert.equal((await w.resolve(slot)).name, BIRD, `slot ${slot + 1} uses the majority charm`);

  // 8. a split vote is refused, never guessed
  w = world({ manifest: { slots: slots([BIRD, DISC, BIRD, DISC]) }, files: [inNew(BIRD), inNew(DISC)] });
  await assert.rejects(() => w.resolve(0), /different charms/);

  // 9. sourceCharm outranks every slot record, including this slot's own
  w = world({ manifest: { sourceCharm: inNew(BIRD), sourceCharmName: BIRD, slots: slots([DISC, DISC, DISC]) }, files: [inUsed(BIRD), inNew(DISC)] });
  assert.equal((await w.resolve(0)).name, BIRD);

  // 10. sourceCharmName alone (path missing) still identifies the charm
  w = world({ manifest: { sourceCharmName: BIRD, slots: slots([DISC]) }, files: [inUsed(BIRD), inNew(DISC)] });
  assert.equal((await w.resolve(0)).fullPath, inUsed(BIRD));

  // 11. no code path reaches the latest-file fallback any more
  const resolver = cut(page, "async function resolveOriginalCharm(setPath, slotIndex, fallbackFolderPath) {", "// Function to retrieve top N latest files from storage");
  assert(!/listLatestFile\s*\(/.test(resolver), "resolveOriginalCharm no longer calls listLatestFile");
}

// ---- server: assertCharmBelongsToSet ---------------------------------------
async function serverGuard() {
  const src = cut(server, "// A charm is identified by its file name.", "function safeErr(err) {");
  const run = (manifestText) => {
    const reads = [];
    const bucket = { file: (name) => ({
      download: async () => {
        reads.push(name);
        if (manifestText == null) throw new Error("No such object");
        return [Buffer.from(manifestText)];
      },
    }) };
    const ctx = vm.createContext({ getBucket: () => bucket, Buffer, console: { warn() {} }, Map, Array, String, JSON, Error });
    vm.runInContext(`${src}; this.check = assertCharmBelongsToSet; this.nameOf = charmNameOfSetManifest;`, ctx);
    return { check: (cat, base, p) => ctx.check(cat, base, p), nameOf: (m) => ctx.nameOf(m), reads };
  };
  const man = (o) => JSON.stringify(o);
  const named = man({ sourceCharm: inNew(BIRD), sourceCharmName: BIRD, slots: [{ slot: 1, newCharm: inNew(BIRD) }] });
  const cat = "Beady_Necklace";

  let g = run(named);
  // the set's own charm, however the page names it
  for (const p of [
    inNew(BIRD), inUsed(BIRD),
    `${ROOT}/_gravity_cache/8123_${BIRD}`,                 // rotated copy
    `${ROOT}/_gravity_cache/_hooped/8123_${BIRD}/Slot_1.png`, // hoop-repaired copy
  ]) await g.check(cat, SET, p);
  // another charm: refused, with the names in the message
  for (const other of [DISC, BASKET]) {
    await assert.rejects(() => g.check(cat, SET, inNew(other)), (e) => e.message.includes(BIRD) && e.message.includes(other) && /no image was made/.test(e.message));
    await assert.rejects(() => g.check(cat, SET, `${ROOT}/_gravity_cache/8123_${other}`), /different charm/);
  }
  // the check reads only that set's manifest
  assert(g.reads.every((r) => r === `${SET}/manifest.json`));

  // a name with characters the page replaces in cache file names still matches
  const odd = "1788_5_Bird & Nest (1).png";
  g = run(man({ sourceCharmName: odd, slots: [] }));
  await g.check(cat, SET, `${ROOT}/_gravity_cache/8123_1788_5_Bird___Nest__1_.png`);
  await assert.rejects(() => g.check(cat, SET, inNew(BIRD)), /different charm/);

  // no evidence, no refusal
  await run(null).check(cat, SET, inNew(DISC));                                   // no manifest yet
  await run("not json{").check(cat, SET, inNew(DISC));                             // unreadable
  await run(man({ slots: [{ slot: 1, newCharm: null }] })).check(cat, SET, inNew(DISC)); // no charm recorded
  await run(man({ slots: [{ newCharm: inNew(BIRD) }, { newCharm: inNew(DISC) }] })).check(cat, SET, inNew(BASKET)); // split vote

  // majority of slot records when there is no sourceCharm
  g = run(man({ slots: [{ newCharm: inNew(DISC) }, { newCharm: inNew(BIRD) }, { newCharm: inNew(BIRD) }, { newCharm: inNew(BASKET) }] }));
  assert.equal(g.nameOf({ slots: [{ newCharm: inNew(BIRD) }, { newCharm: inNew(BIRD) }, { newCharm: inNew(DISC) }] }), BIRD);
  await g.check(cat, SET, inUsed(BIRD));
  await assert.rejects(() => g.check(cat, SET, inNew(DISC)), /different charm/);

  // out of scope: no charm sent, Charm Maker categories and paths that are not a listing set
  g = run(named);
  await g.check(cat, SET, "");
  await g.check("Charms", `${ROOT}/Charms/Ready_To_List/Set_4`, inNew(DISC));
  await g.check(cat, `${ROOT}/Charm_Maker/Generated_Charm_Sets/Deriv_3`, inNew(DISC));
  assert.equal(g.reads.length, 0, "out-of-scope requests never read a manifest");

  // wiring: both edits paths ask before any image is read or paid for
  const guardCalls = [...server.matchAll(/await assertCharmBelongsToSet\(/g)].length;
  assert.equal(guardCalls, 2, "the direct and the job-based edits handlers both check");
  const direct = cut(server, "if (kind === \"edits\") {\n      const cat = normalizeCategory(activeCategory);", "if (kind === \"write_manifest\") {");
  assert(direct.indexOf("assertCharmBelongsToSet") > 0 && direct.indexOf("assertCharmBelongsToSet") < direct.indexOf("storagePathToBuffer(basePath0)"), "direct edits: checked before the first image read");
  const job = cut(server, "// kind === \"edits\"\n      if (!input_image && !input_storage_path) {", "const images = [{ buffer: ref.buffer");
  assert(job.indexOf("assertCharmBelongsToSet") > 0 && job.indexOf("assertCharmBelongsToSet") < job.indexOf("storagePathToBuffer(input_storage_path)"), "job edits: checked before the first image read");
}

(async () => {
  await pageResolver();
  console.log("page: Redo only ever resolves the set's own charm; no manifest, split vote or missing file stops it; nothing falls back to the newest charm");
  await serverGuard();
  console.log("server: an edits request carrying another charm than the set's manifest names is refused before any image is read");
})().catch((e) => { console.error(e); process.exit(1); });
