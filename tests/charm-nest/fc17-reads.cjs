// FC17 (Sorter back end, Firebase cost, 7 Oct 2026): the reads the Sorter's own calls cost, measured on a metered in-memory
// Firestore (fc17-meter.cjs) with the real charmNestLibrary ops.
//   1. The learned maps (aliasGet, optionMapGet, noDesignGet) answer `unchanged` to the signature they gave, for a handful of
//      reads, and answer whole again after any write of a map (an alias answer, an option map, a no-design row or its removal).
//   2. masterListFiles answers `unchanged` to the signature of the file records, and whole after a file is indexed or removed.
//   3. putSheet looks at each approved back it must check once, together, with three fields; backs its record already holds
//      are not read; a back invalidated since is still dropped from the record.
//   4. putCharms reads the charms' name rank together with one field.
//   5. A signature that cannot be taken (no aggregation) leaves every answer whole, as before.
//   node tests/charm-nest/fc17-reads.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const fnDir = path.join(__dirname, "../../netlify/functions");
const m = require("./fc17-meter.cjs");
m.install(fnDir);
const ops = require(path.join(fnDir, "charmNestLibrary.js")).ops;
const TS = m.FieldValue.serverTimestamp();

(async () => {
  /* ── 1 · learned maps ── */
  for (let i = 0; i < 1200; i++) m.put("Charm_Sku_Aliases", String(4000000000 + i), { listingId: String(4000000000 + i), by: "operator", title: "t" + i, sku: "SKU-" + i, v: 2, updatedAt: TS });
  for (let i = 0; i < 300; i++) m.put("Charm_Option_Map", String(5000000000 + i), { listingId: String(5000000000 + i), map: { size: { small: { field: "size", value: "S" } } }, updatedAt: TS });
  for (let i = 0; i < 40; i++) m.put("Charm_Sku_NoDesign", "nd" + i, { sku: "ND-" + i, by: "operator", createdAt: TS });
  const sig = {};
  for (const [k, op, field] of [["al", "aliasGet", "aliases"], ["om", "optionMapGet", "maps"], ["nd", "noDesignGet", "list"]]) {
    m.reset(); const first = await ops[op]({}); assert(first.sig && first[field], op + ": a first read is whole and carries its signature"); sig[k] = first.sig;
    assert(m.stats().reads >= 40, op + ": a first read reads the map");
  }
  m.reset();
  const same = [await ops.aliasGet({ ifSig: sig.al }), await ops.optionMapGet({ ifSig: sig.om }), await ops.noDesignGet({ ifSig: sig.nd })];
  assert(same.every(r => r.unchanged === true && !r.aliases && !r.maps && !r.list), "an unchanged map answers unchanged with nothing in it");
  const idle = m.stats(); assert(idle.reads <= 10 && idle.bytes < 2048, `three unchanged maps cost ${idle.reads} reads and ${idle.bytes} bytes (were ${1200 + 300 + 40})`);
  await ops.aliasPut({ listingId: "4000000005", sku: "NEW-1", by: "Paul", title: "x" });
  const a2 = await ops.aliasGet({ ifSig: sig.al }); assert(!a2.unchanged && a2.aliases["4000000005"].sku === "NEW-1" && a2.sig !== sig.al, "an alias answer changes the signature"); sig.al = a2.sig;
  await ops.optionMapPut({ listingId: "5000000001", optionName: "Size", optionValue: "Huge", map: { field: "size", value: "XL" }, by: "Paul" });
  const o2 = await ops.optionMapGet({ ifSig: sig.om }); assert(!o2.unchanged && o2.sig !== sig.om, "an option map answer changes the signature"); sig.om = o2.sig;
  const put = await ops.noDesignPut({ sku: "ND-NEW", by: "Paul" });
  const n2 = await ops.noDesignGet({ ifSig: sig.nd }); assert(!n2.unchanged && n2.list.skus.includes("ND-NEW") && n2.sig !== sig.nd, "a no-design row changes the signature"); sig.nd = n2.sig;
  await ops.noDesignDelete({ id: put.id });
  const n3 = await ops.noDesignGet({ ifSig: sig.nd }); assert(!n3.unchanged && !n3.list.skus.includes("ND-NEW"), "removing a no-design row changes the signature");
  assert((await ops.aliasGet({ ifSig: a2.sig })).unchanged, "and the new signature holds until the next write");

  /* ── 2 · master file records ── */
  for (let f = 0; f < 6; f++) m.put("Charm_Master_Files", "ab" + f + "cdef0123", { masterHash: "ab" + f + "cdef0123", name: "M" + f, skus: Array.from({ length: 300 }, (_, i) => "SKU-" + i), blocked: [], indexedAt: TS });
  const f1 = await ops.masterListFiles({}); assert(f1.files.length === 6 && f1.filesSig && f1.index, "a first read of the files is whole");
  m.reset(); const f2 = await ops.masterListFiles({ ifFilesSig: f1.filesSig }), fileReads = m.stats().reads;
  assert(f2.unchanged === true && !f2.files && f2.index && fileReads <= 6, `unchanged file records are not read again (${fileReads} reads)`);
  await ops.masterPutFile({ file: { masterHash: "ab9cdef0123", name: "New", charms: 1, skus: ["A-1"] } });
  const f3 = await ops.masterListFiles({ ifFilesSig: f1.filesSig }); assert(!f3.unchanged && f3.files.length === 7, "a file indexed changes the signature");
  await ops.masterRemoveFile({ masterHash: "ab9cdef0123" });
  const f4 = await ops.masterListFiles({ ifFilesSig: f3.filesSig }); assert(!f4.unchanged && f4.files.length === 6, "a file removed changes the signature");

  /* ── 3 · putSheet and its backs ── */
  const sheetId = "GF_Oct-06-26_Set-1_Sheet-1", N = 30, B = 12;
  const poolIds = Array.from({ length: N }, (_, i) => `35210000${String(i).padStart(2, "0")}_9900${String(i).padStart(3, "0")}_1`);
  const back = (i, at) => ({ poolId: poolIds[i], sheetId, order: poolIds[i].split("_")[0], text: "Love " + i, lines: ["Love"], approvedAt: at, approvedBy: "Paul", invalidated: false, verified: { geometry: { ok: true, detail: "x".repeat(500) }, file: { ok: true } }, outputs: { ai: { path: "a", url: "u" }, png: { path: "p", url: "u" } }, fit: { path: "p".repeat(1500) } });
  const backs = Array.from({ length: B }, (_, i) => back(i, 1780000000000 + i));
  for (const b of backs) m.put("Charm_Pool_Back", b.poolId, b);
  for (const id of poolIds) m.put("Charm_Pool", id, { poolId: id, orderId: id.split("_")[0], state: "written", sheetId, setId: "set-1", sku: "S", spec: { t: "t".repeat(500) }, updatedAt: TS });
  const sheet = { id: sheetId, metal: "gold", day: "2026-10-06", setId: "set-1", poolIds, orders: [...new Set(poolIds.map(p => p.split("_")[0]))], backPool: backs.map(b => ({ ...b })), charms: poolIds.map(id => ({ id, poolId: id })), placements: poolIds.map(id => ({ id, x: 1, y: 2 })) };
  assert((await ops.putSheet({ sheet })).ok, "a sheet is saved");
  m.reset(); assert((await ops.putSheet({ sheet: { ...sheet, saving: true } })).ok, "and saved again");
  const re = m.stats(); assert(re.reads === 1, `a re-save of a sheet whose ${B} backs its record holds reads the record only (${re.reads} reads; were ${1 + B})`);
  // a back invalidated meanwhile: the record no longer holds it, and a stale page that still lists it does not put it back
  await ops.backInvalidate({ poolIds: [backs[0].poolId] });
  m.reset(); await ops.putSheet({ sheet: { ...sheet, saving: false } });
  const stale = m.stats(); assert(stale.reads <= 3, `a stale page's re-save checks the one back it holds that the record does not (${stale.reads} reads)`);
  assert(!m.store.get("Charm_Nest_Sheets/" + sheetId).backPool.some(x => x.poolId === backs[0].poolId), "the invalidated back is not put back on the record");
  assert(m.store.get("Charm_Nest_Sheets/" + sheetId).backPool.length === B - 1, "the others stay");
  // a newer approval from the page is looked at once and kept
  const newer = back(1, 1780000009999), s2 = { ...sheet, backPool: [newer] };
  m.reset(); await ops.putSheet({ sheet: s2 }); const nw = m.stats();
  assert(nw.reads <= 3, `a newer approval is checked once (${nw.reads} reads)`);

  /* ── 4 · putCharms ── */
  for (let i = 0; i < 50; i++) m.put("Charm_Nest_Library", "a".repeat(8) + String(i).padStart(8, "0"), { hash: "a".repeat(8) + String(i).padStart(8, "0"), name: "C" + i, namedBy: "operator", thumbUrl: "u".repeat(400), timesUsed: 1 });
  const charms = Array.from({ length: 50 }, (_, i) => ({ hash: "a".repeat(8) + String(i).padStart(8, "0"), name: "Model name " + i, namedBy: "claude" }));
  m.reset(); await ops.putCharms({ charms }); const pc = m.stats();
  assert(pc.reads === 50 && pc.bytes < 50 * 120, `putCharms reads each charm once, with one field (${pc.reads} reads, ${pc.bytes} bytes)`);
  assert(m.store.get("Charm_Nest_Library/" + "a".repeat(8) + "00000000").name === "C0", "an operator's name is never overwritten by a model's");

  console.log(`fc17 reads OK · three unchanged maps ${idle.reads} reads (was ${1200 + 300 + 40}) · unchanged files ${fileReads} reads (were 6 files, 7 reads) · sheet re-save ${re.reads} read (was ${1 + B}) · putCharms ${pc.bytes} bytes`);
})().catch(e => { console.error(e); process.exit(1); });
