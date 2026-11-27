/*  netlify/functions/_charmNestMaster.js
 *  The master index write, shared by charmNestLibrary (browser-side indexing of a small master) and
 *  charmMaster-background (server-side indexing of a large one), so both paths apply the same rules:
 *    · Charm_Master_Index is keyed by SKU alone — a SKU is one design whatever colour it is ordered in;
 *    · a SKU present in two different master files is a duplicate: BOTH entries are blocked until fixed — unless the write says
 *      `newest` (MASTERNEW, Paul 18 Oct 2026: "Any existing/duplicated SKUs must be removed in favour of the new file"): then the
 *      file being indexed wins, the entry is re-indexed from it, and what it replaced is recorded on the entry (`replaced`);
 *    · re-indexing the same master (same hash, or a file that `replaces` an older hash) simply updates;
 *    · a size that moved more than 5 % on re-index is flagged, never silently accepted;
 *    · operator overrides (engravable, upAngle, backKeepOut, confirmed vision labels) survive a re-index.        */
"use strict";
const { str, num } = require("./_charmNestAuth");

const INDEX = "Charm_Master_Index", FILES = "Charm_Master_Files";
const isLabelSku = s => /^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$/.test(String(s || ""));   // free text, as the shop's SKUs are ("T-REX_84495", "HUGGIE HOOPS- UMBRELLA"): a label read from a master file
// KEEPOWNSKU (Paul, 20 Oct 2026): a design saved from an order under the order's OWN Etsy SKU keeps it as Etsy carries it ("TEST_@"): an index key is any upper-case name of 2 to 61 characters that
// starts with a letter or digit and holds no "/" (a Firestore key cannot) and no control character (charm-nest-sku.js etsyKey makes it). A master file's labels stay the stricter isLabelSku.
const isSku = s => isLabelSku(s) || /^[A-Z0-9][^\u0000-\u001f\u007f\/a-z]{1,60}$/.test(String(s || ""));

/** The optional `pair` of a design that draws more than one separate body (charm-nest-pair.js, PAIRMASTER; contract.md):
    { v: 1, bodies: 2 to 8, mismatched: boolean }. undefined = the entry says nothing (what is stored stays as it is, which is
    what every indexer written before pairs sends); null = "not a pair design" (the stored one is removed); an object that is
    not that shape is ignored, never stored and never a reason to remove what is there. */
function cleanPair(p) {
  if (p === undefined) return undefined;
  if (p === null) return null;
  if (typeof p !== "object" || Array.isArray(p)) return undefined;
  const bodies = Math.floor(+p.bodies);
  if (!(bodies >= 2 && bodies <= 8)) return undefined;
  return { v: 1, bodies, mismatched: p.mismatched === true };
}

/** SIZEFIX (Paul, 14 Nov 2026: "correct the charm repository ... don't change the already ones"): `sizeFix` { from: [hashes], at } on an entry re-indexed at the size the master draws it: the drawings it replaced. A sorter does not make again
    the pieces already cut from those; only pieces made up from now on take the new drawing (charm-nest-bridge.js rdFixed). null = none. */
function cleanSizeFix(v) {
  if (!v || typeof v !== "object" || !Array.isArray(v.from)) return null;
  const from = v.from.map(h => String(h || "").slice(0, 80)).filter(Boolean).slice(0, 20);
  return from.length ? { from, at: num(v.at) || null } : null;
}

/** The optional `facing` of a design (Paul, amendment 2): the way the master drawing faces, "L" or "R" (or "X": the old "reads one way" word, kept if a record holds it; it no longer stops a Right being turned over). Same shape as `pair`:
    undefined = says nothing (what is stored stays), null = take it away, anything else that is not "L" or "R" is ignored. */
function cleanFacing(f) {
  if (f === undefined) return undefined;
  if (f === null) return null;
  return f === "L" || f === "R" || f === "X" ? f : undefined;   // "X": the old "reads one way" word (letters, numbers); stored values are kept, but nothing reads them any more (Paul, 10 Oct 2026: every earring pair is mirrored)
}

/** The optional `facings` of a MISMATCHED design (a left body and a right body under one SKU): the way each body faces as a person said it, a flat list of "L" | "R" | null (null = not said),
    at most 8. undefined = says nothing, null (or a list with nothing in it) = take it away, anything that is not a list is ignored. A re-index never writes it. */
function cleanFacings(v) {
  if (v === undefined) return undefined;
  if (v === null) return null;
  if (!Array.isArray(v) || v.length > 8) return undefined;
  const out = v.map(x => (x === "L" || x === "R" ? x : null));
  return out.some(Boolean) ? out : null;
}

/** The optional `sym` of a design (PAIRMIRROR): how far the design is from its own mirror image, measured at indexing by charm-nest-pair.js symmetryOf:
    "symmetric" (the mirror image looks the same: nothing to decide), "slight", or "directional" (it differs: a person says which way it faces). Anything else is ignored. */
function cleanSym(v) { return v === "symmetric" || v === "slight" || v === "directional" ? v : undefined; }

/** [w, h] in points, both positive and finite (the charm's size without its hoop), else null. */
function cleanBody(v) { return Array.isArray(v) && v.length === 2 && v.every(x => typeof x === "number" && isFinite(x) && x > 0 && x < 5000) ? [+v[0].toFixed(3), +v[1].toFixed(3)] : null; }
/** REPOEDIT: the mark the Charm Editor's "Update repository charm" leaves on an entry: { at, by, from (the hash it drew), to (the hash it draws), size, n (how many times) }. */
function cleanEdited(v) {
  if (!v || typeof v !== "object" || !v.to) return null;
  return { at: num(v.at) || null, by: String(v.by || "").slice(0, 80), from: String(v.from || "").slice(0, 80), to: String(v.to || "").slice(0, 80), size: v.size ? String(v.size).slice(0, 12) : null, n: num(v.n) || 1, ...(Array.isArray(v.was) && v.was.length ? { was: v.was.slice(-20).map(h => String(h).slice(0, 80)) } : {}) };
}
function slimEntry(d) {
  if (!d) return null;
  const pair = cleanPair(d.pair), facing = cleanFacing(d.facing), facings = cleanFacings(d.facings), sym = cleanSym(d.sym);
  // (KEEPOVERRIDE, Paul 20 Oct 2026: a SKU the designs window's switch took from another charm says so, so every computer remakes the pieces cut from the old drawing: charm-nest-bridge.js redrawChanged)
  const ov = d.override && typeof d.override === "object" ? { audit: String(d.override.audit || ""), via: String(d.override.via || ""), by: String(d.override.by || ""), at: num(d.override.at) || null, from: d.override.from && typeof d.override.from === "object" ? { skus: Array.isArray(d.override.from.skus) ? d.override.from.skus.slice(0, 20).map(String) : [], charmHash: String(d.override.from.charmHash || ""), aiPath: String(d.override.from.aiPath || ""), retired: !!d.override.from.retired } : null } : null;
  const body = cleanBody(d.bodyPt), drawn = cleanBody(d.drawnPt), sizeFix = cleanSizeFix(d.sizeFix), edited = cleanEdited(d.edited);
  return { ...(edited ? { edited } : {}), ...(ov ? { override: ov } : {}), ...(sizeFix ? { sizeFix } : {}), ...(body ? { bodyPt: body } : {}), ...(drawn ? { drawnPt: drawn } : {}), ...(pair ? { pair } : {}), ...(facing ? { facing } : {}), ...(Array.isArray(facings) ? { facings } : {}), ...(sym ? { sym } : {}), sku: d.sku, masterHash: d.masterHash || null, masterPath: d.masterPath || null, masterName: d.masterName || null, charmHash: d.charmHash || null, widthPt: num(d.widthPt), heightPt: num(d.heightPt), areaPt2: num(d.areaPt2), members: num(d.members), holes: num(d.holes), readerV: num(d.readerV),
    engravable: d.engravable !== false, engravableBy: d.engravableBy || null, upAngle: d.upAngle == null ? null : num(d.upAngle), upSource: d.upSource || null, backKeepOut: d.backKeepOut || [], aiPath: d.aiPath || null, thumbPath: d.thumbPath || null, aiUrl: d.aiUrl || null, thumbUrl: d.thumbUrl || null,
    sizes: d.sizes || null, blocked: d.blocked || null, conflict: d.conflict || null, sizeMoved: d.sizeMoved || null, labelSource: d.labelSource || "text", confirmedBy: d.confirmedBy || null, open: !!d.open, indexedAt: d.indexedAt && d.indexedAt.toMillis ? d.indexedAt.toMillis() : (num(d.indexedAtMs) || null), hashSource: d.hashSource || "browser" };
}

/** A short name for the shape an entry is sent in (the code that makes it). It rides in the index's signature (charmNestLibrary masterIndexSig),
    so a page that kept a copy of the whole index (charm-nest-master-cache.js) reads it again when this code changed, not only when the data did. */
const SHAPE = require("crypto").createHash("sha1").update([slimEntry, cleanPair, cleanFacing, cleanSym, num, cleanEdited].map(f => String(f)).join("\n")).digest("hex").slice(0, 10);

/**
 * entries: [{ sku, size?, masterHash, masterPath, masterName, charmHash, widthPt, heightPt, areaPt2, members, holes, engravable, upAngle, upSource, aiPath, thumbPath, aiUrl, thumbUrl, open, labelSource, confidence, blocked? }]
 * opts: { replaces: [masterHash…] } — hashes of earlier versions of this same master file.
 */
async function putIndex(db, FV, body) {
  // a name the library cannot hold is refused and SAID (`rejected`), never dropped without a word: the run that sent it shows it, and the read-back finds nothing for it
  const sent = (body.entries || []), entries = sent.filter(e => e && isLabelSku(String(e.sku || "").toUpperCase()));
  const masterHash = str(body.masterHash, 80), replaces = new Set((body.replaces || []).map(String).concat([masterHash]));
  // MASTERNEW (Paul, 18 Oct 2026): `newest` = the file being indexed wins over every other master file for the SKUs it carries; `by` = the signed-in person who added it (recorded with what was replaced)
  const newest = body.newest === true, by = str(body.by, 80) || null, movedOut = new Map();   // (movedOut: old master hash → the SKUs this write took from it, for its upload-history record)
  const out = { written: 0, blocked: [], sizeMoved: [], replaced: [], pairKept: [], rejected: sent.filter(e => !(e && isLabelSku(String(e.sku || "").toUpperCase()))).slice(0, 200).map(e => ({ sku: String((e && e.sku) || "").slice(0, 80), reason: "not a name the library can hold (2 to 61 characters: letters, digits, space and _ . , ' & ( ) + -)" })) };
  // group sized designs under one SKU
  const bySku = new Map();
  for (const e of entries) { const sku = String(e.sku).toUpperCase(); if (!bySku.has(sku)) bySku.set(sku, []); bySku.get(sku).push(e); }
  // One read for every SKU, then one batched write: a document at a time was 300 sequential round trips for a batch of
  // 150 and ran into the function's time limit on a real master, losing records with no error to show for it.
  const skus = [...bySku.keys()], existing = new Map();
  for (let i = 0; i < skus.length; i += 200) {
    const part = skus.slice(i, i + 200);
    // (only what the rules below look at of an entry already there: its master, its size, its operator overrides)
    const snaps = await db.getAll(...part.map(s => db.collection(INDEX).doc(s)), { fieldMask: ["masterHash", "masterName", "widthPt", "heightPt", "engravableBy", "upSource", "sizes", "pair", "pairBy", "facingBy", "hashSource", "charmHash", "edited"] });
    snaps.forEach((sn, j) => { if (sn.exists) existing.set(part[j], sn.data()); });
  }
  // KEEP R4: a design SAVED FROM AN ORDER (hashSource "custom-save", written create-only by the Review card's save) is the page's own and is never overwritten by a master file it did not come from: the master's
  // charm of the same name is reported (blocked) and left out, whatever the master's drawing is. The SKUs that were absent when the entries were read are read once more just before the write, so a save that
  // landed in between is kept too (the window was the whole batch; it is now one read and one commit).
  const savedDesign = (e, hash) => !!e && e.hashSource === "custom-save" && !!e.masterHash && e.masterHash !== hash && !replaces.has(e.masterHash);
  const keepSaved = (sku, e) => out.blocked.push({ sku, reason: "this name is a design saved from an order: it is kept as it is (rename the charm in the master file to index it)", with: e.masterHash, saved: true });
  // REPOEDIT (Paul, 17 Nov 2026): a charm a person updated from the Charm Editor (`edited`) keeps that drawing when a master file is indexed again; only a file added as the NEWEST, and not the entry's own file, takes it
  // (a person's newer drawing). It is said (`blocked`, edited: true), never dropped without a word.
  const editedKept = e => !!e && !!cleanEdited(e.edited) && body.overEdited !== true && !(newest && !!e.masterHash && e.masterHash !== masterHash);
  const keepEdited = (sku, e) => out.blocked.push({ sku, reason: "this charm was updated in the Charm Editor" + (e.edited && e.edited.by ? " by " + String(e.edited.by).slice(0, 60) : "") + ": that drawing is kept (add the file as the newest to replace it)", with: e.masterHash || null, edited: true });
  let queue = [];
  const flush = async () => {
    if (!queue.length) return;
    const fresh = queue.filter(q => !q.ex);
    if (fresh.length) {
      const again = await db.getAll(...fresh.map(q => q.ref), { fieldMask: ["masterHash", "hashSource", "charmHash"] });
      again.forEach((sn, i) => { if (sn.exists && savedDesign(sn.data(), masterHash)) { fresh[i].skip = true; keepSaved(fresh[i].sku, sn.data()); out.written--; } });
    }
    const batch = db.batch(); let n = 0;
    for (const q of queue) if (!q.skip) { batch.set(q.ref, q.doc, { merge: true }); n++; }
    queue = [];
    if (n) await batch.commit();
  };
  for (const [sku, list] of bySku) {
    const ref = db.collection(INDEX).doc(sku); const ex = existing.get(sku) || null;
    if (savedDesign(ex, masterHash)) { keepSaved(sku, ex); continue; }
    if (editedKept(ex)) { keepEdited(sku, ex); continue; }
    // A design the library holds as a PAIR is never written back as one body. An entry that says nothing of `pair` (an indexer that read only the labelled body of a row of two) would put one body's
    // file and size over a record that still says two bodies; it is left as it was and the answer names it (`pairKept`). `pair: null` (a person's word, or a deliberate re-index) still takes it away.
    if (ex && (cleanPair(ex.pair) || Object.values(ex.sizes || {}).some(z => z && cleanPair(z.pair))) && !list.some(e => cleanPair(e.pair) !== undefined)) { out.pairKept.push({ sku, reason: "the library holds " + sku + " as a pair and this entry draws one body: left as it was (set pair to null first to make it one charm)" }); continue; }
    // a stable base whatever order the entries arrived in: the unsized entry, else the smallest size
    const LADDER = ["XS", "S", "M", "L", "XL"];
    const base = list.find(e => !e.size) || list.slice().sort((a, b) => {
      const ia = LADDER.indexOf(String(a.size).toUpperCase()), ib = LADDER.indexOf(String(b.size).toUpperCase());
      return (ia < 0 ? 9 : ia) - (ib < 0 ? 9 : ib) || String(a.size).localeCompare(String(b.size));
    })[0];
    const doc = { sku, masterHash, masterPath: str(body.masterPath, 600), masterName: str(body.masterName, 200), indexedAt: FV.serverTimestamp(), indexedAtMs: Date.now(), hashSource: str(body.hashSource || "browser", 20) };
    // (`pair` goes with the geometry of the design, and of each size: only when the entry sent one, or null to take it away)
    const pairOf = e => { const x = cleanPair(e.pair); return x === undefined ? {} : { pair: x === null ? FV.delete() : x }; };
    const facingOf = e => { const x = cleanFacing(e.facing); return x === undefined ? {} : { facing: x === null ? FV.delete() : x }; };
    const facingsOf = e => { const x = cleanFacings(e.facings); return x === undefined ? {} : { facings: x === null ? FV.delete() : x }; };   // (what the indexer read from a folded pair: the second body drawn as the mirror image of the first; a person's words stand over it)
    // (LETTERS-HOOPS, Paul 9 Nov 2026: "update all the sizings to only include the actual charm and not the hoop": `bodyPt` [w, h] is the charm's cut size WITHOUT its hoop, sent only by an indexer that knows it; the Library card shows it)
    const bodyOf = e => { const b = cleanBody(e.bodyPt); return b ? { bodyPt: b } : {}; };
    const drawnOf = e => { const b = cleanBody(e.drawnPt); return b ? { drawnPt: b } : {}; };   // (TRUESIZE, Paul 22 Nov 2026: the charm as drawn, the size every person reads; widthPt / heightPt stay the reader's frame)
    const symOf = e => { const x = cleanSym(e.sym); return x === undefined ? {} : { sym: x }; };   // (measured, not a person's word: a re-index writes it again)
    const geom = e => Object.assign({ charmHash: str(e.charmHash, 80), widthPt: num(e.widthPt), heightPt: num(e.heightPt), areaPt2: num(e.areaPt2), members: num(e.members), holes: num(e.holes), aiPath: str(e.aiPath, 600), thumbPath: str(e.thumbPath, 600), aiUrl: str(e.aiUrl, 900), thumbUrl: str(e.thumbUrl, 900), open: !!e.open, labelSource: str(e.labelSource || "text", 20), confidence: e.confidence == null ? null : num(e.confidence), readerV: num(e.readerV) }, pairOf(e), facingOf(e), facingsOf(e), symOf(e), bodyOf(e), drawnOf(e));
    Object.assign(doc, geom(base));
    { const sf = cleanSizeFix(base.sizeFix); if (sf) doc.sizeFix = sf; }   // (SIZEFIX)
    const sized = list.filter(e => e.size);
    if (sized.length) { doc.sizes = {}; for (const e of sized) doc.sizes[String(e.size).toUpperCase()] = geom(e); if (!list.find(e => !e.size)) { doc.charmHash = null; doc.aiPath = ""; doc.thumbPath = ""; doc.aiUrl = ""; doc.thumbUrl = ""; } }
    else if (ex && ex.sizes) doc.sizes = FV.delete();
    // a person's decision about a doubtful design (masterPatch `pair`, pairBy "operator") stands over what a re-index reads from the drawing
    if (ex && ex.pairBy === "operator") { delete doc.pair; for (const z of Object.values(doc.sizes || {})) delete z.pair; }
    // so does a person's `facing` (masterPatch, facingBy "operator"): a re-index never overwrites the way a person said the drawing faces
    if (ex && ex.facingBy === "operator") { delete doc.facing; delete doc.facings; for (const z of Object.values(doc.sizes || {})) { delete z.facing; delete z.facings; } }
    // derived defaults, kept when an operator already overrode them
    if (!(ex && ex.engravableBy === "operator")) { doc.engravable = base.engravable !== false; doc.engravableBy = "index"; }
    if (!(ex && ex.upSource === "operator")) { doc.upAngle = base.upAngle == null ? null : num(base.upAngle); doc.upSource = str(base.upSource || "index", 20); }
    if (base.backKeepOut) doc.backKeepOut = base.backKeepOut;
    // duplicates across masters (with `newest`, there are none: the newest file's drawing is the one entry)
    let blocked = base.blocked ? str(base.blocked, 300) : null;
    if (ex && ex.masterHash && !replaces.has(ex.masterHash) && ex.masterHash !== masterHash && !newest) {
      blocked = `SKU present in two master files: ${ex.masterName || ex.masterHash} and ${doc.masterName || masterHash}`;
      doc.conflict = { with: ex.masterHash, withName: ex.masterName || null, at: Date.now() };
      out.blocked.push({ sku, reason: blocked, with: ex.masterHash });
    } else doc.conflict = FV.delete();
    // size moved more than 5 % on re-index — only against a different master file; re-writing the same one is not a change
    if (ex && ex.masterHash && ex.masterHash !== masterHash && num(ex.widthPt) && num(doc.widthPt)) {
      const dw = Math.abs(doc.widthPt - ex.widthPt) / ex.widthPt, dh = Math.abs(doc.heightPt - ex.heightPt) / (ex.heightPt || 1);
      if (dw > 0.05 || dh > 0.05) { doc.sizeMoved = { from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt], at: Date.now(), by }; out.sizeMoved.push({ sku, from: [ex.widthPt, ex.heightPt], to: [doc.widthPt, doc.heightPt] }); }
      else doc.sizeMoved = FV.delete();
    } else doc.sizeMoved = FV.delete();
    doc.blocked = blocked || FV.delete();
    // MASTERNEW: an entry another master file wrote is re-indexed from this one (newest, or a file that replaces it): what it was, from which file, and who replaced it, stay on the entry.
    // The write is a merge, so what a person set that the drawing does not carry (their angle, engravable, pair and facing words, confirmations, keep-outs) is kept, as for any re-index.
    if (ex && ex.masterHash && ex.masterHash !== masterHash && (newest || replaces.has(ex.masterHash))) {
      const rec = { from: ex.masterHash, fromName: ex.masterName || null, fromSize: [num(ex.widthPt), num(ex.heightPt)], toSize: [doc.widthPt, doc.heightPt], at: Date.now(), by };
      doc.replaced = rec; out.replaced.push(Object.assign({ sku }, rec));
      if (!movedOut.has(ex.masterHash)) movedOut.set(ex.masterHash, []); movedOut.get(ex.masterHash).push(sku);
    }
    if (ex && ex.edited) doc.edited = FV.delete();   // (REPOEDIT: a newer master file's drawing took the entry: the editor's mark goes)
    if (!ex) doc.firstIndexedAt = FV.serverTimestamp();
    queue.push({ ref, doc, sku, ex }); out.written++;
    if (queue.length >= 400) await flush();
  }
  await flush();
  // the older file's upload-history record stops listing the SKUs it no longer holds (its other SKUs, and its own entries, are as they were): one read and one merge per older file
  for (const [old, list] of movedOut) {
    try {
      const ref = db.collection(FILES).doc(old), sn = await ref.get(); if (!sn.exists) continue;
      const f = sn.data() || {}, gone = new Set(list);
      if (Array.isArray(f.skus)) await ref.set({ skus: f.skus.filter(s => !gone.has(String(s).toUpperCase())), movedOut: { to: masterHash, toName: str(body.masterName, 200) || null, skus: list.slice(0, 500), count: list.length, at: Date.now(), by } }, { merge: true });
    } catch (_) { /* the entries are written; the old record is display only */ }
  }
  return out;
}

/**
 * REPOEDIT (Paul, 17 Nov 2026: "inside the Charm Editor we need to give users an option to update the Charm repository for a given charm ... the SKU of that charm wherever it came from ... has to be
 * matched to the SKU for that charm so then when it gets updated, it's always the correct one that gets updated"). ONE entry's drawing is replaced by the drawing the Charm Editor holds, in one transaction.
 *   body: { sku, size?, expect: { charmHash }, entry: { charmHash, widthPt, heightPt, areaPt2, members, holes, aiPath, thumbPath, upAngle?, upSource?, readerV? }, by, keepPlaced? }
 * NEVER creates an entry and never renames one: the SKU must be in the index (a size: that size must be), must draw ONE body (a pair is two), and must STILL draw what the editor opened (expect.charmHash is
 * the entry's, or the size's, hash now), else `changed` and nothing is written. The files are the page's uploads under charmnest/master/edited/ and must be in Storage.
 * What changes: the geometry and the files of that entry (or of that one size: the other sizes and every other SKU are untouched). What stays: the SKU, its master file's hash, name and path (the Library lists
 * it under its file as before), hashSource, and everything a person set (angle, engravable, facing, keep-outs, blocks). `edited` stays on the entry (putIndex keeps an edited entry when its master file is
 * indexed again), and Charm_Master_Edits keeps what it drew before (its hash, size and files: nothing is deleted from Storage).
 * keepPlaced: the pieces already made up keep the drawing they were cut from (sizeFix.from lists the old hashes: the page's redrawNoted); otherwise every computer re-makes the open pieces (redrawChanged).
 * deps: { db, FV, bucket?, tokenUrl? } → { ok: true, sku, size, entry (slim), was, same? } | { ok: false, reason, text, ... }
 */
const EDIT_DIR = /^charmnest\/master\/edited\/[A-Za-z0-9][A-Za-z0-9 _.,'&()+\-]{1,90}\.(ai|png)$/, EDITS = "Charm_Master_Edits";
async function editorPut(deps, body) {
  const { db, FV } = deps, b = body || {}, no = (reason, text, more) => Object.assign({ ok: false, reason, text }, more || {});
  if (b.sandbox === true || b.sandbox === 1 || b.sandbox === "1" || b.sandbox === "true") return no("sandbox", "this is the sandbox: the repository is not written");
  const sku = String(b.sku || "").trim().toUpperCase(), size = String(b.size || "").trim().toUpperCase(), e = b.entry && typeof b.entry === "object" ? b.entry : {}, by = str(b.by, 80).trim();
  if (!isSku(sku)) return no("bad-sku", "that SKU cannot be read");
  if (!by) return no("nobody", "nobody is signed in");
  const hash = str(e.charmHash, 80), expect = str(b.expect && b.expect.charmHash, 80), aiPath = str(e.aiPath, 600), thumbPath = str(e.thumbPath, 600);
  if (!expect) return no("no-expect", "the page did not say which drawing it opened");
  if (!hash || !(num(e.widthPt) > 0) || !(num(e.heightPt) > 0) || !(num(e.members) > 0)) return no("bad-entry", "the new drawing was not sent whole");
  if (!EDIT_DIR.test(aiPath) || !/\.ai$/.test(aiPath) || (thumbPath && (!EDIT_DIR.test(thumbPath) || !/\.png$/.test(thumbPath)))) return no("bad-file", "the new drawing's file is not where an update keeps it");
  if (deps.bucket) {
    try { const [there] = await deps.bucket.file(aiPath).exists(); if (!there) return no("not-uploaded", "the new drawing's file did not reach the repository"); if (thumbPath) { const [t] = await deps.bucket.file(thumbPath).exists(); if (!t) return no("not-uploaded", "the new drawing's picture did not reach the repository"); } }
    catch (er) { return no("not-uploaded", "the new drawing's file could not be checked: " + String((er && er.message) || er).slice(0, 120)); }
  }
  let aiUrl = "", thumbUrl = "";
  if (typeof deps.tokenUrl === "function") { try { [aiUrl, thumbUrl] = await Promise.all([deps.tokenUrl(aiPath), thumbPath ? deps.tokenUrl(thumbPath) : ""]); } catch (_) { /* withLinks makes them at the next read */ } }
  const ref = db.collection(INDEX).doc(sku), now = Date.now();
  /* SAMEFILE (Paul 27 Nov 2026: "When Saving or Updating the repository inside the charm editor, those changes do not always propagate throughout the system"): several SKUs can be ONE drawing (the Library's card is one per
     file); an update of one of them left the others drawing the old file, so their orders, sheets and pictures never changed. The SKUs the page names as the same drawing (siblings: the same file, the same charm, one
     size, no pair) are updated with it in the same transaction; one that no longer draws that file is left as it is, and said. */
  const sibs = size ? [] : [...new Set((Array.isArray(b.siblings) ? b.siblings : []).map(x => String(x || "").trim().toUpperCase()).filter(x => isSku(x) && x !== sku))].slice(0, 24), sibRefs = sibs.map(x => db.collection(INDEX).doc(x));
  const out = await db.runTransaction(async tx => {
    const sn = await tx.get(ref); if (!sn.exists) return no("missing", sku + " is not in the charm repository");
    const sibSnaps = sibRefs.length ? await Promise.all(sibRefs.map(r => tx.get(r))) : [];
    const d = sn.data() || {}, sizes = d.sizes && typeof d.sizes === "object" ? d.sizes : null;
    if (size && !(sizes && sizes[size])) return no("missing-size", sku + " has no size " + size + " in the charm repository");
    if (!size && sizes && Object.keys(sizes).length && !d.charmHash) return no("needs-size", sku + " is drawn in sizes: the size was not said");
    const slot = size ? sizes[size] : d;
    if (cleanPair(d.pair) || cleanPair(slot.pair)) return no("pair", sku + " is a pair (two bodies in one drawing): add it as a master file");
    const was = str(slot.charmHash, 80);
    if (was !== expect) return no("changed", sku + " was changed in the repository after it was opened here: open it again", { charmHash: was, edited: cleanEdited(d.edited) });
    if (was === hash) return { ok: true, same: true, sku, size: size || null, was };
    const geom = { charmHash: hash, widthPt: num(e.widthPt), heightPt: num(e.heightPt), areaPt2: num(e.areaPt2), members: num(e.members), holes: num(e.holes), aiPath, thumbPath, aiUrl: str(aiUrl, 900), thumbUrl: str(thumbUrl, 900), open: false, bodyPt: FV.delete(), drawnPt: cleanBody(e.drawnPt) || FV.delete() };   // (TRUESIZE: the new drawing's size as drawn, or none: never the old drawing's)
    if (num(e.readerV)) geom.readerV = num(e.readerV);
    const geom0 = Object.assign({}, geom);   // (SAMEFILE: the drawing alone, before the patch below (which is this same object when no size is named) takes this SKU's own angle and marks)
    const oldHashes = [d.charmHash].concat(Object.values(sizes || {}).map(z => z && z.charmHash)).filter(Boolean).map(String);
    // (EVERYOLD, 27 Nov 2026: every drawing an update ever replaced is kept (was, the last twenty), so a piece still cut from the FIRST drawing after a second update is found and made again, not only one cut from the last)
    const wasOf = ed => [...new Set((ed && Array.isArray(ed.was) ? ed.was : []).concat(ed && ed.from ? [ed.from] : []).map(String))];
    const edited = { at: now, by, from: was, to: hash, size: size || null, n: (num(d.edited && d.edited.n) || 0) + 1, was: [...new Set(wasOf(d.edited).concat([was]))].filter(h => h && h !== hash).slice(-20) };
    const patch = Object.assign(size ? { sizes: { [size]: geom } } : geom, { edited, indexedAt: FV.serverTimestamp(), indexedAtMs: now, sizeMoved: FV.delete(), conflict: FV.delete() });
    if (!size && d.upSource !== "operator" && e.upAngle !== undefined) { patch.upAngle = e.upAngle == null ? null : num(e.upAngle); patch.upSource = str(e.upSource || "index", 20); }
    if (b.keepPlaced === true) { const had = cleanSizeFix(d.sizeFix); patch.sizeFix = { from: [...new Set((had ? had.from : []).concat(oldHashes))].slice(-20), at: now }; }
    else if (d.sizeFix) patch.sizeFix = FV.delete();
    tx.set(ref, patch, { merge: true });
    tx.set(db.collection(EDITS).doc(), { sku, size: size || null, by, at: now, atTs: FV.serverTimestamp(), from: was, to: hash, fromAi: str(slot.aiPath, 600) || null, fromThumb: str(slot.thumbPath, 600) || null, toAi: aiPath, toThumb: thumbPath || null,
      fromSize: [num(slot.widthPt), num(slot.heightPt)], toSize: [geom.widthPt, geom.heightPt], masterHash: d.masterHash || null, masterName: d.masterName || null, keepPlaced: b.keepPlaced === true, piece: str(b.piece, 80) || null, order: str(b.order, 40) || null });
    // (SAMEFILE: each sibling that still draws the very file and charm this one drew takes the new drawing too, its own angle kept when a person set it)
    const withIt = [], leftOut = [];
    sibSnaps.forEach((s2, i) => {
      const x = sibs[i], d2 = s2.exists ? s2.data() || {} : null, z2 = d2 && d2.sizes && typeof d2.sizes === "object" ? Object.keys(d2.sizes).length : 0;
      if (!d2 || z2 || cleanPair(d2.pair) || str(d2.aiPath, 600) !== str(slot.aiPath, 600) || str(d2.charmHash, 80) !== was) { leftOut.push(x); return; }
      const p2 = Object.assign({}, geom0, { edited: Object.assign({}, edited, { n: (num(d2.edited && d2.edited.n) || 0) + 1, with: sku, was: [...new Set(wasOf(d2.edited).concat([was]))].filter(h => h && h !== hash).slice(-20) }), indexedAt: FV.serverTimestamp(), indexedAtMs: now, sizeMoved: FV.delete(), conflict: FV.delete() });
      if (d2.upSource !== "operator" && e.upAngle !== undefined) { p2.upAngle = e.upAngle == null ? null : num(e.upAngle); p2.upSource = str(e.upSource || "index", 20); }
      if (b.keepPlaced === true) { const had = cleanSizeFix(d2.sizeFix); p2.sizeFix = { from: [...new Set((had ? had.from : []).concat([was]))].slice(-20), at: now }; } else if (d2.sizeFix) p2.sizeFix = FV.delete();
      tx.set(sibRefs[i], p2, { merge: true });
      tx.set(db.collection(EDITS).doc(), { sku: x, size: null, by, at: now, atTs: FV.serverTimestamp(), from: was, to: hash, fromAi: str(d2.aiPath, 600) || null, fromThumb: str(d2.thumbPath, 600) || null, toAi: aiPath, toThumb: thumbPath || null,
        fromSize: [num(d2.widthPt), num(d2.heightPt)], toSize: [geom.widthPt, geom.heightPt], masterHash: d2.masterHash || null, masterName: d2.masterName || null, keepPlaced: b.keepPlaced === true, with: sku, piece: str(b.piece, 80) || null, order: str(b.order, 40) || null });
      withIt.push(x);
    });
    return { ok: true, sku, size: size || null, was, siblings: withIt, ...(leftOut.length ? { siblingsLeft: leftOut } : {}) };
  });
  if (out && out.ok && !out.same) {
    try { const sn = await ref.get(); out.entry = slimEntry(sn.data()); } catch (_) { /* the page reads it */ }
    if ((out.siblings || []).length) { try { out.siblingEntries = (await db.getAll(...out.siblings.map(x => db.collection(INDEX).doc(x)))).filter(s2 => s2.exists).map(s2 => slimEntry(s2.data())); } catch (_) { /* the page reads them */ } }
  }
  return out;
}

async function putFile(db, FV, body) {
  const f = body.file || {}; const hash = str(f.masterHash, 80); if (!/^[0-9a-f]{8,64}$/i.test(hash)) return { error: "bad master hash" };
  const doc = { masterHash: hash, path: str(f.path, 600), url: str(f.url, 900), name: str(f.name, 200), charms: num(f.charms), labelled: num(f.labelled), held: num(f.held), unlabelled: (f.unlabelled || []).slice(0, 500), orphans: (f.orphans || []).slice(0, 500), duplicates: (f.duplicates || []).slice(0, 500), undecodable: num(f.undecodable), blocked: (f.blocked || []).slice(0, 500),
    // the lists are capped so one document cannot grow without bound; the counts are what the sheet really held
    unlabelledCount: Array.isArray(f.unlabelled) ? f.unlabelled.length : num(f.unlabelledCount), orphanCount: Array.isArray(f.orphans) ? f.orphans.length : num(f.orphanCount),
    duplicateCount: Array.isArray(f.duplicates) ? f.duplicates.length : num(f.duplicateCount), blockedCount: Array.isArray(f.blocked) ? f.blocked.length : num(f.blockedCount), skus: (f.skus || []).map(s => String(s).toUpperCase()).slice(0, 2000), indexedAt: FV.serverTimestamp(), indexedAtMs: Date.now(), indexedBy: str(f.indexedBy || "browser", 20), pageW: num(f.pageW), pageH: num(f.pageH) };
  if (Array.isArray(f.visionReads)) doc.visionReads = f.visionReads.slice(0, 1000);
  if (f.replaces) doc.replaces = (f.replaces || []).map(String).slice(0, 20);
  if (f.by) doc.by = str(f.by, 80);   // (MASTERNEW: the signed-in person who added the file)
  await db.collection(FILES).doc(hash).set(doc, { merge: true });
  return { ok: true, masterHash: hash };
}

module.exports = { INDEX, FILES, SHAPE, isSku, slimEntry, cleanSizeFix, cleanBody, cleanPair, cleanFacing, cleanFacings, cleanSym, cleanEdited, putIndex, putFile, editorPut };
