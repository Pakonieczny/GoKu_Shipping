#!/usr/bin/env node
/*  scripts/catalog-repair.cjs — the safety net around a catalogue repair (backup · verify · restore).
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════
 *  The repair itself is scripts/index-master.cjs --only <SKUs> (see CATALOG-runbook.md). This script does the three things
 *  that go before and after it, and nothing else:
 *
 *    backup   READ-ONLY. Reads the whole SKU index (GET charmNestLibrary?op=masterList, 1-2 calls, one read per SKU) and the
 *             master file records (op=masterListFiles), writes them to DIR/index-all.json and DIR/files.json (Storage download
 *             tokens are replaced by REDACTED), and downloads the current per-SKU .ai / .png of every SKU being repaired
 *             (plain GETs of the stored links: no function call, no Firestore read) into DIR/files/<storage path>, with
 *             DIR/manifest.json (bytes + sha256 of each). Nothing on the site is changed.
 *    verify   READ-ONLY. Reads the index again and checks (1) every staged record (index-master --out-dir) is live exactly as
 *             staged (file path, charm hash, size, holes), (2) every staged .ai now served draws the same as the staged
 *             one, (3) no SKU that was NOT repaired changed against the backup. Exit 1 on any difference.
 *    diff     READ-ONLY. Takes the files a staged run (index-master --out-dir) would write and compares each, drawn part by drawn
 *             part, with the .ai the site serves for that SKU now. The SKUs whose live file already draws the same are left
 *             out: only the ones that differ are written (DIR/changed-skus.json, which index-master --only takes).
 *    junk     READ-ONLY. Lists the SKUs in a backed-up index that the reader no longer accepts as SKUs (a measurement such as "11.4 MM",
 *             a view word such as "FRONT", and with --numbers a bare 1-3 digit number): callouts the master's artists wrote beside a
 *             charm that an older reader took for its SKU. DIR/junk-skus.json.
 *    prune    WRITES, and only with --write (without it, it lists what it would do). Removes those junk records from the index
 *             (op masterRemoveSku, one call, one read and one delete each). The files stay in Storage; restore can bring any back.
 *    restore  WRITES. Puts the backed-up .ai / .png of the named SKUs back at their storage paths (same path, so every stored
 *             link keeps working) and writes their index records back as the backup holds them. --dry lists what it would do.
 *
 *    node scripts/catalog-repair.cjs backup  --origin https://goldenspike.app --out DIR [--skus FILE|A,B] [--no-files]
 *    node scripts/catalog-repair.cjs diff    --origin https://goldenspike.app --stage DIR [--skus FILE|A,B]
 *    node scripts/catalog-repair.cjs verify  --origin https://goldenspike.app --stage DIR --backup DIR [--changed FILE]
 *    node scripts/catalog-repair.cjs junk    --from DIR [--numbers] [--exclude FILE]
 *    node scripts/catalog-repair.cjs prune   --origin https://goldenspike.app --from DIR [--skus FILE|A,B] [--exclude FILE] [--numbers] [--write]
 *    node scripts/catalog-repair.cjs restore --origin https://goldenspike.app --from DIR [--skus FILE|A,B] [--dry]
 *
 *  The passcode (when the site has one) is read from the EDIT_PASSCODE environment variable and is never printed or saved.
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path"), crypto = require("crypto");
const { api, upload, onlySet } = require("./index-master.cjs");
const { filePrint, printDiff } = require("./audit-catalog.cjs");

const sha = b => crypto.createHash("sha256").update(b).digest("hex");
const redact = v => String(v).replace(/([?&]token=)[^&"\s]+/g, "$1REDACTED");
const json = (o) => JSON.stringify(o, (k, v) => (typeof v === "string" ? redact(v) : v), 1);
const up = s => String(s || "").toUpperCase();

function args(argv) {
  const o = { cmd: argv[2], origin: "", out: "", from: "", stage: "", backup: "", skus: "", dry: false, noFiles: false, passcode: process.env.EDIT_PASSCODE || "", concurrency: 4 };
  for (let i = 3; i < argv.length; i++) {
    const a = argv[i], v = argv[i + 1];
    if (a === "--origin") { o.origin = String(v || "").replace(/\/+$/, ""); i++; }
    else if (a === "--out") { o.out = v; i++; } else if (a === "--from") { o.from = v; i++; } else if (a === "--stage") { o.stage = v; i++; } else if (a === "--backup") { o.backup = v; i++; }
    else if (a === "--skus") { o.skus = v; i++; } else if (a === "--changed") { o.changed = v; i++; } else if (a === "--dry") o.dry = true; else if (a === "--no-files") o.noFiles = true; else if (a === "--numbers") o.numbers = true; else if (a === "--write") o.write = true; else if (a === "--exclude") { o.exclude = v; i++; }
    else if (a === "--concurrency") { o.concurrency = Math.max(1, Math.min(8, +v || 4)); i++; }
  }
  return o;
}

/** A read-only GET of one library function (the ops used here only read). */
async function get(o, fn, q) {
  const headers = {}; if (o.passcode) headers["X-Edit-Passcode"] = o.passcode;
  const res = await fetch(`${o.origin}/.netlify/functions/${fn}?${new URLSearchParams(q)}`, { headers });
  const txt = await res.text(); let data; try { data = JSON.parse(txt); } catch (_) { data = { error: txt.slice(0, 200) }; }
  if (!res.ok) throw new Error(`${fn} ${q.op}: HTTP ${res.status} ${data && data.error || ""}`);
  return data;
}
/** Every part of the index, as the sorter reads it (each answer says where the next starts). */
async function readIndex(o) {
  const entries = []; let index = null;
  for (let cursor = null, part = 0; ; part++) {
    if (part === 200) throw new Error("the index did not end after 200 parts");
    const r = await get(o, "charmNestLibrary", Object.assign({ op: "masterList", limit: "3000" }, cursor ? { cursor } : {}));
    if (r.index) index = r.index;
    for (const e of r.entries || []) if (e && e.sku) entries.push(e);
    if (!(cursor = r.next || null)) break;
  }
  return { entries, index };
}
/** The files an entry points at: its own, or one per size. [{ path, url, kind }] */
function filesOf(e) {
  const out = [], add = (g) => { if (g && g.aiPath) out.push({ path: g.aiPath, url: g.aiUrl || "", kind: "ai" }); if (g && g.thumbPath) out.push({ path: g.thumbPath, url: g.thumbUrl || "", kind: "png" }); };
  add(e); for (const s of Object.values(e.sizes || {})) add(s);
  return out;
}
/** The charms to repair, as whole designs: every SKU that shares a file with a named SKU is rewritten with it. */
function affected(entries, want) {
  const byFile = new Map(); for (const e of entries) for (const f of filesOf(e)) if (f.kind === "ai") { if (!byFile.has(f.path)) byFile.set(f.path, []); byFile.get(f.path).push(e); }
  const hit = new Set(); for (const e of entries) if (want.has(up(e.sku))) hit.add(e);
  for (const e of [...hit]) for (const f of filesOf(e)) if (f.kind === "ai") for (const x of byFile.get(f.path) || []) hit.add(x);
  return [...hit];
}
async function pool(n, items, fn) { const q = items.slice(); await Promise.all(Array.from({ length: Math.min(n, q.length) }, async () => { while (q.length) await fn(q.shift()); })); }
async function download(o, f) {
  // the stored link is a plain Storage GET (no Netlify call, no Firestore read). A record without one is reported, not worked around:
  // charmNestOutput op=url is a POST that can write a download token, and nothing here may write.
  const url = f.url; if (!url) throw new Error(`${f.path}: the index record holds no link for it`);
  const res = await fetch(url); if (!res.ok) throw new Error(`GET ${f.path}: HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

/* ── backup ── */
async function backup(o, log) {
  if (!o.origin || !o.out) throw new Error("backup needs --origin and --out");
  fs.mkdirSync(o.out, { recursive: true });
  const t0 = Date.now(), { entries, index } = await readIndex(o);
  const bySource = {}, byMaster = {}; for (const e of entries) { const k = e.labelSource || "text"; bySource[k] = (bySource[k] || 0) + 1; const m = (e.masterName || "?") + " " + String(e.masterHash || "").slice(0, 12); byMaster[m] = (byMaster[m] || 0) + 1; }
  log(`read the index: ${entries.length} SKU record(s)${index ? ` (index signature ${JSON.stringify(index)})` : ""} · by label source ${JSON.stringify(bySource)} · by master file ${JSON.stringify(byMaster)}`);
  const files = await get(o, "charmNestLibrary", { op: "masterListFiles" });
  fs.writeFileSync(path.join(o.out, "index-all.json"), json({ readAt: new Date().toISOString(), origin: new URL(o.origin).host, count: entries.length, index, entries }));
  fs.writeFileSync(path.join(o.out, "files.json"), json({ readAt: new Date().toISOString(), files: files.files || [], index: files.index || null }));
  const want = o.skus ? onlySet(o.skus) : null;
  const list = want ? affected(entries, want) : [];
  const missing = want ? [...want].filter(s => !entries.some(e => up(e.sku) === s)) : [];
  const manifest = { readAt: new Date().toISOString(), skus: list.map(e => e.sku).sort(), files: [] };
  if (want && !o.noFiles) {
    const seen = new Map(); for (const e of list) for (const f of filesOf(e)) if (!seen.has(f.path)) seen.set(f.path, f);
    let n = 0, bytes = 0; const failed = [];
    await pool(o.concurrency, [...seen.values()], async f => {
      try { const b = await download(o, f), dest = path.join(o.out, "files", f.path); fs.mkdirSync(path.dirname(dest), { recursive: true }); fs.writeFileSync(dest, b); manifest.files.push({ path: f.path, kind: f.kind, bytes: b.length, sha256: sha(b) }); bytes += b.length; if (++n % 50 === 0) log(`  ${n}/${seen.size} files`); }
      catch (e) { failed.push({ path: f.path, error: e.message }); }
    });
    manifest.files.sort((a, b) => a.path.localeCompare(b.path)); manifest.failed = failed;
    log(`downloaded ${n} file(s), ${(bytes / 1048576).toFixed(1)} MB${failed.length ? ` · ${failed.length} FAILED: ${failed.slice(0, 5).map(x => x.path).join(", ")}` : ""}`);
    if (failed.length) process.exitCode = 1;
  }
  fs.writeFileSync(path.join(o.out, "manifest.json"), json(manifest));
  fs.writeFileSync(path.join(o.out, "backup-info.json"), json({ at: new Date().toISOString(), origin: new URL(o.origin).host, indexRecords: entries.length, named: want ? want.size : 0, notInIndex: missing, skusBackedUp: list.length, designFiles: manifest.files.filter(f => f.kind === "ai").length, seconds: Math.round((Date.now() - t0) / 1000), reads: "masterList (1 Firestore read per SKU record) + masterListFiles (<= 200 file records)", note: "tokens are redacted; restore re-puts the files at the same storage paths" }));
  log(`backup written to ${o.out}${missing.length ? ` · ${missing.length} named SKU(s) are not in the index: ${missing.slice(0, 20).join(", ")}` : ""}`);
  return { entries: entries.length, skus: list.length, files: manifest.files.length, missing };
}

/* ── diff ── */
async function diff(o, log) {
  if (!o.origin || !o.stage) throw new Error("diff needs --origin and --stage (the --out-dir of an index-master run)");
  const staged = JSON.parse(fs.readFileSync(path.join(o.stage, "records.json"), "utf8")), { entries } = await readIndex(o);
  const live = new Map(entries.map(e => [up(e.sku), e])), want = o.skus ? onlySet(o.skus) : null;
  // one design file can carry several SKUs: compare it once, answer for all of them
  const byFile = new Map();
  for (const e of staged.entries) { if (want && !want.has(up(e.sku))) continue; if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, { e, skus: [] }); byFile.get(e.aiPath).skus.push(up(e.sku)); }
  const changed = [], same = [], gone = [], failed = [], rec = {}; let n = 0;
  await pool(o.concurrency, [...byFile.entries()], async ([aiPath, { e, skus }]) => {
    try {
      const l = live.get(up(e.sku)); const g = l && (e.size ? (l.sizes || {})[up(e.size)] : l);
      if (!l || !g || !g.aiPath) { gone.push({ skus, aiPath, why: "no live record or file for it" }); return; }
      const mine = await filePrint(fs.readFileSync(path.join(o.stage, "files", aiPath)));
      const theirs = await filePrint(await download(o, { path: g.aiPath, url: g.aiUrl }));
      const why = printDiff(theirs, mine);
      (why.length ? changed : same).push({ skus, aiPath, live: g.aiPath, why: why.join("; "), liveParts: theirs.n, stagedParts: mine.n });
      // what the staged record would change in the index besides the drawing: the numbers the nesting and the engraving read
      const rel = (a, b) => (a > 0 && b > 0 ? Math.abs(a - b) / a : 0), note = (k, v) => { (rec[k] = rec[k] || []).push({ skus, ...v }); };
      if (rel(g.widthPt, e.widthPt) > 0.02 || rel(g.heightPt, e.heightPt) > 0.02) note("sizeMoved", { from: [g.widthPt, g.heightPt].map(v => +(+v).toFixed(2)), to: [e.widthPt, e.heightPt].map(v => +(+v).toFixed(2)) });
      if ((g.holes || 0) !== (e.holes || 0)) note("holesChanged", { from: g.holes, to: e.holes });
      if (l.engravableBy !== "operator" && (l.engravable !== false) !== (e.engravable !== false)) note("engravableChanged", { from: l.engravable !== false, to: e.engravable !== false });
      if (l.upSource !== "operator" && l.upAngle != null && e.upAngle != null && Math.abs(((l.upAngle - e.upAngle + 540) % 360) - 180) > 1) note("upAngleChanged", { from: l.upAngle, to: e.upAngle });
      if (e.blocked && !g.blocked && !l.blocked) note("newlyBlocked", { why: e.blocked });
    } catch (err) { failed.push({ skus, aiPath, error: err.message }); }
    if (++n % 100 === 0) log(`  ${n}/${byFile.size} designs compared`);
  });
  // a design is rewritten when the file draws differently OR the index record would change (size, holes, up angle, engravable, blocked):
  // the reader groups a stored file again when it loads, so some fixes change the record and not what the file draws
  const skusChanged = [...new Set(changed.flatMap(x => x.skus).concat(Object.values(rec).flatMap(v => v.flatMap(x => x.skus))))].sort();
  // live records of this master that the staged run does not carry: SKUs read from the sheet by Claude's vision (not by text), SKUs
  // the reader no longer labels, SKUs gone from the master. A whole-master stage only (a restricted one says nothing about them).
  let liveOnly = null;
  if (!staged.only && !want) {
    const mine = new Set(staged.entries.map(e => up(e.sku)));
    liveOnly = entries.filter(e => !mine.has(up(e.sku)) && (up(e.masterHash) === up(staged.masterHash) || (staged.masterName && e.masterName === staged.masterName))).map(e => ({ sku: e.sku, labelSource: e.labelSource || null, masterHash: String(e.masterHash || "").slice(0, 12) }));
  }
  fs.writeFileSync(path.join(o.stage, "diff.json"), json({ at: new Date().toISOString(), designs: byFile.size, changed, same: same.length, gone, failed, recordChanges: rec, liveOnly }));
  fs.writeFileSync(path.join(o.stage, "changed-skus.json"), json(skusChanged));
  const reasons = {}; for (const c of changed) reasons[c.why.split(";")[0].replace(/\d+/g, "N")] = (reasons[c.why.split(";")[0].replace(/\d+/g, "N")] || 0) + 1;
  log(`diff: ${byFile.size} design file(s) compared with what the site serves · ${changed.length} draw differently, ${skusChanged.length} SKUs to rewrite (drawing or record) · ${same.length} already draw the same · ${gone.length} have no live file · ${failed.length} could not be compared`);
  for (const [k, v] of Object.entries(reasons).sort((a, b) => b[1] - a[1]).slice(0, 8)) log(`  ${v} × ${k}`);
  if (liveOnly) { const bySrc = {}; for (const x of liveOnly) bySrc[x.labelSource || "text"] = (bySrc[x.labelSource || "text"] || 0) + 1; log(`  ${liveOnly.length} live record(s) of this master are not in the staged run (by label source: ${JSON.stringify(bySrc)}): the vision-labelled ones cannot be rebuilt from this PC, see diff.json liveOnly`); }
  for (const [k, v] of Object.entries(rec)) log(`  record check: ${v.length} design(s) with ${k}${k === "sizeMoved" || k === "engravableChanged" || k === "upAngleChanged" || k === "newlyBlocked" ? " (look at them in diff.json before writing)" : ""}`);
  if (failed.length) { log(`  ! could not compare: ${failed.slice(0, 5).map(x => x.skus[0] + " (" + x.error + ")").join(", ")}`); process.exitCode = 1; }
  return { designs: byFile.size, changed: changed.length, skus: skusChanged.length, same: same.length, gone: gone.length, failed: failed.length };
}

/* ── verify ── */
const KEYS = ["masterHash", "charmHash", "widthPt", "heightPt", "areaPt2", "members", "holes", "engravable", "upAngle", "aiPath", "thumbPath", "blocked", "open"];
const same = (a, b) => (typeof a === "number" && typeof b === "number" ? Math.abs(a - b) < 1e-6 : JSON.stringify(a == null ? null : a) === JSON.stringify(b == null ? null : b));
function diffEntry(a, b, keys) {
  const out = [];
  for (const k of keys) if (!same(a[k], b[k])) out.push(`${k}: ${JSON.stringify(a[k])} -> ${JSON.stringify(b[k])}`);
  const sa = Object.keys(a.sizes || {}).sort().join(), sb = Object.keys(b.sizes || {}).sort().join(); if (sa !== sb) out.push(`sizes: ${sa} -> ${sb}`);
  else for (const s of Object.keys(a.sizes || {})) for (const k of ["charmHash", "aiPath", "widthPt", "heightPt"]) if (!same(a.sizes[s][k], b.sizes[s][k])) out.push(`sizes.${s}.${k}`);
  return out;
}
async function verify(o, log) {
  if (!o.origin || !o.stage) throw new Error("verify needs --origin and --stage (the --out-dir the repair was staged in)");
  const staged = JSON.parse(fs.readFileSync(path.join(o.stage, "records.json"), "utf8"));
  const { entries } = await readIndex(o), live = new Map(entries.map(e => [up(e.sku), e]));
  const bad = [], stagedSkus = new Set(), only = o.changed ? onlySet(o.changed) : null;   // --changed: the SKUs the repair really wrote (diff's changed-skus.json)
  // a staged record is the entry as sent to masterPutIndex; sized designs are several records under one SKU
  const bySku = new Map(); for (const e of staged.entries) { const k = up(e.sku); if (only && !only.has(k)) continue; stagedSkus.add(k); if (!bySku.has(k)) bySku.set(k, []); bySku.get(k).push(e); }
  const checked = new Set();
  // index-master --only keeps the hash a design already has while its size, area and holes are unchanged: the live hash may be the backup's
  const was = o.backup ? new Map(JSON.parse(fs.readFileSync(path.join(o.backup, "index-all.json"), "utf8")).entries.map(b => [up(b.sku), b])) : new Map();
  const heldHash = (sku, e, g) => { const b = was.get(sku), t = b && (e.size ? (b.sizes || {})[up(e.size)] : b); return !!(t && t.charmHash && t.charmHash === g.charmHash); };
  for (const [sku, list] of bySku) {
    const l = live.get(sku); if (!l) { bad.push({ sku, why: "not in the live index" }); continue; }
    for (const e of list) {
      const g = e.size ? (l.sizes || {})[up(e.size)] : l; if (!g) { bad.push({ sku, why: `size ${e.size} missing` }); continue; }
      const d = [];
      for (const k of ["charmHash", "aiPath", "widthPt", "heightPt", "members", "holes"]) if (!same(e[k], g[k]) && !(k === "charmHash" && heldHash(sku, e, g))) d.push(`${k}: staged ${JSON.stringify(e[k])} live ${JSON.stringify(g[k])}`);
      if (up(l.masterHash) !== up(staged.masterHash)) d.push(`masterHash: staged ${staged.masterHash} live ${l.masterHash}`);
      if (d.length) bad.push({ sku, why: d.join("; ") });
      const f = path.join(o.stage, "files", e.aiPath);
      if (!checked.has(e.aiPath) && fs.existsSync(f)) {
        checked.add(e.aiPath);
        try {
          // (not byte for byte: pdf-lib stamps each file with the second it was written, so two builds of one charm are never the same bytes;
          //  what must match is what the file draws, part by part)
          const theirs = await filePrint(await download(o, { path: e.aiPath, url: g.aiUrl || l.aiUrl })), mine = await filePrint(fs.readFileSync(f)), why = printDiff(theirs, mine);
          if (why.length) bad.push({ sku, why: `served ${e.aiPath} does not draw as the staged file does: ${why.join("; ")}` });
        } catch (err) { bad.push({ sku, why: `could not read ${e.aiPath}: ${err.message}` }); }
      }
    }
  }
  // nothing else may have moved
  let collateral = 0, compared = 0;
  if (o.backup) {
    const before = JSON.parse(fs.readFileSync(path.join(o.backup, "index-all.json"), "utf8"));
    const fileOf = e => filesOf(e).filter(f => f.kind === "ai").map(f => f.path);
    const sharing = new Set(); for (const e of before.entries) if (stagedSkus.has(up(e.sku))) for (const p of fileOf(e)) sharing.add(p);
    for (const b of before.entries) {
      const k = up(b.sku); if (stagedSkus.has(k) || fileOf(b).some(p => sharing.has(p))) continue;   // (the rewritten ones, and the SKUs that share their file)
      compared++; const l = live.get(k);
      if (!l) { bad.push({ sku: b.sku, why: "was in the backup, not live now (collateral)" }); collateral++; continue; }
      const d = diffEntry(b, l, KEYS); if (d.length) { bad.push({ sku: b.sku, why: "changed though it was not repaired: " + d.join("; ") }); collateral++; }
    }
    const extra = entries.filter(e => !before.entries.some(b => up(b.sku) === up(e.sku)) && !stagedSkus.has(up(e.sku))); for (const e of extra) bad.push({ sku: e.sku, why: "new in the live index though not staged" });
  }
  log(`verify: ${bySku.size} staged SKU(s) checked (${checked.size} served file(s) compared part by part with the staged ones)${o.backup ? ` · ${compared} other SKU(s) compared with the backup, ${collateral} changed` : ""} · ${bad.length} problem(s)`);
  for (const b of bad.slice(0, 60)) log(`  ! ${b.sku}: ${b.why}`);
  if (bad.length) process.exitCode = 1;
  return { checked: bySku.size, bad };
}

/* ── restore ── */
async function restore(o, log) {
  if (!o.origin || !o.from) throw new Error("restore needs --origin and --from (the backup folder)");
  const before = JSON.parse(fs.readFileSync(path.join(o.from, "index-all.json"), "utf8")), manifest = JSON.parse(fs.readFileSync(path.join(o.from, "manifest.json"), "utf8"));
  const want = o.skus ? onlySet(o.skus) : new Set(manifest.skus.map(up));
  const list = affected(before.entries, want);
  const paths = new Set(); for (const e of list) for (const f of filesOf(e)) paths.add(f.path);
  const have = new Map(manifest.files.map(f => [f.path, f]));
  const lost = [...paths].filter(p => !have.has(p) || !fs.existsSync(path.join(o.from, "files", p)));
  log(`restore: ${list.length} SKU record(s), ${paths.size} file(s) to put back${lost.length ? ` · ${lost.length} file(s) are not in the backup: ${lost.slice(0, 10).join(", ")}` : ""}${o.dry ? " · DRY RUN" : ""}`);
  if (lost.length) throw new Error("the backup does not hold every file: nothing was written");
  if (o.dry) return { skus: list.length, files: paths.size };
  const urls = new Map();
  await pool(o.concurrency, [...paths], async p => {
    const b = fs.readFileSync(path.join(o.from, "files", p)); if (sha(b) !== have.get(p).sha256) throw new Error(`${p}: the backup copy is damaged (sha256)`);
    const r = await upload(o.origin, o.passcode, p, b, /\.png$/i.test(p) ? "image/png" : "application/illustrator"); urls.set(p, r.url);
  });
  // the records, grouped by the master file each came from, in the shape masterPutIndex takes (a sized design is one record per size)
  const groups = new Map();
  for (const e of list) {
    const k = [e.masterHash || "", e.masterName || "", e.masterPath || "", e.hashSource || "local"].join("\u0001"); if (!groups.has(k)) groups.set(k, []);
    const mk = (g, size) => ({ sku: e.sku, size: size || undefined, charmHash: g.charmHash, widthPt: g.widthPt, heightPt: g.heightPt, areaPt2: g.areaPt2, members: e.members, holes: e.holes, engravable: e.engravable, upAngle: e.upAngle, upSource: e.upSource, backKeepOut: e.backKeepOut, aiPath: g.aiPath, aiUrl: urls.get(g.aiPath) || "", thumbPath: g.thumbPath, thumbUrl: urls.get(g.thumbPath) || "", open: e.open, labelSource: e.labelSource, blocked: e.blocked || null });
    if (e.sizes && Object.keys(e.sizes).length) for (const [s, g] of Object.entries(e.sizes)) groups.get(k).push(mk(g, s)); else groups.get(k).push(mk(e, null));
  }
  let wrote = 0;
  for (const [k, ents] of groups) {
    const [masterHash, masterName, masterPath, hashSource] = k.split("\u0001");
    for (let i = 0; i < ents.length; i += 150) { await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterPutIndex", entries: ents.slice(i, i + 150), masterHash, masterPath: masterPath || null, masterName, hashSource, replaces: [] }); wrote += Math.min(150, ents.length - i); }
  }
  // read back
  let bad = 0; const skus = [...new Set(list.map(e => up(e.sku)))], now = new Map();
  for (let i = 0; i < skus.length; i += 300) { const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterGetMany", skus: skus.slice(i, i + 300) }); for (const [k, e] of Object.entries(r.entries || {})) now.set(up(k), e); }
  for (const b of list) { const l = now.get(up(b.sku)); const d = l ? diffEntry(b, l, ["masterHash", "charmHash", "aiPath", "widthPt", "heightPt", "members", "holes"]) : ["missing"]; if (d.length) { bad++; log(`  ! ${b.sku}: ${d.join("; ")}`); } }
  log(`restored ${wrote} record(s), ${paths.size} file(s) · ${bad ? bad + " differ" : "all read back as in the backup"}`);
  if (bad) process.exitCode = 1;
  return { wrote, files: paths.size, bad };
}

/* ── junk · prune ── */
/** The SKUs of a backed-up index that the reader's own SKU rule refuses now. */
function junkOf(entries, o) {
  const { CharmNestPDF: P } = require(path.join(__dirname, "..", "netlify/functions/_charmNestPdf.js"));
  const out = [], byFile = new Map(), excluded = o.exclude ? onlySet(o.exclude) : new Set();
  for (const e of entries) for (const f of filesOf(e)) if (f.kind === "ai") { if (!byFile.has(f.path)) byFile.set(f.path, []); byFile.get(f.path).push(e); }
  const bad = new Set();
  for (const e of entries) { const k = up(e.sku); if (!P.parseSkuLabel(k)) bad.add(k); else if (o.numbers && /^\d{1,3}(?:[.,]\d+)?$/.test(k)) bad.add(k); }
  for (const e of entries) {
    const k = up(e.sku); if (!bad.has(k)) continue;
    const mates = [...new Set(filesOf(e).filter(f => f.kind === "ai").flatMap(f => (byFile.get(f.path) || []).map(x => up(x.sku))))].filter(x => x !== k);
    out.push({ sku: e.sku, aiPath: e.aiPath || Object.values(e.sizes || {}).map(g => g.aiPath)[0] || null, sharesFileWith: mates, shareGood: mates.filter(x => !bad.has(x)), skipped: excluded.has(k) ? "on a sheet or in the pool (exclude list)" : null });
  }
  return out;
}
async function junk(o, log) {
  if (!o.from) throw new Error("junk needs --from (a backup folder with index-all.json)");
  const before = JSON.parse(fs.readFileSync(path.join(o.from, "index-all.json"), "utf8")), list = junkOf(before.entries, o);
  fs.writeFileSync(path.join(o.from, "junk-skus.json"), json(list));
  const keep = list.filter(x => !x.skipped);
  log(`junk: ${list.length} of ${before.entries.length} index records are not SKUs by today's rule (${keep.length} to prune, ${list.length - keep.length} skipped: in use) · ${list.filter(x => x.shareGood.length).length} of them share a file with a real SKU, which keeps the file · ${o.from}/junk-skus.json`);
  log(`  e.g. ${list.slice(0, 12).map(x => x.sku).join(", ")}`);
  return list;
}
async function prune(o, log) {
  if (!o.origin || !o.from) throw new Error("prune needs --origin and --from");
  const before = JSON.parse(fs.readFileSync(path.join(o.from, "index-all.json"), "utf8")), want = o.skus ? onlySet(o.skus) : null;
  let list = junkOf(before.entries, o).filter(x => !want || want.has(up(x.sku)));
  const skipped = list.filter(x => x.skipped); list = list.filter(x => !x.skipped);
  log(`prune: ${list.length} junk record(s) to remove${skipped.length ? `, ${skipped.length} left (in use): ${skipped.slice(0, 8).map(x => x.sku).join(", ")}` : ""}${o.write ? "" : " · DRY (add --write to remove them)"}`);
  if (!o.write) { log(`  would remove: ${list.slice(0, 30).map(x => x.sku).join(", ")}${list.length > 30 ? " …" : ""}`); return { would: list.length }; }
  // the backup must hold what is being removed, or restore could not bring it back
  const man = JSON.parse(fs.readFileSync(path.join(o.from, "manifest.json"), "utf8")), have = new Set(man.files.map(f => f.path));
  const unbacked = list.filter(x => x.aiPath && !have.has(x.aiPath));
  if (unbacked.length) throw new Error(`${unbacked.length} record(s) have no backed-up file (${unbacked.slice(0, 5).map(x => x.sku).join(", ")}): back them up first (backup --skus <this list>). Nothing was removed.`);
  let n = 0; const failed = [];
  await pool(2, list, async x => { try { await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterRemoveSku", sku: x.sku }); n++; } catch (e) { failed.push({ sku: x.sku, error: e.message }); } });
  const gone = new Set(); const skus = list.map(x => up(x.sku));
  for (let i = 0; i < skus.length; i += 300) { const r = await api(o.origin, o.passcode, "charmNestLibrary", { op: "masterGetMany", skus: skus.slice(i, i + 300) }); for (const k of Object.keys(r.entries || {})) gone.add(up(k)); }
  const still = skus.filter(k => gone.has(k));
  fs.writeFileSync(path.join(o.from, "pruned.json"), json({ at: new Date().toISOString(), removed: n, failed, stillThere: still }));
  log(`pruned ${n} record(s)${failed.length ? ` · ${failed.length} FAILED: ${failed.slice(0, 5).map(f => f.sku + " " + f.error).join("; ")}` : ""}${still.length ? ` · ${still.length} still in the index: ${still.slice(0, 8).join(", ")}` : " · read back: all gone"}`);
  if (failed.length || still.length) process.exitCode = 1;
  return { removed: n, failed: failed.length, still: still.length };
}

async function main(argv, log = console.log) {
  const o = args(argv);
  if (o.cmd === "backup") return backup(o, log);
  if (o.cmd === "diff") return diff(o, log);
  if (o.cmd === "verify") return verify(o, log);
  if (o.cmd === "restore") return restore(o, log);
  if (o.cmd === "junk") return junk(o, log);
  if (o.cmd === "prune") return prune(o, log);
  throw new Error("usage: catalog-repair.cjs backup|diff|verify|junk|prune|restore --origin <site> …  (see the header of this file)");
}
module.exports = { main, affected, filesOf, junkOf };
if (require.main === module) main(process.argv).catch(e => { console.error("catalog-repair:", e.message); process.exit(1); });
