/*  netlify/functions/_charmNestLabels.js — THE ONE LABELS UNIT (cloud plan v2 §1.9 "QR labels", build step 10a; designs.json "QR labels and the set manifest").
 *  It draws every QR label the cloud makes; every other job, and every press (labelsAsk), only marks a label as owed. Ported from the page:
 *    labelPlan / labelsReady / drawSheet   Sets.onSheetSaved's label half and Sets.labelsReady (charm-nest-bridge.js ~15860-15923): the orders placed on a sheet in the code the
 *                                          station reads (O.safeChunks + O.encodeOrderList, byte for byte), its title, the split notes of a pair or group (CharmNestPairLabels), the
 *                                          custom pieces of no order (CharmNestCustomPool); drawn by _charmNestQrLabel.js, saved beside the sheet's files with the download token the
 *                                          file already has, then the sheet's record (putSheet label) and its set's label list (setUpdate labelFiles) and the order's timeline (qrLabel)
 *    ownLabel                              the sheet window's Make QR label for a sheet in no open set (charm-nest-sheetwin.js ownLabel)
 *    evaluate / releaseIssue               Sets.evaluate and Sets.validateRelease, from the records: the run's lines, the set rules' gate, the green line's wait, the cardinal rule
 *                                          (Gate.cardinalSplit), the lines that changed or are held, and the server's own readiness of the set (op setReadiness)
 *    finalize                              Sets.finalize (~16011-16050) merged with SetEdit.remakeFiles: the labels PDF (one 145 pt page a label, vendored pdf-lib), the manifest PDF,
 *                                          set.json, each sheet's .pdf copy (sheetPdf), then the set's record (labels, status "labelled"); a COMMITTED set's files go through
 *                                          flowApply's setFiles step (applySetFiles), as the Library's edit of a committed set does
 *  Units:  passUnit   (pass item "labels", lane "labels")   the presses (labelsAsk: relabel | setFiles | complete) and every QR label a sheet of an open set owes; the presses are
 *                     answered whether or not a run is open (G5: the item is "runless", the driver runs it in a pass with no open run too, each ask on its set's / sheet's own run's records)
 *          stepUnit   (the run's "labels" step)              behind the Etsy freshness gate (§1.8a): each open set's release check and files, or its deferral with words
 *  Every Storage write is fenced first (the pass's lease nonce read again, and the run's carrier: C7); paths, bytes, tokens and timeline ids are deterministic, so a redo writes the
 *  same thing. Rendering yields between labels (P7). No Etsy call, no world build. DORMANT: runs only while the drive is "standby" or "on" with "labels" in drive.cloudSteps.       */
"use strict";
const crypto = require("crypto");
const Core = require("../../charm-nest-assembly-core.js");
const O = require("../../charm-nest-orders.js");
const PairLabels = require("../../charm-nest-pair-labels.js");
const CustomPool = require("../../charm-nest-custom-pool.js");
const SheetName = require("../../charm-nest-sheet-name.js");
const SetRules = require("../../charm-nest-set-rules.js");
const SetEdit = require("../../charm-nest-set-edit.js");
const Faces = require("../../charm-nest-engrave-faces.js");
const QrLabel = require("./_charmNestQrLabel");
const Assembly = require("./_charmNestAssembly");
const { COLLECTIONS } = require("./_charmNestCloudSetOps");

const num = v => (Number.isFinite(+v) ? +v : 0);
const turn = () => new Promise(r => (typeof setImmediate === "function" ? setImmediate(r) : setTimeout(r, 0)));
const hashOf = v => O.textHash(JSON.stringify(v === undefined ? null : v));
const say = (level, text) => { try { console.log(`[labels] ${level}: ${String(text).slice(0, 600)}`); } catch (_) { /* a note only */ } };
const words = r => String((r && (r.error || r.why)) || "no answer").slice(0, 300);
const placedOf = sh => { const byId = new Map((sh.charms || []).map(c => [c.id, c])); return (sh.placements || []).map(p => byId.get(p.id)).filter(Boolean); };
const ridOf = c => String((c && (c.order || c.id)) || "").split("/")[0];
const faceOf = x => (Faces && Faces.faceOf ? Faces.faceOf(x) : x && x.face === "front" ? "front" : "back");
const ansi = t => String(t).replace(/→/g, "->").replace(/↔/g, "<->").replace(/·/g, "-").replace(/[^\x20-\x7E\xA0-\xFF]/g, "?");

/* ── the fence before a Storage write (C7): this pass still holds the lease and the cloud still carries the run ── */
async function fenceCheck(db, fence) {
  if (!fence || !fence.path || !fence.nonce) return;
  const seg = String(fence.path).split("/"), refs = [db.collection(seg[0]).doc(seg[1])].concat(fence.runId ? [db.collection("Charm_Nest_Run_Carrier").doc(String(fence.runId))] : []);
  const [lease, carrier] = await db.getAll(...refs), L = lease.exists ? lease.data() || {} : {};
  if (L.nonce !== fence.nonce || L.state !== "running") throw Object.assign(new Error("fenced: another pass of the cloud holds the lease now; nothing was uploaded"), { fenced: true });
  if (fence.runId) { const c = carrier && carrier.exists ? (carrier.data() || {}).carrier : null; if (!c || c.dev !== "cloud") throw Object.assign(new Error(`fenced: ${(c && c.by) || "a computer"} carries this run now; nothing was uploaded`), { fenced: true }); }
}
/** A file saved in the bucket with the download token it already has (charmNestOutput op_put's rule: every stored link stays good). → { path, url } */
async function upload(ctx, path, buf, contentType) {
  await fenceCheck(ctx.db, ctx.fence);
  const bucket = (ctx.bucket || require("./firebaseAdmin").storage().bucket()), file = bucket.file(path);
  let token = null; try { const [meta] = await file.getMetadata(); const t = meta && meta.metadata && meta.metadata.firebaseStorageDownloadTokens; token = t ? String(t).split(",")[0] : null; } catch (_) { token = null; }
  token = token || (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
  await file.save(buf, { resumable: false, contentType, metadata: { cacheControl: "public, max-age=31536000", metadata: { firebaseStorageDownloadTokens: token, uploadedBy: "cloud" } } });
  return { path, url: "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(token) };
}
async function download(ctx, path) { const bucket = (ctx.bucket || require("./firebaseAdmin").storage().bucket()); const [buf] = await bucket.file(path).download(); return buf; }

/* ── a sheet's label, as the page makes it ── */
const sheetsOf = (V, set) => V.pages.filter(p => !p.recalled && p.setId === set.setId && !p.draft);
/** Sets.pairNotes: the lines a sheet's label carries for a group (a pair, a necklace's discs) with pieces on another sheet of its set. */
function pairNotes(V, sh, set, placed, only) {
  if (!set || !sh || !sh.sheetId) return [];
  const base = PairLabels.piecesOfOrders(set.orders), here = PairLabels.piecesOfCharms(placed);
  if (!here.length) return [];
  const live = sheetsOf(V, set).filter(p => p.sheetId !== sh.sheetId).map(p => ({ sheetId: p.sheetId, sheet: p.fileBase, pieces: PairLabels.piecesOfCharms(placedOf(p)) }));
  live.push({ sheetId: sh.sheetId, sheet: sh.fileBase, pieces: here });
  return PairLabels.splitNotes(sh.sheetId, PairLabels.reconcile(base, live), only);
}
/** What a sheet's label(s) must be now: [{ i, n, slice, payload, label, notes, path }], with the orders and the metal code. */
function labelPlan(V, sh, set) {
  const placed = placedOf(sh), CPL = CustomPool, ids = [...new Set(placed.map(ridOf))].filter(id => id && !(CPL && CPL.orderless(id))), loose = CPL ? CPL.labelNotes(placed) : [];
  const metalDS = O.CARD_TO_METAL[sh.metal] || sh.metal, parts = O.safeChunks(ids, metalDS, 1000, 500, 8); if (!parts.length && placed.length) parts.push([]);
  const out = parts.map((slice, i) => ({ i, n: parts.length, slice, payload: O.encodeOrderList(slice, metalDS),
    label: `${Core.METAL_TAG[sh.metal]} · ${set.name} · Sheet ${sh.sheetIndex || (SheetName && SheetName.numberOf(sh)) || sh.page}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} order${slice.length === 1 ? "" : "s"}${i === 0 && loose.length ? " + custom" : ""}`,
    notes: pairNotes(V, sh, set, placed, slice).concat(i === 0 ? loose : []),
    path: `${sh.folderPath}/${sh.fileBase}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png` }));
  return { parts: out, ids, metalDS, placed };
}
/** Sets.labelsReady: the sheet's label files and its set's list say exactly what would be made now. */
function labelsReady(V, sh, set) {
  const plan = labelPlan(V, sh, set), files = (sh.label && sh.label.files) || [], indexed = (set.labelFiles || []).filter(f => f.sheetId === sh.sheetId);
  return sh.setId === set.setId && !sh.draft && plan.parts.length > 0 && files.length === plan.parts.length && indexed.length === files.length &&
    files.every((f, i) => f.url && f.path && f.sheet === sh.fileBase && f.payload === plan.parts[i].payload && indexed.some(g => g.path === f.path && g.url === f.url && g.part === f.part) && (f.notes || []).join("|") === plan.parts[i].notes.join("|"));
}
/** The timeline of each order on a label (SheetEvents.qrLabel): one event per order and label, its id fixed by the label's codes. */
function qrEvents(sh, ids, files, more) {
  const fnv = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return (h >>> 0).toString(36); };
  const code = fnv((files || []).map(f => f.payload || "").join("|") + "|" + (ids || []).join()), at = Date.now(), name = SheetName ? SheetName.name(sh) : `${Core.METAL_TAG[sh.metal]} Sheet ${sh.sheetIndex || sh.page || 1}`;
  return (ids || []).filter(rid => /^\d{4,}$/.test(String(rid))).map(rid => ({ type: "qrLabel", orderId: String(rid), sheetId: sh.sheetId, sheet: name, setId: (more && more.setId) || "", lineKey: "", transactionId: "", id: `${sh.sheetId}-${code}`, at,
    text: `QR label for ${name}${more && more.set ? ` · ${more.set}` : ""}`, by: "Cloud", data: { metal: sh.metal, labels: (files || []).length, set: (more && more.set) || undefined, own: (more && more.own) || undefined } }));
}
/** Draws a sheet's label(s) and records them: the sheet first (putSheet label), then its set's list (setUpdate labelFiles, labels: null), then the orders' timelines. */
async function drawSheet(ctx, V, sh, set) {
  const plan = labelPlan(V, sh, set), files = [];
  for (const p of plan.parts) {
    await turn();
    const img = QrLabel.renderPng({ payload: p.payload, label: p.label, notes: p.notes });
    const up = await upload(ctx, p.path, img.png, "image/png");
    files.push({ path: up.path, url: up.url, sheet: sh.fileBase, sheetId: sh.sheetId, metal: plan.metalDS, part: p.i + 1, parts: p.n, orders: p.slice, payload: p.payload, ecc: img.ecc, label: p.label, ...(p.notes.length ? { notes: p.notes } : {}) });
  }
  const label = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, ecc: f.ecc, label: f.label, ...(f.notes ? { notes: f.notes } : {}) })), orders: plan.ids, sig: hashOf(files.map(f => [f.payload, f.notes || []])), by: "cloud", at: Date.now() };
  // (the label and its orders only: the sheet's membership is the assembly's and already saved; the server refuses a cloud call that names the membership of a sheet an open set lists)
  const r1 = await ctx.call({ op: "putSheet", sheet: { id: sh.sheetId, label, orders: plan.ids, runId: sh.runId } }, `label|${sh.sheetId}|${label.sig}`);
  if (!r1 || r1.status >= 400) throw new Error(`${sh.fileBase}: its QR label was made but not saved on its record: ${words(r1)}`);
  sh.label = label;
  set.labelFiles = (set.labelFiles || []).filter(f => f.sheetId !== sh.sheetId).concat(files.map(f => Object.assign({ sheetId: sh.sheetId }, f)));
  set.labels = null;
  const r2 = await ctx.call({ op: "setUpdate", setId: set.setId, patch: { labelFiles: Core.setPatch(set).labelFiles, labels: null } }, `label-set|${set.setId}|${sh.sheetId}|${label.sig}`);
  if (!r2 || r2.status >= 400) throw new Error(`${set.name}: the label list was not saved: ${words(r2)}`);
  const events = qrEvents(sh, plan.ids, files, { set: set.name, setId: set.setId });
  for (let i = 0; i < events.length; i += 200) await ctx.call({ op: "timelineAdd", events: events.slice(i, i + 200) }, `qr-events|${sh.sheetId}|${label.sig}|${i}`).catch(() => null);
  say("cloud", `${sh.fileBase}: ${files.length} label${files.length === 1 ? "" : "s"} saved (${plan.ids.length} order${plan.ids.length === 1 ? "" : "s"}) · set ${set.name} now ${(set.sheetIds || []).length} sheet(s)`);
  return files;
}
/** ownLabel (the sheet window's Make QR label for a sheet in no open set). */
async function ownLabel(ctx, rec) {
  const byId = new Map((rec.charms || []).map(c => [c.id, c])), ids = [...new Set((rec.placements || []).map(p => byId.get(p.id)).filter(Boolean).map(c => (c.poolId ? ridOf(c) : "")).filter(Boolean))];
  if (!ids.length) throw new Error("no order is placed on this sheet");
  const metal = O.CARD_TO_METAL[rec.metal] || rec.metal, parts = O.safeChunks(ids, metal, 1000, 500, 8), name = rec.fileBase || rec.folder || rec.id;
  const base = rec.outputs && rec.outputs.ai && rec.outputs.ai.path ? rec.outputs.ai.path.replace(/\/[^/]*$/, "") : `charmnest/sheets/${rec.day}/${name}`, files = [];
  for (const [i, slice] of parts.entries()) {
    await turn();
    const payload = O.encodeOrderList(slice, metal), label = `${Core.METAL_TAG[rec.metal] || ""} · ${name}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} order${slice.length === 1 ? "" : "s"}`;
    const img = QrLabel.renderPng({ payload, label, notes: [] }), up = await upload(ctx, `${base}/${name}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png`, img.png, "image/png");
    files.push({ path: up.path, url: up.url, sheet: name, part: i + 1, parts: parts.length, orders: slice, payload, ecc: img.ecc, label });
  }
  const made = { files, orders: ids, own: true, at: Date.now() };
  const r = await ctx.call({ op: "putSheet", sheet: { id: rec.id, label: made } }, `own-label|${rec.id}|${hashOf(files.map(f => f.payload))}`);
  if (!r || r.status >= 400) throw new Error(`${name}: its own QR label was not saved: ${words(r)}`);
  const events = qrEvents(Object.assign({ sheetId: rec.id }, rec), ids, files, { own: true });
  if (events.length) await ctx.call({ op: "timelineAdd", events: events.slice(0, 200) }, `own-events|${rec.id}`).catch(() => null);
  return made;
}

/* ── release: Sets.evaluate and Sets.validateRelease, from the records ── */
function evaluate(set, run) {
  const byOrder = new Map();
  for (const [key, l] of Object.entries((run && run.lines) || {})) { if (!l || l.orderId == null) continue; const rid = String(l.orderId); if (!byOrder.has(rid)) byOrder.set(rid, []); byOrder.get(rid).push(Object.assign({ key }, l)); }
  const out = { committable: [], held: {}, gone: [] };
  for (const [rid, lines] of byOrder) {
    if (!set.orders[rid]) continue;   // (the set's own orders: the page read every row of the run, and only the set's orders are said)
    if (lines.every(l => l.state === "gone")) { out.gone.push(rid); continue; }
    // (a run line is the record's copy of the page's row: its spec's noDesign, and a card completed by hand (handDone) for spec.customDone, as Orders.lineRecord kept them)
    const ev = O.evaluateOrder(lines.map(l => ({ key: l.key, spec: { material: l.material, quantity: l.quantity, pieceCount: l.pieceCount, designSku: l.sku, noDesign: !!l.noDesign, ...(l.handDone ? { customDone: { state: "done", how: l.handDone.how || "button" } } : {}) }, state: l.state, reason: l.reason, problems: (l.problems || []).map(k => (typeof k === "string" ? { kind: k } : k)), engrave: l.engrave, changePending: l.changePending, hold: l.hold })));
    const byPerson = lines.find(l => l.hold);
    if (ev.committable && !byPerson) out.committable.push(rid); else out.held[rid] = byPerson ? { line: byPerson.key, why: byPerson.hold } : ev.held;
    if (set.orders[rid]) set.orders[rid].held = out.held[rid] || null;
  }
  return out;
}
/** The reason a set cannot go to its labels now, in words; "" when it can (Sets.releaseIssue). */
async function releaseIssue(ctx, V, G, set) {
  const sheets = sheetsOf(V, set);
  if (sheets.some(sh => sh.runHold)) return "A sheet in this set still needs attention";
  { const held = sheets.find(sh => sh.cutCheck && typeof sh.cutCheck === "object" && Object.keys(sh.cutCheck).some(pid => placedOf(sh).some(c => c.poolId === pid)));
    if (held) { const [pid, w] = Object.entries(held.cutCheck).find(([k]) => placedOf(held).some(c => c.poolId === k)); return `${held.fileBase || "A sheet in this set"}: the cut line of ${pid} changed${w && w.why ? ` and ${w.why}` : ""}: move it clear in Manual Control, or nest the sheet again`; } }
  const principle = SetRules.gate(set, sheets);
  if (principle && principle.applies && !principle.ok) return `${set.name || "This set"} ${principle.short}: a set needs at least 1 completed GF sheet and 1 completed SS sheet`;
  { const rg = sheets.find(sh => sh.metal === "rose" && !sh.roseCutAt && !(+sh.laserDoneAt > 0) && sh.autoMatched && String(sh.autoMatched) === String(set.setId)); if (rg) return `${set.name || "This set"} is waiting for the green line cut of ${SheetName ? SheetName.name(rg) : rg.fileBase || "its Rose Gold sheet"}${rg.autoCutWhy ? `: ${rg.autoCutWhy}` : ""}`; }
  const split = G.cardinalSplit(set);
  if (split.length) {
    const there = [...new Set(split.flatMap(i => i.there))], os = split.map(i => i.orderId);
    if (split.length === 1 && split[0].words) return `Order ${split[0].orderId} ${split[0].words}, and ${there.join(", ")} ${there.length === 1 ? "is" : "are"} not in this set: sheets that share a multi-piece order go into the same set`;
    return `${os.length === 1 ? "Order " + os[0] + " is" : "Orders " + os.slice(0, 3).join(", ") + (os.length > 3 ? " and more are" : " are")} also on ${there.join(", ")}, which ${there.length === 1 ? "is" : "are"} not in this set: sheets that share a multi-piece order go into the same set`;
  }
  const pools = new Set(sheets.flatMap(sh => placedOf(sh).map(c => c.poolId)));
  if (Object.values(V.run.lines || {}).some(l => l && l.state !== "gone" && (l.changePending || l.hold) && (l.poolIds || []).some(id => pools.has(id)))) return "An order on this sheet changed or needs review";
  if (!set.sheetIds.length || sheets.length !== set.sheetIds.length || set.sheetIds.some(id => !sheets.some(sh => sh.sheetId === id))) return "The set's sheets are not all in the cloud yet";
  for (const sh of sheets) {
    const pol = G.policy(sh, set.seq);
    if (!pol.include) return `${Core.labelOf(sh.metal)}: ${pol.reason}`;
    if (!sh.persistedDone || !sh.outputs) return `${sh.fileBase}: sheet files are not saved`;
    if (!labelsReady(V, sh, set)) return `${sh.fileBase}: QR labels are missing or out of date`;
  }
  // (the server's own readiness: engraving approvals, saved back files, layout checks, QR labels, every order's pieces; and its cardinal check)
  const rd = await ctx.read({ op: "setReadiness", setId: set.setId });
  if (!rd || rd.error) return `${set.name}: its readiness could not be read (${words(rd)})`;
  if (!rd.ready) {
    // (a 10K / 14K / Rose Gold sheet whose newest charms wait for its Cut Sheet: named, with what to press, as RoseStock.waitWords)
    const uncut = sheets.find(sh => ["rose", "gold10k", "gold14k"].includes(sh.metal) && !sh.roseCutAt && !sh.rosePlanHash && sh.roseStockId);
    return uncut ? `${Core.labelOf(uncut.metal)} ${SheetName ? SheetName.short(uncut) : `Sheet ${uncut.page || 1}`} has charms not cut yet: press Cut Sheet` : (rd.why || "Set is not ready for laser: finish every sheet's engraving approvals, saved back files, layout checks and QR labels");
  }
  if ((rd.split || []).length) { const it = rd.split[0]; return `Order ${it.orderId} also has pieces on ${(it.there || []).join(", ")}, which ${(it.there || []).length === 1 ? "is" : "are"} not in this set: sheets that share a multi-piece order go into the same set`; }
  return "";
}

/* ── finalize (Sets.finalize, SetEdit.remakeFiles) ── */
async function fullSheets(ctx, ids) {
  const out = [];
  for (const id of ids) { const r = await ctx.read({ op: "getSheet", id }); if (r && r.sheet) out.push(Object.assign({ id }, r.sheet)); }
  return out;
}
async function finalize(ctx, V, set, o = {}) {
  const PDFLib = require("../../vendor/pdf-lib-1.17.1.min.js"), { PDFDocument, StandardFonts } = PDFLib;
  const members = sheetsOf(V, set), recs = await fullSheets(ctx, members.map(p => p.sheetId)), byId = new Map(recs.map(r => [r.id, r]));
  const files = (set.labelFiles || []).filter(f => f && f.path).slice().sort((a, b) => String(a.sheet).localeCompare(String(b.sheet)) || num(a.part) - num(b.part));
  const labels = await PDFDocument.create();
  for (const f of files) { await turn(); const img = await labels.embedPng(await download(ctx, f.path)); labels.addPage([145, 145]).drawImage(img, { x: 0, y: 0, width: 145, height: 145 }); }
  const ev = o.edited ? { committable: Object.keys(set.orders), held: {}, gone: [] } : evaluate(set, V.run);
  const man = await PDFDocument.create(), font = await man.embedFont(StandardFonts.Helvetica), bold = await man.embedFont(StandardFonts.HelveticaBold);
  let page = man.addPage([612, 792]), y = 756;
  const line = (t, opts = {}) => { if (y < 48) { page = man.addPage([612, 792]); y = 756; } page.drawText(ansi(t).slice(0, 110), Object.assign({ x: 36, y, size: 9.5, font }, opts)); y -= opts.size ? opts.size + 4 : 13; };
  line(`${set.name} · ${set.day} · run ${set.runId}`, { size: 15, font: bold });
  line(`${set.sheetIds.length} sheet(s) · materials ${(set.materials || []).map(Core.labelOf).join(", ")} · ${Object.keys(set.orders).length} order(s) · ${ev.committable.length} committable · ${Object.keys(ev.held).length} held · ${ev.gone.length} gone${o.edited ? " · edited after commit" : ""}`); y -= 6;
  line("Orders and sheets", { font: bold, size: 11 });
  for (const [rid, od] of Object.entries(set.orders).sort()) { const copies = PairLabels.manifestEntries(od.lines, undefined, "→"); line(`${rid}  ${ev.held[rid] ? "HELD: " + (ev.held[rid].why || "") + "  " : ""}${copies.join("  ")}`); }
  const spans = SetEdit && SetEdit.spanLines ? SetEdit.spanLines(set.orders) : [];
  if (spans.length) { y -= 6; line("Orders on more than one sheet (they stay in one set)", { font: bold, size: 11 }); spans.forEach(t => line(t)); }
  y -= 6; line("Engraving", { font: bold, size: 11 });
  const backsOf = p => ((byId.get(p.sheetId) || {}).backPool || p.backPool || []).filter(b => b && !b.invalidated);
  const backs = members.flatMap(sh => backsOf(sh).map(b => `${sh.fileBase}: ${b.order} ${b.sku} #${b.copy}${faceOf(b) === "front" ? " · FRONT" : ""}${PairLabels.backWord(b) + (PairLabels.discWord ? PairLabels.discWord(b) : "")} "${String(b.text || "").replace(/\n/g, " / ")}" ${b.sizePt} pt · ${b.approvedBy || "?"}`));
  if (backs.length) backs.forEach(b => line(b)); else line("no engraving in this set");
  y -= 6; line("Labels", { font: bold, size: 11 }); files.forEach(f => line(`${f.label}  ${f.path || "(not uploaded)"}`));
  if (o.edited) { const edits = (Array.isArray(set.flowHistory) ? set.flowHistory : []).filter(h => h && h.type === "setEdit"); if (edits.length) { y -= 6; line("Changes after the commit", { font: bold, size: 11 }); edits.slice(-12).forEach(h => line(`${new Date(+h.at || 0).toISOString().slice(0, 16).replace("T", " ")}  ${h.by || ""}: ${h.note || ""}`)); } }
  // (a sheet without its .pdf yet gets it now, copied from its .ai inside the bucket: op_sheetPdf, which reads its own fence before the copy)
  const noPdf = members.filter(sh => { const r = byId.get(sh.sheetId) || {}; return r.outputs && r.outputs.ai && !r.outputs.pdf; });
  if (noPdf.length) { const r = await ctx.call({ op: "sheetPdf", ids: noPdf.map(sh => sh.sheetId) }, `sheet-pdf|${set.setId}|${noPdf.map(sh => sh.sheetId).join(",")}`); if (r && r.urls) for (const sh of noPdf) { const rec = byId.get(sh.sheetId); if (rec && r.urls[sh.sheetId]) rec.outputs = Object.assign({}, rec.outputs, { pdf: { url: r.urls[sh.sheetId] } }); } }
  const cloudOf = rec => rec && rec.outputs ? { ai: rec.outputs.ai && rec.outputs.ai.url, pdf: rec.outputs.pdf && rec.outputs.pdf.url, labelled: rec.outputs.labelled && rec.outputs.labelled.url, report: rec.outputs.report && rec.outputs.report.url, preview: rec.outputs.preview && rec.outputs.preview.url } : null;
  const json = { setId: set.setId, runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials,
    sheets: members.map(sh => { const rec = byId.get(sh.sheetId) || {}; return { sheetId: sh.sheetId, name: sh.fileBase, metal: sh.metal, sheetIndex: sh.sheetIndex, folder: sh.folderPath, orders: sh.label ? sh.label.orders : [], placements: (sh.placements || []).length,
      backs: backsOf(sh).map(b => ({ poolId: b.poolId, ...(faceOf(b) === "front" ? { face: "front" } : {}), order: b.order, sku: b.sku, copy: b.copy, ...(b.side === "L" || b.side === "R" ? { side: b.side, groupKey: b.groupKey || undefined } : {}), text: b.text, approvedBy: b.approvedBy, file: b.outputs && b.outputs.ai && b.outputs.ai.path })),
      verification: sh.verification && { ok: sh.verification.ok }, outputs: cloudOf(rec) }; }),
    orders: Object.fromEntries(Object.entries(set.orders).map(([rid, od]) => [rid, { held: ev.held[rid] || null, lines: Object.values(od.lines || {}) }])), held: ev.held, gone: ev.gone,
    labels: files.map(f => ({ sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, path: f.path })), approvals: backs.length, generatedAt: new Date().toISOString() };
  const backCount = members.reduce((n, sh) => n + backsOf(sh).length, 0);
  const lp = await upload(ctx, `${set.folder}/labels/${set.name}_labels.pdf`, Buffer.from(await labels.save({ useObjectStreams: false })), "application/pdf");
  const mp = await upload(ctx, `${set.folder}/${set.name}_manifest.pdf`, Buffer.from(await man.save({ useObjectStreams: false })), "application/pdf");
  const jp = await upload(ctx, `${set.folder}/set.json`, Buffer.from(JSON.stringify(json, null, 1)), "application/json");
  const made = { pdf: { path: lp.path, url: lp.url }, manifest: { path: mp.path, url: mp.url }, json: { path: jp.path, url: jp.url }, files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
  set.labels = made; set.backCount = backCount;
  if (set.committedAt) {
    // (a committed set's files are written as the Library's edit of a committed set writes them: flowApply's setFiles step, applySetFiles; its status stays)
    const r = await ctx.call({ op: "flowApply", steps: [{ type: "setFiles", setId: set.setId, labelFiles: Core.setPatch(set).labelFiles, labels: made }], device: "cloud", via: "cloud labels" }, `set-files|${set.setId}|${hashOf(made.files)}`);
    if (!r || r.status >= 400) throw new Error(`${set.name}: its files were made but not recorded: ${words(r)}`);
  } else {
    if (!o.keepStatus) set.status = "labelled";
    const patch = Core.setPatch(set), r = await ctx.call({ op: "setUpdate", setId: set.setId, patch: { labels: made, labelFiles: patch.labelFiles, status: set.status, backCount, orders: patch.orders } }, `set-labelled|${set.setId}|${hashOf([made.files, set.status, backCount])}`);
    if (!r || r.status >= 400) throw new Error(`${set.name}: its files were made but not recorded: ${words(r)}`);
  }
  say("cloud", `${set.name}: ${files.length} label(s) collected in labels/${set.name}_labels.pdf · manifest and set.json saved`);
  return made;
}

/* ── the pass item: a person's asks, then every label an open set's sheet owes ── */
async function askList(db, runId) {
  // (the asks still to answer: one single-field query, then this run's)
  const q = await db.collection(COLLECTIONS.LABEL_ASKS).where("state", "in", ["asked", "working"]).limit(100).get();
  const now = Date.now();
  // (an ask of another run, a Library edit of an older run's set, is answered too, with that run's records)
  return q.docs.map(d => d.data()).filter(a => a && (a.state === "asked" || (a.state === "working" && now - num(a.at) > 10 * 60000))).sort((a, b) => (a.runId === runId ? 0 : 1) - (b.runId === runId ? 0 : 1) || num(a.at) - num(b.at));
}
async function answer(ctx, ask, patch) { await ctx.call({ op: "labelsAskPut", key: ask.key, seq: ask.seq, patch }, `label-ask|${ask.key}|${ask.seq}|${patch.state}`); }
/** A run's record: the open run's own (ctx.run), or another run's, read (its lines too); a run the cloud no longer has reads as an empty one of today's rules. */
async function runRecord(ctx, runId) {
  if (ctx.run && ctx.run.runId && runId === ctx.run.runId) return ctx.run;
  const got = runId ? await ctx.read({ op: "runGet", runId }) : null;
  return (got && got.run) || { runId: runId || null, releasePolicy: 2, lines: {} };
}
/** The run an ask belongs to. A ctx whose run is that run (G5: also when no run is open, ctx.run null). */
async function runCtx(ctx, runId) {
  if (ctx.run && ctx.run.runId && (!runId || runId === ctx.run.runId)) return ctx;
  return Object.assign({}, ctx, { run: await runRecord(ctx, runId), runId: runId || null });
}
/** (G5) The run whose records answer an ask: a sheet's ask, for a sheet in a set, is answered with its SET's run (the set, its sheets, its pair notes), the sheet itself drawn from
 *  its own run's record (withOwnRuns); a set's ask with the set's run; a sheet in no set with its own run. */
async function askRunId(ctx, ask) {
  if (ask.setId || !ask.sheetId) return ask.runId || null;
  const sd = await ctx.db.collection("Charm_Nest_Sheets").doc(String(ask.sheetId)).get(), rec = sd.exists ? sd.data() || {} : null;
  if (!rec) return ask.runId || null;
  if (rec.draft || !rec.setId) return rec.runId || ask.runId || null;
  const st = await ctx.db.collection("Charm_Nest_Sets").doc(String(rec.setId)).get(), setRun = st.exists ? (st.data() || {}).runId || null : null;
  return setRun || rec.runId || ask.runId || null;
}
/** (G5) Every sheet the named sets list joins the view, each drawn from ITS OWN run's record when that is not the set's run (a sheet moved in the Library from another run's
 *  drafts into a set keeps its run): Assembly.loadView reads the set's run's sheets only, so such a sheet was missing from its set and its label could not be drawn. */
async function withOwnRuns(ctx, V, setIds) {
  const have = new Set(V.pages.map(p => p.sheetId)), setsById = new Map(V.sets.map(s => [String(s.setId), s])), want = new Set((setIds || []).map(String));
  const ids = [...new Set(V.sets.filter(s => want.has(String(s.setId))).flatMap(s => s.sheetIds || []).map(String))].filter(id => id && !have.has(id));
  if (!ids.length) return V;
  const runs = new Map(V.run && V.run.runId ? [[String(V.run.runId), V.run]] : []);
  for (let i = 0; i < ids.length; i += 100) for (const d of await ctx.db.getAll(...ids.slice(i, i + 100).map(id => ctx.db.collection("Charm_Nest_Sheets").doc(id)))) {
    if (!d.exists) continue;
    const rec = Object.assign({ id: d.id }, d.data());
    if (rec.archived === true || !rec.runId || (V.run && rec.runId === V.run.runId)) continue;   // (one of the set's own run that the view left out is archived or gone: as before)
    const rid = String(rec.runId);
    if (!runs.has(rid)) runs.set(rid, await runRecord(ctx, rid));
    V.pages.push(Core.asPage(rec, { run: runs.get(rid), setsById, now: V.now }));
    V.records.set(rec.id, rec);
  }
  return V;
}
/** The view of a run for the labels unit: its records (Assembly.loadView), with the named sets' sheets of other runs (withOwnRuns; every set of the view when none is named: only the sheets it left out are read). */
async function viewOf(ctx, run, setIds, o) {
  const V = await Assembly.loadView(Object.assign({ db: ctx.db, run, now: Date.now() }, o || {}));
  return withOwnRuns(ctx, V, setIds || V.sets.map(s => s.setId));
}
async function doAsk(ctx0, ask) {
  ctx0 = Assembly.passKeyed(ctx0);
  const open = ctx0.run && ctx0.run.runId ? ctx0.run : null;   // (G5: the open run, or none: an ask is answered either way)
  const ctx = await runCtx(ctx0, await askRunId(ctx0, ask)), run = ctx.run;
  // (setFiles: the set's files made again, behind the Etsy freshness gate (§1.8a), fresh since the press: the mail system synced and the world was built after it. While the gate
  //  fails, the ask waits as it is, its words written once when they change)
  if (ask.kind === "setFiles") {
    let receipts = [];
    if (ask.setId) { const sd = await ctx.db.collection("Charm_Nest_Sets").doc(ask.setId).get(); receipts = sd.exists ? Object.keys((sd.data() || {}).orders || {}) : []; }
    const g = await ctx.gate({ receipts, stepStartedAt: num(ask.at) });
    if (!g.ok) { const w = g.why.join("; "); if (w !== String(ask.why || "")) await answer(ctx, ask, { state: "asked", why: w }); return { waiting: g.why }; }
  }
  await answer(ctx, ask, { state: "working" });
  try {
    if (ask.kind === "complete") {
      const s = await ctx.read({ op: "getSheet", id: ask.sheetId }), rec = s && s.sheet ? Object.assign({ id: ask.sheetId }, s.sheet) : null;
      if (!rec) throw new Error("That sheet is no longer in the Library");
      // (Paul 21 Nov 2026, item 6: the sheet window's Make QR label from a viewer: "done" only once a label was made. A sheet of a COMMITTED set (or one whose set the run no longer
      //  lists) used to be answered "done" with nothing drawn; it gets a label of its own, as the page's own press made it, unless it already has one)
      const inSet = !!(rec.setId && !rec.draft);
      if (inSet && run.runId) { const V = await viewOf(ctx, run, [rec.setId]), sh = V.pages.find(p => p.sheetId === rec.id), set = sh && V.sets.find(x => x.setId === sh.setId);
        if (sh && set && !set.committedAt) { await drawSheet(ctx, V, sh, set); return answer(ctx, ask, { state: "done", doneAt: Date.now() }); }
        if (((rec.label && rec.label.files) || []).length) return answer(ctx, ask, { state: "done", why: "it has its QR label already", doneAt: Date.now() }); }
      if (!inSet && open && rec.runId === open.runId && open.releasePolicy === 2) {   // (the release press is the OPEN run's: a sheet of another run gets a label of its own, ownLabel)
        // (the release press: its release mark, then a look at the sets; a draft that still does not join gets back its filling state and the reason, sheetwin doLabel)
        const wasFull = !!rec.releaseFull, top = rec.topup && !rec.topup.closedAt ? Object.assign({}, rec.topup, { closedAt: Date.now(), byHand: true }) : rec.topup || null;
        const r = await ctx.call({ op: "putSheet", sheet: { id: rec.id, releaseFull: true, topup: top } }, `complete|${rec.id}|${ask.seq}`);
        if (!r || r.status >= 400) throw new Error(words(r));
        await Assembly.assemblyUnit(ctx);
        const V = await viewOf(ctx, run), sh = V.pages.find(p => p.sheetId === rec.id);
        if (sh && !sh.draft) { const set = V.sets.find(x => x.setId === sh.setId); if (set) await drawSheet(ctx, V, sh, set); return answer(ctx, ask, { state: "done", doneAt: Date.now() }); }
        const G = Core.gate(V), open = G.newestOpenOf(run.runId), why = sh ? (sh.setWait || sh.cardinalHold || G.policy(sh, open ? open.seq : 0).reason) : "it is not in the run";
        if (!wasFull) await ctx.call({ op: "putSheet", sheet: { id: rec.id, releaseFull: false, topup: rec.topup || null }, reopen: true }, `complete-undo|${rec.id}|${ask.seq}`);
        return answer(ctx, ask, { state: "refused", why: String(why || "it cannot join a set now"), doneAt: Date.now() });
      }
      await ownLabel(ctx, rec);
      return answer(ctx, ask, { state: "done", doneAt: Date.now() });
    }
    if (!run.runId) throw new Error(ask.setId ? "That set is in no run of the cloud" : "That sheet is in no set of a run");
    const V = await viewOf(ctx, run);
    const set = ask.setId ? V.sets.find(x => x.setId === ask.setId) : (() => { const sh = V.pages.find(p => p.sheetId === ask.sheetId); return sh && sh.setId ? V.sets.find(x => x.setId === sh.setId) : null; })();
    if (!set) throw new Error(ask.setId ? "That set is not one of the run's" : "That sheet is in no set of the run");
    if (ask.kind === "relabel") {
      for (const sh of sheetsOf(V, set).filter(p => !ask.sheetId || ask.setId || p.sheetId === ask.sheetId)) { if (Date.now() > ctx.budgetEnd) return { more: true }; await drawSheet(ctx, V, sh, set); }
      return answer(ctx, ask, { state: "done", doneAt: Date.now() });
    }
    // setFiles: the set's files made again (SetEdit.remakeFiles; the freshness gate was read above)
    for (const sh of sheetsOf(V, set)) if (!labelsReady(V, sh, set) && !set.committedAt) await drawSheet(ctx, V, sh, set);
    await finalize(ctx, V, set, { edited: !!set.committedAt, keepStatus: true });
    return answer(ctx, ask, { state: "done", doneAt: Date.now() });
  } catch (e) {
    if (e && (e.fenced || e.yielded || e.overBudget)) throw e;
    await answer(ctx, ask, { state: "refused", why: String((e && e.message) || e).slice(0, 500), doneAt: Date.now() });
    return { refused: true };
  }
}
async function passUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  // (G5: a person's asks first, answered whether or not a run is open and whatever the open run's rules: each on its own set's / sheet's run's records)
  const run = ctx.run && ctx.run.runId ? ctx.run : null;
  for (const ask of await askList(ctx.db, run ? run.runId : null)) { if (Date.now() > ctx.budgetEnd) return { more: true }; const r = await doAsk(ctx, ask); if (r && r.more) return { more: true }; }
  if (!run || run.releasePolicy !== 2) return undefined;
  // every label an open set's sheet owes (the assembly joined it; the cloud draws its label): the run's sets first, then only the sheets its open sets list
  const sq = await ctx.db.collection("Charm_Nest_Sets").where("runId", "==", run.runId).get(), sets = sq.docs.map(d => Object.assign({ setId: d.id }, d.data()));
  const ids = [...new Set(sets.filter(s => !s.committedAt && !/^superseded/.test(String(s.status || ""))).flatMap(s => s.sheetIds || []))];
  if (!ids.length) return undefined;
  const V = await viewOf(ctx, run, sets.filter(s => !s.committedAt && !/^superseded/.test(String(s.status || ""))).map(s => s.setId), { sets, ids });   // (G5: a sheet of another run in an open set too)
  let drawn = 0;
  for (const set of V.sets.filter(s => !s.committedAt && (s.sheetIds || []).length)) for (const sh of sheetsOf(V, set)) {
    if (labelsReady(V, sh, set)) continue;
    if (Date.now() > ctx.budgetEnd) return { more: true };
    await drawSheet(ctx, V, sh, set); drawn++;
  }
  return drawn ? { drew: drawn } : undefined;
}

/* ── the run's "labels" step (RunCtl.doStep "labels"): behind the freshness gate, each open set's release check and its files, or its deferral in words ── */
async function stepUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run; if (!run || run.releasePolicy !== 2) return undefined;
  const V = await viewOf(ctx, run), G = Core.gate(V), sets = V.sets.filter(s => (s.sheetIds || []).length && !s.committedAt);   // (G5: an open set's sheets of another run, from their own run's records)
  if (!sets.length) return undefined;
  const gate = await ctx.gate({ receipts: [...new Set(sets.flatMap(s => Object.keys(s.orders || {})))] });
  if (!gate.ok) return { wait: "world", why: gate.why.join("; ") };
  // (the cards of the run were drawn at the assembly's last look: the release check reads the same marks)
  G.cardinalApply(run, V.pages.filter(p => p.runId === run.runId && p.outputs && p.persistedDone), G.openSetsOf(run.runId));
  const deferred = {};
  for (const set of sets) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    for (const sh of sheetsOf(V, set)) if (!labelsReady(V, sh, set)) await drawSheet(ctx, V, sh, set);
    const issue = await releaseIssue(ctx, V, G, set);
    if (issue) {
      deferred[set.setId] = issue;
      if (set.status !== "awaiting review") { const r = await ctx.call({ op: "setUpdate", setId: set.setId, patch: { status: "awaiting review" } }, `defer|${set.setId}|${hashOf(issue)}`); if (r && r.status < 400) set.status = "awaiting review"; }
      continue;
    }
    // (a set whose files say what they would say now (its labels, its orders, the orders it holds back) is not written again at every pass: labelsSig, kept with the set)
    const sig = hashOf([(set.labelFiles || []).map(f => [f.path, f.url]), Object.keys(set.orders).sort(), Object.entries(evaluate(set, V.run).held).sort()]);
    if (set.status === "labelled" && set.labels && set.labels.pdf && set.labelsSig === sig) continue;
    await finalize(ctx, V, set, {});
    await ctx.call({ op: "setUpdate", setId: set.setId, patch: { labelsSig: sig, labelsAt: Date.now() } }, `labels-sig|${set.setId}|${sig}`);
  }
  run.deferredSets = deferred;
  await ctx.call({ op: "runPut", run: { runId: run.runId, deferredSets: deferred }, merge: true }, `deferred|${run.runId}|${hashOf(deferred)}`);
  return undefined;
}

module.exports = { passUnit, stepUnit, drawSheet, ownLabel, finalize, releaseIssue, evaluate, labelPlan, labelsReady, pairNotes, fenceCheck, upload, doAsk, withOwnRuns, askRunId };
/* the steps file's entry (_charmNestSteps.js): the pass item "labels" (lane "labels"), and the run's labels step: the automatic Rose Gold cut first (as the page's Gate.flush ran it
   before the labels), only while the lane "rosecut" is the cloud's, then each set's release check and files */
module.exports.units = reg => ({ passItems: [{ name: "labels", lane: "labels", unit: passUnit, runless: true }], labels: [ctx => ((ctx.cloudSteps || []).includes("rosecut") && typeof reg.named.greenAuto === "function" ? reg.named.greenAuto(ctx) : undefined), stepUnit] });
