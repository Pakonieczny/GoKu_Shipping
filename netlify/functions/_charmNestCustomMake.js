/*  netlify/functions/_charmNestCustomMake.js — CUSTOMSEND (Paul, 21 Nov 2026: Send to Sheet and Monogram Approve on a staff computer in mode "on"). A card's sent designs made into
 *  pieces BY THE CLOUD: the port of the page's CustomSheet.read / prepare (charm-nest-bridge.js) for the cloud's piece maker (charm-nest-pool-make.js makePoolRead calls
 *  H.CustomSheet().prepare for a line sent from its card). Each design is read from the cloud's own Storage (the .pdf the page saved, charmnest/custom/<order>/...), traced with the
 *  page's own charm-nest-pdf.js (the cloud's canvas), and each copy the send assigned to a line becomes a pool row of that line under the line's own pool ids (O.poolId), exactly as
 *  the page made them: custom, its file's path and the hash of its piece, its metal the one picked for its file, an earring pair's ears (the Right of one body twice its mirror).
 *  Only a send the cloud holds as SENT (Charm_Custom_Sheet phase "sent") is made; a recorded press still pending is left alone (as the page waits for the press to lift).
 *  Nothing here calls Etsy, a URL or an op: bytes come from the bucket (the host's bytesAt), everything else from the record.                                                   */
"use strict";
const O = require("../../charm-nest-orders.js");
const Pair = require("../../charm-nest-pair.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");

const ACCEPT = /\.(ai|dxf|pdf)$/i;
const LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };

/**
 * create(o) → { sentOf(row), prepare(row, run) }
 *   o: { sends (Map lineKey → the card's Charm_Custom_Sheet record), pdf () → CharmNestPDF, bytes (path) → Uint8Array, settings, stockFor (metal) → { wPt, hPt } }
 */
function create(o) {
  const sends = o.sends instanceof Map ? o.sends : new Map(), settings = o.settings || {};
  const geo = new Map(), sources = new Map();
  const sentOf = row => { const e = row && sends.get(row.key); return e && e.phase === "sent" && e.sent && e.sent.lines ? e : null; };
  const linesOf = e => (e && e.sent && e.sent.lines) || {};
  /** the page's read(F): the design's PDF, traced as a master's is */
  function read(F) {
    if (geo.has(F.id)) return geo.get(F.id);
    const task = (async () => {
      const path = F.cloud && F.cloud.path; if (!path) throw new Error(`${F.name} is not saved in the cloud: send it again from a computer that has it`);
      const bytes = await o.bytes(path), P = o.pdf();
      const parsed = await P.parseSource(bytes, String(F.name || "design").replace(ACCEPT, "") + ".pdf");
      const g = await (P.groupCharmsAsync || P.groupCharms)(parsed, { minPt: +settings.minPt || 6 });
      if (!g.charms.length) throw new Error(`${F.name}: no closed cut outline in it`);
      for (const m of g.orphans || []) {
        if (!m.bbox) continue;
        const c = g.charms.find(c => { const b = c.bbox, pad = 2; return m.bbox[0] >= b[0] - pad && m.bbox[1] >= b[1] - pad && m.bbox[2] <= b[2] + pad && m.bbox[3] <= b[3] + pad; });
        if (c && !c.members.includes(m)) c.members.push(m);
      }
      for (const c of g.charms) { const r = P.integrateRings(c); if (r.left.length) throw new Error(`${F.name}: a hoop could not join its piece — ${r.left[0]}`); }
      await P.buildSilhouettes(parsed, g.charms, +settings.silhouetteRes || 6);
      const charms = g.charms.filter(c => !c.open);
      if (!charms.length) throw new Error(`${F.name}: its outlines are not closed`);
      return { parsed, g, charms };
    })();
    geo.set(F.id, task); task.catch(() => geo.delete(F.id));
    return task;
  }
  async function sourceOf(e, F) {
    const id = "cust:" + String(F.hash || F.id).slice(0, 16);
    if (sources.has(id)) return sources.get(id);
    const { parsed, charms } = await read(F);
    const src = { id, parsed, charms };
    charms.forEach((c, i) => Object.assign(c, { id: id + ":" + i, sourceId: id, index: i, name: F.name, sku: "CUSTOM", excluded: false, engravable: false, backKeepOut: null, custom: true }));
    sources.set(id, src); return src;
  }
  const mirrors = new Map();
  function mirroredOf(src, i) {
    const k = src.id + ":" + i; if (mirrors.has(k)) return mirrors.get(k);
    const t = (async () => {
      const base = src.charms[i], geoM = Pair.pieceGeometry ? Pair.pieceGeometry(base, { side: "R", bodyIndex: 0, mirror: true }) : null;
      if (!geoM || geoM === base) throw new Error("the mirrored (right-hand) piece cannot be made");
      const made = Object.assign({}, base, geoM, { id: `${src.id}:${i}m`, mirror: true }); delete made.frontAt; made.pinned = null;
      await o.pdf().buildSilhouettes(src.parsed, [made], +settings.silhouetteRes || 6);
      return made;
    })(); mirrors.set(k, t); t.catch(() => mirrors.delete(k)); return t;
  }
  const pairLine = row => Object.assign({}, row.line, { receiptId: row.order.receiptId, transactionId: row.line.transactionId, quantity: row.spec && row.spec.quantity, form: row.spec && row.spec.form, spec: row.spec });
  function pairPlan(row) { try { const per = Pair.piecesFor(pairLine(row), null); return per.length >= 2 && per.length % 2 === 0 && per.every(pc => pc.side === "L" || pc.side === "R") ? per : null; } catch (_) { return null; } }
  async function earsOf(row, e, mine) {
    const plan = pairPlan(row); if (!plan || plan.length !== mine.length) return null;
    const out = new Array(mine.length).fill(null);
    for (let u = 0; u < mine.length; u += 2) {
      const a = mine[u], b = mine[u + 1];
      if (a.f === b.f && a.i === b.i) { out[u] = { side: "L", mirror: false, bodyIndex: 0 }; out[u + 1] = { side: "R", mirror: true, bodyIndex: 0 }; continue; }
      if (a.f === b.f) {
        const F = e.files.find(x => x.id === a.f), cs = F ? (await read(F)).charms : null, ca = cs && cs[a.i], cb = cs && cs[b.i];
        if (ca && cb && ca.bbox && cb.bbox) { const aLeft = (ca.bbox[0] + ca.bbox[2]) <= (cb.bbox[0] + cb.bbox[2]); out[u] = { side: aLeft ? "L" : "R", mirror: false, bodyIndex: aLeft ? 0 : 1 }; out[u + 1] = { side: aLeft ? "R" : "L", mirror: false, bodyIndex: aLeft ? 1 : 0 }; continue; }
      }
      out[u] = { side: "L", mirror: false, bodyIndex: 0, assumed: true }; out[u + 1] = { side: "R", mirror: false, bodyIndex: 1, assumed: true };
    }
    return out;
  }
  function tooBig(F) {
    if (!F.metal || !F.maxPt || !o.stockFor) return false;
    const st = o.stockFor(F.metal), ins = +settings.insetPt || 0;
    return F.maxAreaPt2 > (st.wPt - 2 * ins) * (st.hPt - 2 * ins) * (+settings.maxFill || 0.8) || F.minPt > Math.max(st.wPt, st.hPt) - 2 * ins;
  }
  /** the page's prepare(row, run): a sent line's pieces from the card's files, each on its own metal */
  async function prepare(row, run) {
    const e = sentOf(row); if (!e) return null;
    const mine = linesOf(e)[row.key] || [];
    if (!mine.length) { row.state = "noDesign"; row.reason = "made with its order's other piece"; return null; }
    const sp = row.spec || {}, rid = row.order.receiptId, pools = [], charms = [];
    const ears = await earsOf(row, e, mine).catch(() => null), gk = Pair.groupKey ? Pair.groupKey(pairLine(row)) : `${rid}:${row.line.transactionId}`;
    for (const [n, pc] of mine.entries()) {
      if (!pc || pc.removed) continue;
      const F = (e.files || []).find(x => x.id === pc.f); if (!F) throw new Error("a design sent for it is missing");
      if (tooBig(F)) { row.state = "oversize"; row.reason = `${F.name} does not fit the ${LABEL[F.metal] || F.metal} plate`; return null; }
      const src = await sourceOf(e, F), ear = ears && ears[n], base = ear && ear.mirror ? await mirroredOf(src, pc.i) : src.charms[pc.i];
      if (!base) throw new Error(`${F.name} has no piece ${pc.i + 1}`);
      const fields = ear ? PoolPieces.fieldsOf({ side: ear.side, mirror: ear.mirror, bodyIndex: ear.bodyIndex, groupKey: gk, groupSize: mine.length }) : null;
      const copy = n + 1, poolId = O.poolId(row.order, row.line, copy), name = String(F.name || "").replace(ACCEPT, "");
      const c = Object.assign({}, base, { id: `${src.id}:${poolId}`, pinned: null, name: `${rid} · Custom · ${name}${mine.length > 1 ? ` · ${copy}/${mine.length}` : ""}`, custom: true, customCk: e.ck, order: rid,
        poolId, metal: F.metal, lineKey: row.key, customIndex: pc.i, excluded: false, engravable: false, backKeepOut: null });
      const g = { widthPt: Math.round((+base.widthPt || 0) * 100) / 100, heightPt: Math.round((+base.heightPt || 0) * 100) / 100, areaPt2: Math.round((+base.areaPt2 || 0) * 10) / 10 };
      // (MULTICHARM, 25 Nov 2026: customIndex, which charm of its file the piece is, as the page writes it: a piece made again is cut from exactly that charm, charm-nest-pool-make.js ensureBase)
      pools.push(Object.assign({ poolId, runId: run ? run.runId : null, setId: run ? run.setId || null : null, sheetId: null, orderId: rid, orderDate: +row.order.createTs || 0, arrivedAt: row.arrivedAt || 0 }, +row.frontAt > 0 ? { frontAt: +row.frontAt } : {},
        { transactionId: row.line.transactionId, sku: sp.designSku || row.line.sku || "CUSTOM", material: F.metal, size: null, form: sp.form || null, chain: sp.chain || null, copy, quantity: mine.length, charmHash: c.hash || null, masterHash: null,
          aiPath: (F.cloud && F.cloud.path) || null, engrave: false, state: "ready", lineKey: row.key, updateTs: row.order.updateTs || null, custom: true, customFile: F.name, customCk: e.ck, customIndex: pc.i }, g.areaPt2 > 0 && g.widthPt > 0 && g.heightPt > 0 ? g : {}, fields || {}));
      if (fields) Object.assign(c, fields);
      charms.push(c);
    }
    if (!charms.length) { row.state = "noDesign"; row.reason = "its sent designs were removed from the sheets"; return null; }
    row.engrave = Object.assign({}, row.engrave || {}, { needed: false, state: "none", approved: true });
    return { sp: Object.assign({}, sp, { material: charms[0].metal, quantity: charms.length, designSku: sp.designSku || row.line.sku || "CUSTOM" }), pools, charms, custom: true };
  }
  return { sentOf, prepare, read };
}

module.exports = { create };
