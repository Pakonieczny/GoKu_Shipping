/* Does the property harness have teeth? A small reference implementation of issues() (written from the same rules, over the
 * page's ctx {rows, allSheets}) is run clean, which must give ZERO disagreements, and then with one plausible bug at a time
 * (each is a mistake the old code or a quick rewrite could make). Every mutant must be caught by issues-property's comparison.
 *
 *   node tests/charm-nest/issues-mutants.cjs
 * The old code (before round 2) is the thirteenth, real mutant: it must be caught on a shop shaped like Paul's too. */
'use strict';
const assert = require('node:assert/strict');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const OLD = require('./fixtures/charm-nest-readiness.before-round2.js');
const NEW = require(require('path').join(__dirname, '../../charm-nest-readiness.js'));
const P = require('./issues-property.cjs');
const { paulShop } = require('./issues-paul.cjs');

const record = r => r.orderId ? r : ({ orderId: String(r.order.receiptId), transactionId: r.line.transactionId, state: r.state, quantity: r.spec.quantity, poolIds: r.poolIds, problems: (r.problems || []).map(p => p.kind), sku: r.line.sku, hold: r.hold, changePending: r.changePending, noDesign: r.spec.noDesign, engraveCandidate: r.spec.engraveCandidate, engrave: r.engrave });
const problemKey = l => { const k = l.problems[0].kind || l.problems[0]; return k === 'unmatchedSku' ? (l.sku ? 'unmatched' : 'noSku') : k === 'missingSize' || k === 'blockedSku' ? 'noDesign' : 'unmatched'; };
const lineOf = pid => String(pid).slice(0, String(pid).lastIndexOf('_'));

/** The rules, in the page's terms, with one switch per plausible mistake. */
function refIssues(sheet, ctx, mut = {}) {
  const lines = Object.fromEntries(ctx.rows.map(r => [r.key, record(r)]));
  const sheets = mut.archived ? ctx.allSheets.concat(ctx.archivedToo || []) : ctx.allSheets;
  const where = new Map();
  for (const s of sheets) for (const pid of OLD.idsOf(s)) { const xs = where.get(pid) || []; xs.push(s); where.set(pid, xs); }
  const phys = new Map(sheets.map(s => [s.id, OLD.sheet(s, { physicalOnly: true })]));
  const ready = s => +s.laserDoneAt > 0 || phys.get(s.id).ready || (!mut.noCompletedBefore && phys.get(s.id).included && OLD.completedBefore(s));
  if (!mut.noCompletedBefore && OLD.completedBefore(sheet) && !mut.completedBeforeLoud) return [];
  const byOrder = new Map();
  for (const key of Object.keys(lines).sort()) {
    const l = lines[key], oid = String(l.orderId);
    if (!mut.gone && l.state === 'gone') continue;
    if (!mut.noDesign && (l.noDesign || l.state === 'noDesign')) continue;
    const q = mut.noQty ? 1 : Math.max(1, +l.quantity || 1), ids = [...new Set(l.poolIds || [])], used = new Set(ids.map(x => +String(x).split('_').pop()));
    const copies = ids.map(pid => ({ pid, copy: +String(pid).split('_').pop() }));
    for (let n = 1, need = q - ids.length; need > 0; n++) if (!used.has(n)) { copies.push({ pid: `${key}_${n}`, copy: n }); used.add(n); need--; }
    for (const c of copies.sort((a, b) => a.copy - b.copy)) { const xs = byOrder.get(oid) || []; xs.push({ l, pid: c.pid, key, copy: c.copy }); byOrder.set(oid, xs); }
  }
  const out = [];
  for (const [oid, pieces] of byOrder) {
    const mine = pieces.filter(p => p.pid && (where.get(p.pid) || []).some(s => s.id === sheet.id));
    if (mut.ordersListed ? !(sheet.orders || []).includes(oid) : !mine.length) continue;
    if (pieces.length < 2 && !pieces.some(p => !mut.ignoreHeld && (p.l.hold || p.l.changePending))) continue;
    if (mut.ownReady && !phys.get(sheet.id).ready) { out.push({ step: 'orders', key: 'otherSheetNotReady', orderId: oid, sheetId: sheet.id, pieceCount: pieces.length, pieces: [] }); continue; }
    const bad = [];
    for (const p of pieces) {
      if (mine.includes(p) && !(mut.ownPiece && !ready(sheet)) && (mut.ignoreHeld || !(p.l.hold || p.l.changePending))) continue;
      const on = p.pid ? (where.get(p.pid) || []) : [];
      let why = null;
      if (!mut.ignoreHeld && (p.l.hold || p.l.changePending)) why = 'held';
      else if (mut.problemBeatsNested && (p.l.problems || []).length) why = problemKey(p.l);
      else if (mut.lineState && !['written', 'labelled', 'committed'].includes(p.l.state)) why = 'pooled';
      else if (!on.length) why = (p.l.problems || []).length ? problemKey(p.l) : 'pooled';
      else if (mut.archived ? on.some(s => !ready(s)) : on.every(s => !ready(s))) why = 'otherSheetNotReady';
      if (why) bad.push({ p, why, on });
    }
    if (bad.length) out.push({ step: 'orders', key: bad[0].why, orderId: oid, sheetId: sheet.id, pieceCount: pieces.length, pieces: bad.map(b => ({ index: pieces.indexOf(b.p) + 1, lineKey: b.p.key, copy: b.p.copy, poolId: b.p.pid, kind: b.why, sheetLabel: b.on[0] ? O.labelOf(b.on[0]) : null, why: 'x' })) });
  }
  return out;
}

const MUTANTS = {
  clean: {}, ownReady: { ownReady: 1 }, lineState: { lineState: 1 }, ownPieceOnMultiOrder: { ownPiece: 1 }, noDesignBlocks: { noDesign: 1 }, goneBlocks: { gone: 1 }, ignoreHeld: { ignoreHeld: 1 },
  archivedCounts: { archived: 1 }, problemBeatsNested: { problemBeatsNested: 1 }, ordersListed: { ordersListed: 1 }, ignoresQuantity: { noQty: 1 }, noCompletedBefore: { noCompletedBefore: 1 }, loudCompleted: { completedBeforeLoud: 1 }
};

function caught(name, mut, shops) {
  for (let i = 0; i < shops; i++) {
    const shop = S.materialize(S.makeSpec(500000 + i)), archived = shop.sheets.filter(s => s.archived);
    const impl = { ...NEW, issues: (sheet, ctx) => NEW.issues(sheet, ctx).filter(i => !(i.step === 'orders' && i.key !== 'unverified')).concat(refIssues(sheet, { ...ctx, archivedToo: archived }, mut)) };
    const d = P.disagreementsWith(impl, shop, ['rows', 'records']);
    if (d.length) return { shop: i, type: d[0].type };
  }
  return null;
}

function main() {
  const results = {};
  for (const [name, mut] of Object.entries(MUTANTS)) {
    const hit = caught(name, mut, name === 'clean' ? 400 : 700);
    results[name] = hit;
    if (name === 'clean') assert.equal(hit, null, `the clean reference implementation must agree with the oracle: ${JSON.stringify(hit)}`);
    else assert(hit, `mutant "${name}" went undetected by the harness`);
  }
  // the old code, a real mutant: caught on Paul's shop
  const paul = P.oldAgainstOracle(paulShop());
  assert(paul.falseAlarms >= 48 && paul.wrongReason >= 5, 'the old code is caught on a shop shaped like Paul\'s');
  console.log(`PASS: clean reference agrees with the oracle; ${Object.keys(MUTANTS).length - 1} mutants all caught (${Object.entries(results).filter(([k]) => k !== 'clean').map(([k, v]) => `${k}:${v.type}@${v.shop}`).join(', ')}); old code caught on Paul's shop (${paul.falseAlarms} false alarms)`);
}
if (require.main === module) main();
module.exports = { refIssues, MUTANTS };
