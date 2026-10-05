/* Does the property harness have teeth? A small reference implementation of issues() (written from the same rules, over the
 * page's ctx {rows, allSheets}) is run clean, which must give ZERO disagreements, and then with one plausible bug at a time
 * (each is a mistake the old code or a quick rewrite could make). Every mutant must be caught by issues-property's comparison.
 *
 *   node tests/charm-nest/issues-mutants.cjs
 * The old code (before round 2) is a real mutant too: it must be caught on a shop shaped like Paul's too. */
'use strict';
const assert = require('node:assert/strict');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const OLD = require('./fixtures/charm-nest-readiness.before-round2.js');
const fs = require('node:fs');
const NEW = require(require('path').join(__dirname, '../../charm-nest-readiness.js'));
const P = require('./issues-property.cjs');
const { paulShop } = require('./issues-paul.cjs');

// (hand: the line is completed by hand as this shape says it: the page's row carries the custom order's record as spec.customDone, the server's record as handDone)
const handOfShape = r => { const c = r.orderId ? r.handDone : r.spec && r.spec.customDone; return !!c && c.state !== 'open' && c.how !== 'sheet'; };
const record = r => r.orderId ? { ...r, hand: handOfShape(r) } : ({ orderId: String(r.order.receiptId), transactionId: r.line.transactionId, state: r.state, quantity: r.spec.quantity, poolIds: r.poolIds, problems: (r.problems || []).map(p => p.kind), sku: r.line.sku, hold: r.hold, changePending: r.changePending, noDesign: r.spec.noDesign, engraveCandidate: r.spec.engraveCandidate, engrave: r.engrave, hand: handOfShape(r) });
const problemKey = l => { const k = l.problems[0].kind || l.problems[0]; return k === 'unmatchedSku' ? (l.sku ? 'unmatched' : 'noSku') : k === 'missingSize' || k === 'blockedSku' ? 'noDesign' : 'unmatched'; };
const lineOf = pid => String(pid).slice(0, String(pid).lastIndexOf('_'));

/** The rules, in the page's terms, with one switch per plausible mistake. */
function refIssues(sheet, ctx, mut = {}) {
  const lines = Object.fromEntries(ctx.rows.map(r => [r.key, record(r)]));
  // the two mutants that read the custom order's record wrongly look at it directly (the shop's own Charm_Custom_Orders record of the line)
  const handNow = (key, l) => {
    if (mut.forgetsHand) return false;
    // (round 8) only the Complete Order button releases a piece: a QR label printed from Custom Orders is no completion, as a quick reading of `how` could make it
    if (mut.printNotHand) { const c = ctx.shop && ctx.shop.customs && ctx.shop.customs[key]; return !!c && c.state !== 'open' && c.how === 'button'; }
    if (mut.reopenIsDone || mut.sheetIsHand) { const c = ctx.shop && ctx.shop.customs && ctx.shop.customs[key]; return !!c && (mut.reopenIsDone || c.state !== 'open') && (mut.sheetIsHand || c.how !== 'sheet'); }
    return l.hand;
  };
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
    const hand = handNow(key, l) && !(l.poolIds || []).length;
    if (!mut.noDesign && (l.noDesign || l.state === 'noDesign') && !hand) continue;
    const q = mut.noQty ? 1 : Math.max(1, +l.quantity || 1), ids = [...new Set(l.poolIds || [])], used = new Set(ids.map(x => +String(x).split('_').pop()));
    const copies = ids.map(pid => ({ pid, copy: +String(pid).split('_').pop() }));
    for (let n = 1, need = q - ids.length; need > 0; n++) if (!used.has(n)) { copies.push({ pid: `${key}_${n}`, copy: n }); used.add(n); need--; }
    // completed by hand and on no sheet: resolved, not a piece (a held one is still a held piece)
    const onSheet = copies.some(c => sheets.some(s => OLD.idsOf(s).includes(c.pid)));
    for (const c of copies.sort((a, b) => a.copy - b.copy)) { if (hand && !onSheet && (mut.handBeatsHold || !(l.hold || l.changePending))) continue; const xs = byOrder.get(oid) || []; xs.push({ l, pid: c.pid, key, copy: c.copy }); byOrder.set(oid, xs); }
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
      else if (mut.archived ? on.some(s => !ready(s)) : on.every(s => !ready(s))) {
        // round 7: a not-ready sheet of the SAME set is the set's wait, not the order's (mutant sameSetWaits: the old rule, which listed it)
        const me = O.effSet(sheet);
        if (mut.sameSetWaits || !(me && on.every(s => O.effSet(s) === me))) why = 'otherSheetNotReady';
      }
      const split = why === 'otherSheetNotReady' && !!O.effSet(sheet) && !(p.l.hold || p.l.changePending) && on.some(s => O.effSet(s) && O.effSet(s) !== O.effSet(sheet));
      // round 8: its not-ready sheet is in no set while this one is in one (the mutant noSetSilent says nothing of it; noSetForSplit calls any other set's sheet "no set")
      const noSet = why === 'otherSheetNotReady' && !split && !!O.effSet(sheet) && !(p.l.hold || p.l.changePending) && !mut.noSetSilent && on.every(s => !O.effSet(s));
      if (why) bad.push({ p, why, on, split, noSet });
    }
    if (bad.length) {
      const sp = bad.find(b => b.split), ns = !sp && bad.find(b => b.noSet), here = sheet.setSeq ? `Set ${sheet.setSeq}` : '', there = sp ? (() => { const o = sp.on.find(s => O.effSet(s) && O.effSet(s) !== O.effSet(sheet)); return o && o.setSeq ? `Set ${o.setSeq}` : ''; })() : '';
      out.push({ step: 'orders', key: bad[0].why, orderId: oid, sheetId: sheet.id, pieceCount: pieces.length, ...(sp ? { split: true, ...(here && there && here !== there ? { sets: [here, there] } : {}) } : ns ? { noSet: true } : {}),
        why: sp && here && there ? `Split between ${here} and ${there}: its other piece` : ns && bad.length === 1 ? `Its other piece is on ${O.labelOf(ns.on[0])}, which is in no set` : 'x',
        pieces: bad.map(b => ({ index: pieces.indexOf(b.p) + 1, lineKey: b.p.key, copy: b.p.copy, poolId: b.p.pid, kind: b.why, sheetLabel: b.on[0] ? O.labelOf(b.on[0]) : null, why: b.noSet ? `On ${O.labelOf(b.on[0])}, in no set and not ready yet` : 'x', ...(b.split ? { split: true } : b.noSet ? { noSet: true } : {}) })) });
    }
  }
  return out;
}

const MUTANTS = {
  clean: {}, sameSetWaits: { sameSetWaits: 1 }, ownReady: { ownReady: 1 }, lineState: { lineState: 1 }, ownPieceOnMultiOrder: { ownPiece: 1 }, noDesignBlocks: { noDesign: 1 }, goneBlocks: { gone: 1 }, ignoreHeld: { ignoreHeld: 1 },
  archivedCounts: { archived: 1 }, problemBeatsNested: { problemBeatsNested: 1 }, ordersListed: { ordersListed: 1 }, ignoresQuantity: { noQty: 1 }, noCompletedBefore: { noCompletedBefore: 1 }, loudCompleted: { completedBeforeLoud: 1 },
  // Paul, round 6 point 1: a piece completed by hand is resolved. The mistakes: forgetting it (the stale run record still says 'unmatched'), letting it beat a hold,
  // reading a reopened custom order, or one sent to the sheets (how 'sheet'), as completed by hand
  handBlocks: { forgetsHand: 1 }, handBeatsHold: { handBeatsHold: 1 }, reopenedStaysResolved: { reopenIsDone: 1 }, sentToSheetsIsHand: { sheetIsHand: 1 },
  // round 8: Print QR label alone releases a piece too (either button, or both); and a wait for a sheet in no set is said as that
  printLeavesItHolding: { printNotHand: 1 }, noSetUnsaid: { noSetSilent: 1 }
};

function caught(name, mut, shops) {
  for (let i = 0; i < shops; i++) {
    const shop = S.materialize(S.makeSpec(500000 + i)), archived = shop.sheets.filter(s => s.archived);
    const impl = { ...NEW, issues: (sheet, ctx) => NEW.issues(sheet, ctx).filter(i => !(i.step === 'orders' && i.key !== 'unverified' && i.orderId)).concat(refIssues(sheet, { ...ctx, archivedToo: archived, shop }, mut)) };
    const d = P.disagreementsWith(impl, shop, ['rows', 'records']);
    if (d.length) return { shop: i, type: d[0].type };
  }
  return null;
}

/** The REAL module with the round-7 rule taken out again (a not-ready sheet of the asking sheet's own set listed as the order's wait, as before): the harness
 *  must catch it too, on the code that ships and not only on the reference. Built from the module's own source so it can never drift from it. */
function realWithOldRule() {
  const file = require('path').join(__dirname, '../../charm-nest-readiness.js'), src = fs.readFileSync(file, 'utf8');
  const target = '!ownSetWait(b,mySet)';
  assert(src.includes(target), 'the same-set rule is where the mutant expects it (charm-nest-readiness.js forSheet)');
  const m = { exports: {} }; new Function('module', 'exports', 'self', src.replace(target, 'true'))(m, m.exports, undefined);
  return m.exports;
}
/** The REAL module with one rule of round 8 taken out again, built from its own source so it can never drift from it. */
function realMutant(target, replacement) {
  const file = require('path').join(__dirname, '../../charm-nest-readiness.js'), src = fs.readFileSync(file, 'utf8');
  assert(src.includes(target), `the rule is where the mutant expects it: ${target}`);
  const m = { exports: {} }; new Function('module', 'exports', 'self', src.replace(target, replacement))(m, m.exports, undefined);
  return m.exports;
}
/** The shipped module with round 7's quiet set-wait entries put back in a sheet's list (round 8, Paul: "only related items to that particular sheet", took them out): one
 *  {step:'laser', key:'waitsOnSheet', quiet:true} entry per mate sheet that keeps the set from being approved, read from setGate as before. Built from the module's own source.
 *  → { R, src }: the patched module and its source (a page can run it). */
function withWaitsBack(src) {
  const anchor = 'const itself=sheet(rec).included && (completedBefore(rec) || sheet(rec).ready);';
  assert(src.includes(anchor), 'the place where round 8 took the set\'s wait out of a sheet\'s list is where the mutant expects it (charm-nest-readiness.js sheetIssues)');
  const put = anchor + "if(ctx.set){const g0=setGate(ctx.set,[rec,...(ctx.sheets || []).filter(m=>!m.archived && (m.id || m.sheetId)!==sid)]);for(const w of g0.blockers)if(w.sheetId!==sid && !g0.sheets.some(x=>x.missing && x.sheetId===w.sheetId))out.push({step:'laser',key:'waitsOnSheet',quiet:true,label:w.sheetLabel,stepKey:w.step,stepLabel:w.stepLabel,why:w.why,counter:w.counter || null,text:w.text,sheetId:sid,sheetLabel:label,open:{type:'sheet',id:w.sheetId}});}";
  const patched = src.replace(anchor, put), m = { exports: {} };
  new Function('module', 'exports', 'self', patched)(m, m.exports, undefined);
  return { R: m.exports, src: patched };
}
function main() {
  const results = {};
  for (const [name, mut] of Object.entries(MUTANTS)) {
    const hit = caught(name, mut, name === 'clean' ? 400 : 700);
    results[name] = hit;
    if (name === 'clean') assert.equal(hit, null, `the clean reference implementation must agree with the oracle: ${JSON.stringify(hit)}`);
    else assert(hit, `mutant "${name}" went undetected by the harness`);
  }
  // the real module with the same-set rule taken out again: caught by the same comparison, and on a shop shaped like Paul's
  const back = realWithOldRule(); let real = null;
  for (let i = 0; i < 400 && !real; i++) { const d = P.disagreementsWith(back, S.materialize(S.makeSpec(500000 + i)), ['rows', 'records', 'pre']); if (d.length) real = { shop: i, type: d[0].type }; }
  assert(real, 'the shipped module with the same-set rule put back went undetected by the harness');
  const paulOld = P.disagreementsWith(back, paulShop(), ['rows', 'records', 'pre']).filter(d => d.type === 'falsePositive' || d.type === 'sameSetWaitListed');
  assert(P.mateChecks(back, paulShop()).some(d => d.type === 'sameSetWaitListed') || paulOld.length, 'putting the same-set wait back is caught on a shop shaped like Paul\'s');
  // round 8, on the code that ships: (1) only a Complete Order press releases a piece (a QR label printed from Custom Orders does not), (2) a wait for a sheet in no
  // set is worded as the plain wait again
  const realMuts = { 'print alone does not release': realMutant("c.state!=='open' && c.how!=='sheet'", "c.state!=='open' && c.how==='button'"), 'no-set wait unsaid': realMutant('const noSetFrom=(b,me)=>!!(', 'const noSetFrom=(b,me)=>false && !!(') };
  const realHits = {};
  for (const [name, impl] of Object.entries(realMuts)) {
    let hit = null; for (let i = 0; i < 400 && !hit; i++) { const d = P.disagreementsWith(impl, S.materialize(S.makeSpec(500000 + i)), ['rows', 'records', 'pre']); if (d.length) hit = { shop: i, type: d[0].type }; }
    assert(hit, `the shipped module with "${name}" went undetected by the harness`); realHits[name] = hit;
  }
  // round 8: the set's wait put back in a sheet's list is caught by the same harness: on a shop shaped like Paul's and on random shops
  const waits = withWaitsBack(fs.readFileSync(require('path').join(__dirname, '../../charm-nest-readiness.js'), 'utf8'));
  assert(P.mateChecks(waits.R, paulShop()).some(d => d.type === 'setWaitListed'), 'putting the set\'s wait back in the list is caught on a shop shaped like Paul\'s');
  let waitHit = null;
  for (let i = 0; i < 300 && !waitHit; i++) { const d = P.mateChecks(waits.R, S.materialize(S.makeSpec(700000 + i))).filter(x => x.type === 'setWaitListed'); if (d.length) waitHit = { shop: i, type: d[0].type }; }
  assert(waitHit, 'the set\'s wait put back in the list went undetected on random shops');
  // the old code, a real mutant: caught on Paul's shop
  const paul = P.oldAgainstOracle(paulShop());
  // (round 7: both sheets are in ONE set, so even the five shared orders are no issue: the old code's 53 listed orders are all false alarms)
  assert(paul.falseAlarms >= 53 && paul.real === 0, 'the old code is caught on a shop shaped like Paul\'s');
  console.log(`PASS: clean reference agrees with the oracle; ${Object.keys(MUTANTS).length - 1} mutants all caught (${Object.entries(results).filter(([k]) => k !== 'clean').map(([k, v]) => `${k}:${v.type}@${v.shop}`).join(', ')}); same-set rule put back in the shipped module caught (${real.type}@${real.shop}); round-8 rules taken out of the shipped module caught (${Object.entries(realHits).map(([k, v]) => `${k}: ${v.type}@${v.shop}`).join(', ')}); the set's wait put back in a sheet's list caught (${waitHit.type}@${waitHit.shop}); old code caught on Paul's shop (${paul.falseAlarms} false alarms)`);
}
if (require.main === module) main();
module.exports = { refIssues, MUTANTS, withWaitsBack };
