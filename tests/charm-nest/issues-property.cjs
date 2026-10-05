/* Property harness: CharmNestReadiness.issues() against the independent oracle (issues-oracle.cjs) over thousands of synthetic
 * shops, and the OLD code (tests/charm-nest/fixtures/charm-nest-readiness.before-round2.js) over a shop shaped like Paul's, to
 * count exactly how many false alarms it showed.
 *
 *   node tests/charm-nest/issues-property.cjs [--shops 2500] [--seed 1] [--budget-ms 25000] [--dual] [--ghost] [--no-lost] [--no-sets] [--no-pieces] [--verbose]
 *       (checks: the issues list in three modes against the oracle, the sheet's own entry, the 'not checked yet' entry, a set's union and laser mates,
 *        explain(), the piece numbers and sheets OrderPieces gives, and that nothing handed to the code under test was changed)
 *   node tests/charm-nest/issues-property.cjs --old          (the old code against the oracle, Paul's shop and random shops; informational)
 *   ISSUES_IMPL=/path/to/charm-nest-readiness.js node ...    (another build of the module)
 * Exit code 1 on any disagreement. A disagreement is printed with a MINIMAL reproducing shop (the shrinker removes orders,
 * lines, sheets and trouble while the disagreement stays). */
'use strict';
const path = require('path');
const assert = require('node:assert/strict');
const O = require('./issues-oracle.cjs');
const S = require('./issues-shop.cjs');
const { paulShop } = require('./issues-paul.cjs');

const NEW = require(process.env.ISSUES_IMPL || path.join(__dirname, '../../charm-nest-readiness.js'));
const OLD = require('./fixtures/charm-nest-readiness.before-round2.js');
const clone = x => JSON.parse(JSON.stringify(x));

/** The sheet records as the page has them from the server: the raw record plus the engraving decisions the server attaches. */
const pageSheets = (shop, R = NEW) => S.memo(shop, 'pageSheets:' + (R === OLD ? 'old' : R === NEW ? 'new' : 'x'), () => pageSheetsFresh(shop, R));
function pageSheetsFresh(shop, R) {
  const rows = S.recordRows(shop), dec = R.decisions(rows);
  return shop.sheets.filter(s => !s.archived).map(s => ({ ...clone(s), engraving: Object.fromEntries(R.idsOf(s).map(id => [id, dec[id] || { needed: true, state: 'unknown', approved: false }])) }));
}
const sortIds = xs => [...new Set(xs)].sort();

/** The OLD gate as the page and the server ran it: orderReports over all lines and sheets, then each sheet's blockers. */
function oldBlocked(shop) {
  const rows = S.recordRows(shop), sheets = pageSheets(shop, OLD), reports = OLD.orderReports(rows, sheets), out = {};
  for (const s of sheets) {
    const rec = { ...s, orderReadiness: Object.fromEntries(OLD.orderIds(s).map(id => [id, reports[id] || { ready: false, why: 'Order readiness has not been verified' }])) };
    out[s.id] = OLD.orderBlockers(rec);
  }
  return out;
}

/** Old code against the oracle: per sheet, shown (old) vs real (oracle). */
function oldAgainstOracle(shop) {
  const t = O.truth(shop), blocked = oldBlocked(shop), res = { sheets: {}, shown: 0, real: 0, falseAlarms: 0, missed: 0, wrongReason: 0 };
  for (const sid of Object.keys(t.sheets)) {
    const shown = blocked[sid] || [], real = O.issueOrders(t, sid), shownIds = sortIds(shown.map(b => b.id));
    const fp = shownIds.filter(id => !real.includes(id)), fn = real.filter(id => !shownIds.includes(id));
    // an issue shown for a real order but for a reason that is not true ("line is pooled" while the piece is on a sheet)
    const wrong = shown.filter(b => real.includes(b.id) && /line is|not every copy|not on a saved sheet/i.test(String(b.why)) && !(t.sheets[sid].orders[b.id].offenders.some(o => o.reasons.has('pooled')))).map(b => b.id);
    res.sheets[sid] = { label: t.sheets[sid].label, orders: shopOrders(shop, sid), shown: shownIds.length, real: real.length, falseAlarms: fp.length, missed: fn.length, wrongReason: wrong.length, fp, fn, wrong };
    res.shown += shownIds.length; res.real += real.length; res.falseAlarms += fp.length; res.missed += fn.length; res.wrongReason += wrong.length;
  }
  return res;
}
const shopOrders = (shop, sid) => new Set(shop.sheets.find(s => s.id === sid).orders).size;

/* ── the new code ─────────────────────────────────────────────────────────────── */
/** The records as op_laserStatus answers them: each sheet carries its own reading of its orders (orderReadiness). */
const serverLike = (shop, R = NEW) => S.memo(shop, 'serverLike:' + (R === NEW ? 'new' : 'x'), () => serverLikeFresh(shop, R));
function serverLikeFresh(shop, R) {
  const rows = S.recordRows(shop), sheets = pageSheets(shop, R), reps = R.orderReports(rows, sheets);
  return sheets.map(s => ({ ...s, orderReadiness: Object.fromEntries(R.orderIds(s).map(id => [id, (reps[id] ? R.forSheet(reps[id], s.id) : null) || { ready: false, why: 'Order readiness has not been verified' }])) }));
}
/** What the code under test shows for each live sheet, three ways: reading the page's rows (rows / records shape) and
 *  reading the record the server answered (pre). All steps, so the sheet's own blocker is there too. */
function newIssues(shop, mode = 'rows', R = NEW) {
  const out = {};
  if (mode === 'pre') { for (const s of serverLike(shop, R)) out[s.id] = R.issues(s, {}); return out; }
  const rows = mode === 'rows' ? S.uiRows(shop) : S.recordRows(shop), all = pageSheets(shop, R);
  for (const s of all) out[s.id] = R.issues(s, { rows, allSheets: all });
  return out;
}
const orderEntries = list => (list || []).filter(i => i && i.step === 'orders' && i.key !== 'unverified');
const unverifiedEntries = list => (list || []).filter(i => i && i.step === 'orders' && i.key === 'unverified');
const ownEntries = list => (list || []).filter(i => i && !i.orderId && i.step !== 'laser' && i.key !== 'unverified');
const num = v => (+v > 0 ? +v : 0);

/** Disagreements between one shop's issues() answers and the oracle. [] when they agree on everything checked. */
const disagreements = (shop, modes) => disagreementsWith(NEW, shop, modes);
function disagreementsWith(R, shop, modes = ['rows', 'records', 'pre']) {
  const out = [];
  for (const mode of modes) {
    let got;
    try { got = newIssues(shop, mode, R); } catch (e) { return [{ type: 'throws', mode, detail: e.stack.split('\n').slice(0, 3).join(' | ') }]; }
    out.push(...listChecks(R, shop, mode, got));
  }
  return out;
}
/** The comparison itself: got = { sheetId: the issues list shown for that sheet }, whatever produced it (issues() in any mode, the page's feed, the panel). */
function listChecks(R, shop, mode, got) {
  const t = O.truth(shop), out = [], raw = new Map(shop.sheets.filter(s => !s.archived).map(s => [s.id, s]));
  {
    for (const sid of Object.keys(t.sheets)) {
      const real = t.sheets[sid].orders, entries = orderEntries(got[sid]), ids = entries.map(e => String(e.orderId));
      if (new Set(ids).size !== ids.length) out.push({ type: 'duplicate', mode, sheet: sid, detail: ids.filter((x, i) => ids.indexOf(x) !== i).join(',') });
      for (const e of entries) {
        const id = String(e.orderId), r = real[id];
        if (!r) { out.push({ type: 'falsePositive', mode, sheet: sid, order: id, detail: `${e.key} ${JSON.stringify((e.pieces || []).map(p => [p.index, p.sheetLabel, p.why]))}` }); continue; }
        if (e.sheetId != null && e.sheetId !== sid) out.push({ type: 'wrongSheet', mode, sheet: sid, order: id, detail: `entry says sheet ${e.sheetId}` });
        const kinds = new Set(r.offenders.flatMap(o => [...o.reasons]));
        if (!kinds.has(e.key)) out.push({ type: 'wrongKey', mode, sheet: sid, order: id, detail: `shows ${e.key}; the real reasons are ${[...kinds].join('/')}` });
        if (e.pieceCount != null && e.pieceCount !== r.livePieces) out.push({ type: 'wrongCount', mode, sheet: sid, order: id, detail: `pieceCount ${e.pieceCount}, oracle ${r.livePieces}` });
        out.push(...pieceChecks(e, r, t, mode, sid, id));
      }
      for (const id of Object.keys(real)) if (!ids.includes(id)) out.push({ type: 'falseNegative', mode, sheet: sid, order: id, detail: JSON.stringify(real[id].offenders.map(o => [o.lineKey, o.copy, [...o.reasons], o.sheets.map(s => s.label)])) });
      out.push(...ownChecks(got[sid], raw.get(sid), t, mode, sid));
      out.push(...ghostChecks(got[sid], t.sheets[sid], raw.get(sid), mode, sid));
    }
  }
  return out;
}
/** Orders the sheet lists that no line record exists for: ONE calm 'not checked yet' entry for the sheet, naming exactly those orders. */
function ghostChecks(list, rec, rawSheet, mode, sid) {
  const un = unverifiedEntries(list), out = [];
  if (num(rawSheet.laserDoneAt) || O.completedBefore(rawSheet)) { if (un.length) out.push({ type: 'unverifiedOnSilentSheet', mode, sheet: sid }); return out; }
  if (!rec.ghosts.length && un.length) out.push({ type: 'unverifiedFalsePositive', mode, sheet: sid, detail: JSON.stringify(un[0]).slice(0, 160) });
  if (rec.ghosts.length && !un.length) out.push({ type: 'unverifiedMissing', mode, sheet: sid, detail: rec.ghosts.join(',') });
  if (un.length > 1) out.push({ type: 'unverifiedDuplicate', mode, sheet: sid });
  if (un.length === 1 && rec.ghosts.length && JSON.stringify([...(un[0].orderIds || [])].sort()) !== JSON.stringify(rec.ghosts)) out.push({ type: 'unverifiedWrongOrders', mode, sheet: sid, detail: `got ${JSON.stringify(un[0].orderIds)} want ${JSON.stringify(rec.ghosts)}` });
  return out;
}
/** The sheet's own entry: present exactly when one of its own steps is behind, and naming a step that really is. */
function ownChecks(list, rawSheet, t, mode, sid) {
  if (!list) return [];
  const own = ownEntries(list), want = O.ownTruth(rawSheet, t.lines), out = [];
  if (num(rawSheet.laserDoneAt)) { if (list.length) out.push({ type: 'doneSheetHasIssues', mode, sheet: sid, detail: JSON.stringify(list).slice(0, 160) }); return out; }
  if (own.length > 1) out.push({ type: 'duplicateOwn', mode, sheet: sid, detail: own.map(o => o.key).join(',') });
  if (!want.length && own.length) out.push({ type: 'falsePositiveOwn', mode, sheet: sid, detail: `${own[0].step}/${own[0].key}` });
  if (want.length && !own.length) out.push({ type: 'falseNegativeOwn', mode, sheet: sid, detail: `true: ${want.join(',')}` });
  if (own.length && want.length && !want.includes(own[0].key)) out.push({ type: 'wrongOwnKey', mode, sheet: sid, detail: `${own[0].key}, true: ${want.join(',')}` });
  if (own.length && O.OWN_STEP[own[0].key] && own[0].step !== O.OWN_STEP[own[0].key]) out.push({ type: 'wrongOwnStep', mode, sheet: sid, detail: `${own[0].step}/${own[0].key}` });
  for (const i of list) if (i.step === 'engraving' && i.orderId) out.push({ type: 'engravingRow', mode, sheet: sid, detail: JSON.stringify(i).slice(0, 120) });
  return out;
}
/** The pieces an entry lists must be exactly the blocking pieces, each with the sheet it is really on. */
function pieceChecks(e, r, t, mode, sid, id) {
  const out = [], pcs = e.pieces || [];
  if (!pcs.length) return [{ type: 'noPieces', mode, sheet: sid, order: id, detail: `issue ${e.key} lists no piece` }];
  const off = new Map(r.offenders.map(o => [o.poolId, o])), seen = new Set();
  for (const p of pcs) {
    const o = off.get(p.poolId);
    if (!o) { out.push({ type: 'wrongPiece', mode, sheet: sid, order: id, detail: `lists piece ${p.poolId} (${p.kind}, on ${p.sheetLabel}) which is not blocking` }); continue; }
    seen.add(p.poolId);
    if (p.index != null && p.index !== o.index) out.push({ type: 'wrongPieceIndex', mode, sheet: sid, order: id, detail: `${p.poolId}: index ${p.index}, oracle ${o.index}` });
    const wantLabels = o.sheets.map(s => s.label);
    if (o.sheets.length ? !wantLabels.includes(p.sheetLabel) : p.sheetLabel != null) out.push({ type: 'wrongSheetName', mode, sheet: sid, order: id, detail: `piece ${p.poolId} on ${JSON.stringify(wantLabels)} but entry says ${p.sheetLabel}` });
    if (p.kind && !o.reasons.has(p.kind)) out.push({ type: 'wrongPieceKind', mode, sheet: sid, order: id, detail: `piece ${p.poolId} shown as ${p.kind}; real: ${[...o.reasons].join('/')}` });
  }
  for (const k of off.keys()) if (!seen.has(k)) out.push({ type: 'missingPiece', mode, sheet: sid, order: id, detail: `blocking piece ${k} not listed` });
  return out;
}

/** explain() (the checklist, the step rail) is built on the same truth: its Order check step and its "ready" are the oracle's. */
function explainChecks(R, shop) {
  const t = O.truth(shop), out = [], rows = S.recordRows(shop), pre = serverLike(shop, R);
  for (const rec of pre) {
    const tr = t.sheets[rec.id];
    let e;
    try { e = R.explain(rec, { rows }); } catch (ex) { out.push({ type: 'throws', mode: 'explain', sheet: rec.id, detail: ex.message }); continue; }
    if (e.ready !== !!tr.laserReady && !num(rec.laserDoneAt)) out.push({ type: 'explainReady', mode: 'explain', sheet: rec.id, detail: `explain ready ${e.ready}, oracle ${tr.laserReady}` });
    if (num(rec.laserDoneAt) || tr.completedBefore) continue;
    const st = e.steps.find(x => x.key === 'orders'), want = Object.keys(tr.orders).concat(tr.ghosts).sort();
    const have = [...new Set(st.items.filter(i => i.kind === 'order').map(i => String(i.id)))].sort();
    if (st.state === 'done' && want.length) out.push({ type: 'explainOrdersDone', mode: 'explain', sheet: rec.id, detail: `says done, oracle: ${want.join(',')}` });
    if (st.state !== 'done' && !want.length) out.push({ type: 'explainOrdersFalse', mode: 'explain', sheet: rec.id, detail: `${st.state}: ${st.detail}` });
    if (JSON.stringify(have) !== JSON.stringify(want)) out.push({ type: 'explainOrderItems', mode: 'explain', sheet: rec.id, detail: `items ${JSON.stringify(have)} oracle ${JSON.stringify(want)}` });
  }
  return out;
}

/** OrderPieces (the order window's Sheet tab, the Overview words, the piece sheet buttons) must say the same as the oracle: which sheet
 *  holds each piece, which pieces are on none, and the same "piece N" numbers the issues list uses. */
const OP = require('../../charm-nest-order-pieces.js');
// (the lines as the page hands them over: its rows, so a piece completed by hand carries the custom order's record as spec.customDone, as OrderPieces' lineOfRow reads it)
const opLine = r => ({ key: r.key, transactionId: r.line.transactionId, sku: r.line.sku || '', title: '', material: r.metal, quantity: r.spec.quantity, state: r.state, poolIds: (r.poolIds || []).slice(), hold: r.hold || null, changePending: !!r.changePending, problems: (r.problems || []).map(p => ({ kind: p.kind })), noDesign: !!r.spec.noDesign, spec: { noDesign: !!r.spec.noDesign, quantity: r.spec.quantity, customDone: r.spec.customDone || null }, reason: r.reason || '' });
function orderPiecesChecks(shop) {
  const t = O.truth(shop), out = [], byOrder = new Map(), sheetById = new Map(shop.sheets.filter(s => !s.archived).map(s => [s.id, s]));
  for (const r of S.uiRows(shop)) { const oid = String(r.order.receiptId); (byOrder.get(oid) || byOrder.set(oid, []).get(oid)).push(opLine(r)); }
  for (const [oid, ls] of byOrder) {
    let got;
    try { got = OP.resolve({ orderId: oid, lines: ls, pools: shop.pool, sheets: shop.sheets, sheetsKnown: true }); } catch (ex) { out.push({ type: 'throws', mode: 'orderPieces', sheet: '-', order: oid, detail: ex.message }); continue; }
    const live = got.filter(p => !p.gone && !p.noDesign), want = t.pieces.get(oid) || [], have = new Map(live.map(p => [p.poolId, p]));
    for (const w of want) {
      const g = have.get(w.poolId);
      if (!g) { out.push({ type: 'opMissingPiece', mode: 'orderPieces', sheet: '-', order: oid, detail: w.poolId }); continue; }
      if (g.nested !== (w.sheets.length > 0)) out.push({ type: 'opNested', mode: 'orderPieces', sheet: '-', order: oid, detail: `${w.poolId}: OrderPieces nested=${g.nested} (${g.sheetLabel}), oracle sheets ${JSON.stringify(w.sheets)}` });
      else if (g.nested && !w.sheets.includes(g.sheetId)) out.push({ type: 'opSheet', mode: 'orderPieces', sheet: '-', order: oid, detail: `${w.poolId}: OrderPieces says ${g.sheetId}, oracle ${JSON.stringify(w.sheets)}` });
      else if (g.nested && g.sheetLabel !== O.labelOf(sheetById.get(g.sheetId))) out.push({ type: 'opSheetName', mode: 'orderPieces', sheet: '-', order: oid, detail: `${w.poolId}: ${g.sheetLabel} vs ${O.labelOf(sheetById.get(g.sheetId))}` });
      if (g.index !== w.index) out.push({ type: 'opIndex', mode: 'orderPieces', sheet: '-', order: oid, detail: `${w.poolId}: OrderPieces piece ${g.index}, oracle ${w.index}` });
    }
    const wantIds = new Set(want.map(w => w.poolId));
    for (const g of live) if (!wantIds.has(g.poolId)) out.push({ type: 'opExtraPiece', mode: 'orderPieces', sheet: '-', order: oid, detail: `${g.poolId} (${g.state}, ${g.sheetLabel})` });
  }
  return out;
}

/** A set's issues are its member sheets' (one entry per sheet and order, no duplicates, nothing for a cut member). */
function setChecks(R, shop) {
  const t = O.truth(shop), out = [], rows = S.recordRows(shop), all = pageSheets(shop, R), pre = serverLike(shop, R), live = new Map(shop.sheets.filter(s => !s.archived).map(s => [s.id, s]));
  for (const set of shop.sets) {
    const members = set.sheetIds.filter(id => live.has(id));
    for (const mode of ['rows', 'pre']) {
      let got;
      try { got = R.issues(set, mode === 'rows' ? { rows, allSheets: all } : { sheets: pre.filter(s => set.sheetIds.includes(s.id)) }); } catch (e) { out.push({ type: 'throws', mode: 'set-' + mode, detail: e.message }); continue; }
      const want = [];
      for (const id of members) {
        const s = live.get(id); if (num(s.laserDoneAt)) continue;
        for (const o of Object.keys(t.sheets[id].orders)) want.push(`orders|${id}|${o}`);
        if (t.sheets[id].ghosts.length && !t.sheets[id].completedBefore) want.push(`orders|${id}|unverified`);
        if (O.ownTruth(s, t.lines).length) want.push(`own|${id}`);
      }
      // an own entry may name any true key: compare step-less (sheet only)
      const haveN = got.map(i => (i.step === 'orders' ? `orders|${i.sheetId}|${i.key === 'unverified' ? 'unverified' : i.orderId}` : `own|${i.sheetId}`)).sort(), wantN = want.sort();
      if (JSON.stringify(haveN) !== JSON.stringify(wantN)) out.push({ type: 'setUnion', mode: 'set-' + mode, set: set.setId, detail: `got ${JSON.stringify(haveN).slice(0, 300)} want ${JSON.stringify(wantN).slice(0, 300)}` });
    }
  }
  return out;
}
/** A sheet in a set, asked for with its set: a mate that is not laser-ready is a 'waitsOnSheet' entry, once, and only for a sheet that is itself ready. */
function mateChecks(R, shop) {
  const t = O.truth(shop), out = [], rows = S.recordRows(shop), all = pageSheets(shop, R), pre = serverLike(shop, R), live = new Map(shop.sheets.filter(s => !s.archived).map(s => [s.id, s]));
  for (const set of shop.sets) {
    const ids = set.sheetIds.filter(id => live.has(id));
    for (const mode of ['rows', 'pre']) for (const id of ids) {
      const x = live.get(id); if (num(x.laserDoneAt) || x.draft || x.solidIncluded === false) continue;
      const members = mode === 'rows' ? all.filter(s => ids.includes(s.id)) : pre.filter(s => ids.includes(s.id));
      const subject = mode === 'rows' ? all.find(s => s.id === id) : pre.find(s => s.id === id);
      let got;
      try { got = R.issues(subject, mode === 'rows' ? { rows, allSheets: all, set, sheets: members } : { set, sheets: members }).filter(i => i.step === 'laser' && i.key === 'waitsOnSheet'); } catch (e) { out.push({ type: 'throws', mode: 'mates-' + mode, detail: e.message }); continue; }
      const ts = t.sheets[id], p = ts.physical, itself = ts.included && (ts.completedBefore || (p.layout && p.front && p.approval && p.backs && p.qr && !Object.keys(ts.orders).length && !ts.ghosts.length));
      const want = itself ? ids.filter(m => m !== id && !t.sheets[m].laserReady).sort() : [];
      const have = got.map(i => i.open && i.open.id).sort();
      if (JSON.stringify(have) !== JSON.stringify(want)) out.push({ type: 'setMates', mode: 'mates-' + mode, sheet: id, detail: `got ${JSON.stringify(have)} want ${JSON.stringify(want)}` });
    }
  }
  return out;
}

/* ── runner ─────────────────────────────────────────────────────────────────────── */
function argv(name, dflt) { const i = process.argv.indexOf('--' + name); if (i < 0) return dflt; const v = process.argv[i + 1]; return v == null || v.startsWith('--') ? true : isNaN(+v) ? v : +v; }
const allChecks = shop => {
  const out = disagreements(shop).concat(argv('no-sets', false) ? [] : setChecks(NEW, shop).concat(mateChecks(NEW, shop), explainChecks(NEW, shop))).concat(argv('no-pieces', false) ? [] : orderPiecesChecks(shop));
  for (const name of S.untouched(shop)) out.push({ type: 'mutatesInput', mode: 'all', sheet: '-', detail: `${name} was changed by the code under test` });
  return out;
};
function runProperty() {
  const shops = argv('shops', 2500), seed0 = argv('seed', 1), budget = argv('budget-ms', 25000), dual = !!argv('dual', false), noLost = !!argv('no-lost', false), ghost = !!argv('ghost', false), t0 = Date.now(), counts = {};
  let ran = 0, bad = 0, sheets = 0, issuesSeen = 0, expected = 0;
  for (let i = 0; i < shops && Date.now() - t0 < budget; i++, ran++) {
    const spec = S.makeSpec(seed0 * 100003 + i, { dual, noLost, ghost }), shop = S.materialize(spec), d = allChecks(shop);
    const tr = O.truth(shop);
    for (const sid of Object.keys(tr.sheets)) { sheets++; expected += Object.keys(tr.sheets[sid].orders).length; }
    if (!d.length) continue;
    bad++;
    for (const x of d) counts[x.type] = (counts[x.type] || 0) + 1;
    if (bad <= 3 || argv('verbose', false)) {
      const min = S.shrink(spec, sp => allChecks(S.materialize(sp)).length > 0), md = allChecks(S.materialize(min));
      console.log(`\nDISAGREEMENT (seed ${spec.seed}, shrunk to ${min.orders.length} orders / ${min.sheets.length} sheets):\n${S.describe(min)}\n  -> ${md.slice(0, 4).map(x => `${x.type}[${x.mode}] sheet ${x.sheet} order ${x.order || ''} ${x.detail}`).join('\n     ')}`);
    }
  }
  console.log(`${bad ? 'FAIL' : 'PASS'}: ${ran} shops, ${sheets} sheets, ${expected} real order issues expected, ${bad} shops disagree ${JSON.stringify(counts)} in ${Date.now() - t0} ms`);
  return bad === 0;
}

function runOld() {
  const p = oldAgainstOracle(paulShop());
  console.log('OLD code on a shop shaped like Paul\'s (67 orders GF Sheet 1, 48 on SS Sheet 1, 5 multi-piece orders over both):');
  for (const [sid, r] of Object.entries(p.sheets)) console.log(`  ${r.label}: ${r.orders} orders, old code flags ${r.shown} (false alarms ${r.falseAlarms}, real ${r.real}, real but with a wrong reason ${r.wrongReason}, missed ${r.missed})`);
  let shops = 0, shown = 0, real = 0, fa = 0, fn = 0, wr = 0;
  for (let i = 0; i < 600; i++) { const x = oldAgainstOracle(S.materialize(S.makeSpec(900000 + i))); shops++; shown += x.shown; real += x.real; fa += x.falseAlarms; fn += x.missed; wr += x.wrongReason; }
  console.log(`  random shops (${shops}): old code flagged ${shown} (sheet, order) pairs; real ${real}; false alarms ${fa} (${(100 * fa / Math.max(1, shown)).toFixed(1)}% of what it showed); real but not shown ${fn}; wrong reason ${wr}`);
}

if (require.main === module) {
  if (argv('old', false)) runOld();
  else {
    if (typeof NEW.issues !== 'function') { console.log('SKIP: CharmNestReadiness.issues() is not in this build yet'); process.exit(0); }
    process.exitCode = runProperty() ? 0 : 1;
  }
}
module.exports = { pageSheets, serverLike, oldBlocked, oldAgainstOracle, newIssues, disagreements, disagreementsWith, listChecks, setChecks, mateChecks, explainChecks, orderPiecesChecks, allChecks, runProperty };
