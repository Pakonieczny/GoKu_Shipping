// RGLINK (Paul, 23 Nov 2026: "The rose gold sheet should keep on nesting orders until Either: 1. The GF sheet that has the linked order is ready to be put into a [set] OR 2. another multi
// piece mixed order arrives to the GF draft sheet and forces the Green Cut line to occure so as to not connect 2 GF or SS sheets to a single RG sheet" · "a rose gold sheet that doesn't already
// have any existing links with other colour sheets should first nest the charm that links it to another coloured sheet and then add the green cut line, not before the part is nested" ·
// "the RG had its own path. Do not fuck with [10K / 14K]"). Replaces RGFILL (an open RG draft took any order and mixed two sets' pieces before any line).
// At 00:07 order 4244381926 (its Gold Filled pieces on GF Draft 21) came while RG Draft 1 (d-bfaa108fb9d28aa7) was tied to GF Draft 20 by order 4243151567. The fixture is the run as the
// cloud saw it then (tests/charm-nest/fixtures/rg-fill-open.json: the records, read only).
//   1  the second link: order 4244381926 does not go on RG Draft 1 and opens no new draft: it waits while RG Draft 1's green line is cut (cut: "link"), on RG Draft 1
//   2  an RG-only order goes on RG Draft 1 (no cut); an order of RG Draft 1's own GF sheet (GF Draft 20) goes on it too
//   3  the first link: RG Draft 1 with no GF / SS link takes order 4244381926 (nested first, nothing cut before)
//   4  a draft owing its link cut takes nothing and no RG line opens a new draft meanwhile (it waits for the cut); another open RG draft that keeps the rule is used
//   5  RG Draft 1 cut: the order goes on a new draft on the LEFTOVER of RG Draft 1's own plate (its green line's), not a fresh plate
//   6  what a Rose Gold nest passes on keeps the rule: never onto a draft it would tie to a second GF sheet
//   7  10K / 14K exactly as before: the same world turned 10K, Decision 2 still refuses the draft, and no link wait
//   node tests/charm-nest/rg-fill-open.cjs
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), Place = require(path.join(root, 'charm-nest-place.js'));
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-fill-open.json'), 'utf8'));
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); if (!c) failed++; };
const clone = o => JSON.parse(JSON.stringify(o));
const D1 = 'd-bfaa108fb9d28aa7', GF20 = 'd-1ff008d1c436435d', NOW = Date.UTC(2026, 10, 23, 5, 7, 0), PLATE = 'rgs-19c277c9-7607-4545-89a3-f0280e75ef3d';
function host(records) { return Place.recordHost({ run: FX.run, records, sets: FX.sets, settings: {}, preset: 'A', partial: {}, now: NOW }); }
const pieces = (metal, order) => FX.rosePieces.map((c, i) => Object.assign({}, c, { metal }, order ? { order, poolId: `${order}_1_${i + 1}`, id: `pool:${order}_1_${i + 1}` } : {}));
function choose(records, metal, order) {
  const W = host(records), rid = order || FX.order, list = pieces(metal, order);
  const d = W.rules.chooseSheet({ metal, rid, run: FX.run, list, info: null, matePages: [], byMetalSize: 2 });
  return { W, id: d.page && (d.page.sheetId || d.page.id || null), fresh: !!(d.page && d.page._new), wait: d.wait, waitOn: d.wait && d.wait.sheet && (d.wait.sheet.sheetId || d.wait.sheet.id), left: d.page && d.page.greenLeftover };
}
const withD1 = patch => clone(FX.records).map(r => (r.id === D1 ? Object.assign(r, typeof patch === 'function' ? patch(r) : patch) : r));
// an order tied to GF Draft 20 (RG Draft 1's own GF sheet): a copy of order 4243151567's GF piece under a new order id
const tieToGF20 = (recs, order) => recs.map(r => { if (r.id !== GF20) return r; const c = r.charms.find(x => String(x.poolId).startsWith('4243151567')); r.charms = r.charms.concat([Object.assign({}, c, { poolId: `${order}_9_1`, id: `pool:${order}_9_1`, order })]); r.poolIds = (r.poolIds || []).concat([`${order}_9_1`]); return r; });

/* 1 · the second link */
{ const r = choose(clone(FX.records), 'rose');
  ok('1 · order 4244381926 (GF Draft 21) waits on RG Draft 1 (tied to GF Draft 20) while its green line is cut at once: not placed there, no new draft, no fresh plate', !!r.wait && r.wait.kind === 'green' && r.wait.cut === 'link' && r.waitOn === D1 && !r.fresh, { id: r.id, fresh: r.fresh, wait: r.wait && { kind: r.wait.kind, cut: r.wait.cut, on: r.waitOn } });
}
/* 2 · RG-only and the draft's own GF sheet */
{ const r = choose(clone(FX.records), 'rose', '9000000001');
  ok('2 · an RG-only order goes on RG Draft 1, no wait, no cut', r.id === D1 && !r.wait && !r.fresh, { id: r.id, wait: r.wait });
  const r2 = choose(tieToGF20(clone(FX.records), '9000000002'), 'rose', '9000000002');
  ok('2 · an order tied to GF Draft 20 (RG Draft 1\'s own GF sheet) goes on RG Draft 1, no wait', r2.id === D1 && !r2.wait && !r2.fresh, { id: r2.id, wait: r2.wait });
}
/* 3 · the first link */
{ const recs = withD1(r => ({ charms: r.charms.filter(c => !String(c.poolId).startsWith('4243151567')), poolIds: (r.poolIds || []).filter(id => !String(id).startsWith('4243151567')) }))
    .map(r => (r.id === GF20 ? Object.assign(r, { charms: r.charms.filter(c => !String(c.poolId).startsWith('4243151567')) }) : r));
  const r = choose(recs, 'rose');
  ok('3 · RG Draft 1 with no GF / SS link takes order 4244381926: its first link is nested on it, nothing cut before', r.id === D1 && !r.wait && !r.fresh, { id: r.id, wait: r.wait });
}
/* 4 · a draft owing its link cut */
{ const recs = withD1({ roseLinkCut: { at: NOW - 30e3, orderId: FX.order } });
  const r = choose(recs, 'rose', '9000000003');
  ok('4 · RG Draft 1 owes its link cut: an RG-only order waits for the cut (not on it, no new draft, no fresh plate)', !!r.wait && r.wait.cut === 'link' && r.waitOn === D1 && !r.fresh, { id: r.id, wait: r.wait && r.wait.kind, on: r.waitOn });
  const other = clone(recs.find(x => x.id === D1)); Object.assign(other, { id: 'd-other-open', page: 30, roseLinkCut: null, roseStockId: 'rgs-other', charms: other.charms.filter(c => !String(c.poolId).startsWith('4243151567')), createdAt: NOW, cardStartedAt: NOW });
  const r2 = choose(recs.concat([other]), 'rose', '9000000004');
  ok('4 · another open RG draft that keeps the rule takes it meanwhile', r2.id === 'd-other-open' && !r2.wait, { id: r2.id, wait: r2.wait && r2.wait.kind });
}
/* 5 · after the cut: the same plate's leftover */
{ const recs = withD1({ roseCutAt: NOW - 10e3, roseCutRevision: 1, roseRevision: 1, rosePlanHash: 'h', roseLinkCut: null });
  const r = choose(recs, 'rose');
  ok('5 · RG Draft 1 cut: order 4244381926 goes on a new draft on the leftover of RG Draft 1\'s own plate (revision 1), not a fresh plate', r.fresh && !r.wait && r.left && r.left.stockId === PLATE && r.left.revision === 1, { id: r.id, fresh: r.fresh, left: r.left, wait: r.wait });
}
/* 6 · a Rose Gold nest's overflow */
{ const recs = clone(FX.records), W = host(recs), d1 = W.views().find(v => v.sheetId === D1);
  const from = Object.assign({}, d1, { sheetId: 'rg-from', id: 'rg-from', charms: pieces('rose'), placements: [], roseLinkCut: null });
  const next = W.rules.nextSheet(from, pieces('rose'), {});
  ok('6 · what a Rose Gold nest passes on never goes on RG Draft 1 when it would tie it to a second GF sheet', !next || (next.sheetId || next.id) !== D1, { next: next && (next.sheetId || next.id), fresh: !!(next && next._new) });
  const rgOnly = pieces('rose', '9000000005'), from2 = Object.assign({}, from, { charms: rgOnly });
  const next2 = W.rules.nextSheet(from2, rgOnly, {});
  ok('6 · an RG-only order a nest passes on goes on the open RG draft (RG Draft 1)', !!next2 && (next2.sheetId || next2.id) === D1, { next: next2 && (next2.sheetId || next2.id) });
}
/* 7 · 10K / 14K unchanged */
for (const metal of ['gold10k', 'gold14k']) {
  const recs = clone(FX.records).map(r => (r.id === D1 ? Object.assign(r, { metal }) : r));
  const r = choose(recs, metal);
  ok(`7 · ${metal}: the same world turned ${metal} keeps Decision 2 as before (the draft with another GF partner is refused) and never a link wait`, !(r.id === D1 && !r.wait) && !(r.wait && r.wait.cut), { id: r.id, fresh: r.fresh, wait: r.wait && r.wait.kind, cut: r.wait && r.wait.cut });
}
console.log(failed ? `FAIL rg-fill-open (${failed})` : 'PASS rg-fill-open'); process.exit(failed ? 1 : 0);
