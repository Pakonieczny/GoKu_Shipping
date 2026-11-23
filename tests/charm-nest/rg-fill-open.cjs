// RGFILL (Paul, 23 Nov 2026: "Why did this rose gold draft not get used further and now it started another draft 2 ... I told you this can never happen" · "It should never start a brand new
// draft sheet if there's still room on a previous draft that's why we have the green cut lines for" · "the RG had its own path. Do not fuck with [10K / 14K]").
// At 00:07 the cloud's placement opened a new Rose Gold draft (on RG Sheet 1's leftover) for order 4244381926 while RG Draft 1 (d-bfaa108fb9d28aa7) was 20% full: the order's Gold Filled pieces
// are on another GF draft than RG Draft 1's orders', and Decision 2 (one GF partner sheet) refused RG Draft 1. For Rose Gold the green cut line divides a sheet by set; an open RG draft takes the
// order. The fixture is the run as the cloud saw it then (tests/charm-nest/fixtures/rg-fill-open.json: the records, read only, positions and names; the draft it opened removed).
//   1  the cloud's real placement rules (charm-nest-place.js recordHost, chooseSheet) on those records: order 4244381926's Rose Gold pieces go on RG Draft 1, no new draft, no wait
//   2  what a Rose Gold nest passes on (nextSheet) goes to the open RG draft, not a new one
//   3  10K / 14K are exactly as before: the same world turned 10K, Decision 2 still refuses the draft (the rule unchanged for them)
//   4  a Rose Gold draft that is cut, released full or lasered takes nothing new (the green line's own leftover is the next page, as before)
//   node tests/charm-nest/rg-fill-open.cjs
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), Place = require(path.join(root, 'charm-nest-place.js'));
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures', 'rg-fill-open.json'), 'utf8'));
let failed = 0; const ok = (n, c, d) => { console.log((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 500))); if (!c) failed++; };
const clone = o => JSON.parse(JSON.stringify(o));
const D1 = 'd-bfaa108fb9d28aa7', NOW = Date.UTC(2026, 10, 23, 5, 7, 0);
function host(records, partial) { return Place.recordHost({ run: FX.run, records, sets: FX.sets, settings: {}, preset: 'A', partial: partial || {}, now: NOW }); }
const pieces = metal => FX.rosePieces.map(c => Object.assign({}, c, { metal }));
function choose(records, metal) {
  const W = host(records), list = pieces(metal);
  const d = W.rules.chooseSheet({ metal, rid: FX.order, run: FX.run, list, info: null, matePages: [], byMetalSize: 2 });
  return { page: d.page, wait: d.wait, id: d.page && (d.page.sheetId || d.page.id || null), fresh: !!(d.page && d.page._new), events: d.events };
}

/* 1 · the placement of order 4244381926 */
{ const r = choose(clone(FX.records), 'rose');
  ok('1 · order 4244381926\'s Rose Gold pieces go on RG Draft 1 (20% full), not on a new draft, and do not wait', r.id === D1 && !r.fresh && !r.wait, { id: r.id, fresh: r.fresh, wait: r.wait, events: r.events.map(e => e.kind) });
}
/* 2 · what a Rose Gold nest passes on */
{ const recs = clone(FX.records), W = host(recs), views = W.views(), d1 = views.find(v => v.sheetId === D1);
  const from = Object.assign({}, d1, { sheetId: 'rg-from', id: 'rg-from', charms: pieces('rose'), placements: [] });
  const next = W.rules.nextSheet(from, pieces('rose'), {});
  ok('2 · what a Rose Gold nest passes on goes to the open RG draft (RG Draft 1), not a new draft', !!next && (next.sheetId || next.id) === D1 && !next._new, { next: next && (next.sheetId || next.id), fresh: !!(next && next._new) });
}
/* 3 · 10K / 14K unchanged */
for (const metal of ['gold10k', 'gold14k']) {
  const recs = clone(FX.records).map(r => (r.id === D1 ? Object.assign(r, { metal }) : r));
  const r = choose(recs, metal);
  ok(`3 · ${metal}: the same world turned ${metal} keeps Decision 2 as before (the open draft with another GF partner is refused: it waits for its green line or another page is used)`, !(r.id === D1 && !r.wait), { id: r.id, fresh: r.fresh, wait: r.wait && r.wait.kind });
}
/* 4 · a closed Rose Gold draft takes nothing new */
for (const [why, patch] of [['cut', { roseCutAt: NOW - 60e3, roseCutRevision: 1 }], ['released full', { releaseFull: true }], ['lasered', { laserDoneAt: NOW - 60e3 }]]) {
  const recs = clone(FX.records).map(r => (r.id === D1 ? Object.assign(r, patch) : r));
  const r = choose(recs, 'rose');
  ok(`4 · RG Draft 1 ${why}: it takes nothing new`, r.id !== D1, { id: r.id, fresh: r.fresh });
}
console.log(failed ? `FAIL rg-fill-open (${failed})` : 'PASS rg-fill-open'); process.exit(failed ? 1 : 0);
