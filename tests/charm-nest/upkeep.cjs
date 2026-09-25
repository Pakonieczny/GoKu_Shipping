// A run left in Auto stays open for as long as orders come (Paul, 24 Sep: "everything must be able to stay on
// indefinitely"), and it kept every sheet, line, pool row, job, set and master design it ever made (audit, 25 Sep).
// Upkeep puts away what a day-old commit left behind, and only that:
//   1. a committed sheet a day past its commit goes with the orders on it; the first sheet of the metal (it is the card),
//      the sheet on the card, the newest four committed sheets, a Rose Gold sheet not yet cut and every sheet not yet
//      committed stay, and an order with a piece on a sheet that stays keeps that sheet's neighbours with it;
//   2. an order goes only when the run's line archive holds every line as it is now, and not while a review item,
//      engraving work or its order window is open on it;
//   3. the run counts what went as a resumed run counts what its record left out (lineArchive.base), so its record
//      reads the same;
//   4. sheet numbers stay as they were read, and a new sheet takes the next number;
//   5. committed sets whose sheets have all gone leave, the newest of each group stays for the next set's number;
//   6. master designs nothing uses go after six idle hours.
// The real module code runs with small stand-ins for the page. No network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const page = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const O = require(path.join(root, 'charm-nest-orders.js'));
const slice = (source, from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
const load = (code, env, names) => new Function(...Object.keys(env), `${code}\n;return {${names}};`)(...Object.values(env));

const DAY = 24 * 3600000, T0 = Date.UTC(2026, 8, 20, 12), NOW = T0 + 3 * DAY;
const METALS = [{ key: 'gold' }, { key: 'silver' }, { key: 'rose' }];

function world() {
  const S = { sheets: {}, poolSources: {} }, B = { run: null, orders: { rows: [], byKey: new Map() }, pool: { rows: new Map(), sources: new Map() }, sets: new Map(), review: { items: [] }, engrave: { items: new Map() } };
  const makeSheet = (metal, page) => ({ metal, page, charms: [], placements: [], status: 'idle', runId: 'run-1', sheetId: null, persistedDone: true, dirty: false, workers: [] });
  for (const m of METALS) { const prim = makeSheet(m.key, 1); prim.pages = [prim]; prim.active = 0; S.sheets[m.key] = prim; }
  const pagesOf = m => S.sheets[m].pages, allSheets = () => METALS.flatMap(m => pagesOf(m.key));
  const Sets = { ofRun: runId => [...B.sets.values()].filter(s => s.runId === runId) };
  const Pool = { holding(ids) { const want = new Set(ids), out = new Set(); for (const p of allSheets()) if (p.charms.some(c => want.has(c.poolId))) out.add(p); return out; } };
  const lineRecord = row => [row.key, { state: row.state, orderId: row.order.receiptId, poolIds: row.poolIds }];
  const Orders = { rows: () => B.orders.rows, lineRecord, render() {} };
  const working = new Set();
  const Engrave = { items: () => B.engrave.items, isWorking: j => working.has(j), view: () => ({ focus: null }), render() {} };
  let win = null;
  const OrderWin = { isOpen: () => !!win, key: () => win };
  const saved = [], logged = [];
  const RunCtl = { save: async r => { saved.push(r); }, renderBanner() {} };
  const Session = { schedule() {}, dropBest() {}, poolSourcesInUse() {
    const keep = new Set(); for (const p of allSheets()) for (const c of p.charms) keep.add(c.sourceId);
    return Object.fromEntries(Object.entries(S.poolSources).filter(([id]) => keep.has(id)));
  } };
  const shown = [];
  const window = { Sandbox: { streaming: () => false }, CharmNestOperations: require(path.join(root, 'charm-nest-operations.js')).create(), Recall: { on: () => false },
    CN: { showPage: (m, i) => shown.push([m, i]), renderCard() {} }, Session };
  const env = { window, B, S, METALS, Sets, Orders, O, Pool, Engrave, OrderWin, Review: { render() {} }, RunCtl, Session, allSheets, agent: (...a) => logged.push(a), SimClock: { now: () => NOW } };
  const { Upkeep } = load(slice(bridge, 'const Upkeep = window.Upkeep', '/* ═══ 25 · boot'), env, 'Upkeep');
  // the page's own addPage and removePage, as the card uses them
  const pageEnv = { S, makeSheet, activePage: m => S.sheets[m].pages[S.sheets[m].active], computeSaturation() {}, renderCard() {} };
  const { addPage, removePage } = load(slice(page, 'function addPage(metal)', '/** Whatever did not fit moves on'), pageEnv, 'addPage, removePage');

  // an order: its lines, their pool pieces on the given pages, committed or not, and archived as they are now
  let seq = 0;
  function order(rid, pages, { state = 'committed', archived = true, doneAt = T0 } = {}) {
    const rows = pages.map((pg, i) => {
      const poolId = `pool-${rid}-${i}`, key = `${rid}:${i}`;
      const row = { key, order: { receiptId: rid }, line: {}, state, poolIds: [poolId], doneAt };
      B.orders.rows.push(row); B.orders.byKey.set(key, row);
      B.pool.rows.set(poolId, { poolId, orderId: rid, state, aiPath: 'designs/' + rid });
      if (pg) pg.charms.push({ id: 'c' + (++seq), poolId, sourceId: 'pool:designs_' + rid });
      B.engrave.items.set(key, { key, row, state: 'written' });
      return row;
    });
    B.run.orders.push(rid);
    if (state === 'committed') B.run.committed.push(rid);
    for (const row of rows) { const [k, l] = lineRecord(row); B.run.lines[k] = l; if (archived) B.run.archivedLines[k] = O.textHash(JSON.stringify(l)); }
    return rows;
  }
  // a committed sheet: saved, in a set committed at `at` (and, unless `seen` is false, found committed by a pass then)
  function sheet(metal, at, { committed = true, status = 'complete', seen = true } = {}) {
    const pg = S.sheets[metal].pages.length === 1 && !S.sheets[metal].sheetId ? S.sheets[metal] : addPage(metal);
    pg.sheetId = `${metal}-${pg.page}`; pg.status = status;
    if (committed) { const set = { setId: 'set-' + pg.sheetId, runId: 'run-1', group: 'dispatch', committedAt: at, sheetIds: [pg.sheetId] }; B.sets.set('run-1|committed:' + set.setId, set); if (seen) pg.committedSeenAt = at; }
    return pg;
  }
  B.run = { runId: 'run-1', status: 'processed', orders: [], committed: [], holds: {}, lines: {}, archivedLines: {}, lineArchive: { parts: 3, at: T0 } };
  return { S, B, Upkeep, sheet, order, addPage, removePage, pagesOf, allSheets, working, setWin: k => { win = k; }, saved, logged, shown };
}

(async () => {
  // 1-3. A metal with eight committed sheets and one still filling
  {
    const w = world(), { B, S, Upkeep, sheet, order, pagesOf } = w;
    const gold = [];
    for (let i = 0; i < 8; i++) gold.push(sheet('gold', T0 + i * 3600000));
    const filling = sheet('gold', null, { committed: false, status: 'partial' });
    S.sheets.gold.active = S.sheets.gold.pages.indexOf(filling);
    const perSheet = gold.map((pg, i) => order('R' + i, [pg]));
    const open = order('R-open', [filling], { state: 'pooled', archived: false, doneAt: 0 });
    // an order across sheet 3 (index 2) and the newest committed sheet, which stays: sheet 3 stays with it
    const across = order('R-across', [gold[2], gold[7]]);
    // an order whose archive holds an older copy of a line: it stays, and so does its sheet (index 3)
    const stale = order('R-stale', [gold[3]]); B.run.archivedLines[stale[0].key] = 'old-hash';
    // a held order in the run's holds, all its lines gone from Etsy, on no sheet
    const gone = order('R-gone', [null], { state: 'gone' }); B.run.holds['R-gone'] = { why: 'cancelled' };
    const before = { lines: Object.keys(B.run.lines).length, committed: B.run.committed.length };
    const res = await Upkeep.sweep({ at: NOW });
    const numbers = pagesOf('gold').map(p => p.page);
    // kept: sheet 1 (the card), sheets 3 and 4 (tied to kept work), the newest four committed (5-8), the filling sheet 9
    assert.deepEqual(numbers, [1, 3, 4, 5, 6, 7, 8, 9], 'only sheet 2 could go: ' + numbers);
    assert.equal(res.pages, 1);
    assert.equal(S.sheets.gold.pages[S.sheets.gold.active], filling, 'the card keeps its page');
    assert(!B.orders.byKey.has(perSheet[1][0].key) && B.orders.byKey.has(perSheet[0][0].key), 'the order on sheet 2 went, the one on sheet 1 stayed');
    assert(!B.pool.rows.has(perSheet[1][0].poolIds[0]) && !B.engrave.items.has(perSheet[1][0].key), 'its pool row and engraving job went with it');
    assert(B.orders.byKey.has(across[0].key) && B.orders.byKey.has(stale[0].key) && B.orders.byKey.has(open[0].key), 'tied, stale and open orders stay');
    assert(!B.orders.byKey.has(gone[0].key) && !('R-gone' in B.run.holds), 'an order gone from Etsy on no sheet goes, its hold counted outside');
    // 3. the run: what went is counted as outside the record, as a resume counts it
    const base = B.run.lineArchive.base;
    assert.equal(base.lines, before.lines - Object.keys(B.run.lines).length, 'every line that left is counted');
    assert.equal(base.committed, before.committed - B.run.committed.length, 'every committed order that left is counted');
    assert.equal(base.held, 1);
    assert(!B.run.orders.includes('R1') && !B.run.orders.includes('R-gone') && B.run.orders.includes('R0'), "the run's order list drops them");
    assert(!Object.keys(B.run.archivedLines).some(k => k.startsWith('R1:')), 'their archive marks go');
    assert.equal(w.saved.length, 1, 'the run record is saved once after');
    assert(w.logged.some(a => /Put away 1 sheet and 2 orders/.test(a[2])), 'one line in the activity log');
    // 4. a new sheet takes the next number, and removing it leaves the numbers read on screen as they were
    const next = w.addPage('gold'); assert.equal(next.page, 10, 'a new sheet is sheet 10, not a second sheet 9');
    w.removePage(next); assert.deepEqual(pagesOf('gold').map(p => p.page), [1, 3, 4, 5, 6, 7, 8, 9]);
    // a second pass the same day changes nothing
    const again = await Upkeep.sweep({ at: NOW });
    assert.equal(again.pages + again.orders, 0);
  }

  // 1. nothing before its day is up; Rose Gold not yet cut stays; open work keeps its order
  {
    const w = world(), { B, S, Upkeep, sheet, order, pagesOf } = w;
    const rose = [];
    for (let i = 0; i < 10; i++) rose.push(sheet('rose', T0));
    rose[1].rosePlan = { lines: [] };                                  // sheet 2: a contour not yet cut
    rose[2].roseCutAt = T0;                                            // sheet 3: cut, may go
    const recent = sheet('rose', NOW - 3600000);                       // sheet 11: committed an hour ago
    const rows = rose.map((pg, i) => order('Q' + i, [pg]));
    order('Q-recent', [recent], { doneAt: NOW - 3600000 });
    // sheet 4's order has an open review item, sheet 5's has its order window open, sheet 6's engraving is being fitted
    B.review.items.push({ key: 'held:Q3', rid: 'Q3' });
    w.setWin(rows[4][0].key);
    w.working.add(B.engrave.items.get(rows[5][0].key));
    S.sheets.rose.active = 0;
    const res = await Upkeep.sweep({ at: NOW });
    assert.deepEqual(pagesOf('rose').map(p => p.page), [1, 2, 4, 5, 6, 8, 9, 10, 11], 'sheets 3 and 7 went; the uncut contour, open work and the newest four stay: ' + pagesOf('rose').map(p => p.page));
    assert.equal(res.orders, 2);
    assert(!B.orders.byKey.has(rows[2][0].key) && !B.orders.byKey.has(rows[6][0].key));
    // the next day, with the review answered, the window shut and the fit done, those go too; the uncut contour and the
    // newest four committed sheets stay
    B.review.items = []; w.setWin(null); w.working.clear();
    const later = await Upkeep.sweep({ at: NOW + DAY });
    assert.deepEqual(pagesOf('rose').map(p => p.page), [1, 2, 8, 9, 10, 11], 'next day: ' + pagesOf('rose').map(p => p.page));
    assert.equal(later.orders, 3);
  }

  // 5-6. sets and master designs
  {
    const w = world(), { B, S, Upkeep, sheet, order } = w;
    const pages = [];
    for (let i = 0; i < 7; i++) pages.push(sheet('silver', T0 + i));
    pages.forEach((pg, i) => order('P' + i, [pg]));
    // an idle master nothing uses, a busy one a charm on a kept sheet uses, and a fresh unused one
    const src = (id, usedAt) => { const s = { id, usedAt }; S.poolSources[id] = s; B.pool.sources.set('path/' + id, s); return s; };
    src('pool:idle', NOW - 7 * 3600000); src('pool:designs_P6', 0); src('pool:fresh', NOW - 3600000);
    const res = await Upkeep.sweep({ at: NOW });
    // sheets 2 and 3 go (sheet 1 is the card, 4-7 the newest four); their sets go; the newest committed set stays
    assert.equal(res.pages, 2);
    assert.equal(res.sets, 2, 'the sets of the two sheets that went');
    assert.deepEqual([...B.sets.values()].map(s => s.sheetIds[0]).sort(), ['silver-1', 'silver-4', 'silver-5', 'silver-6', 'silver-7']);
    assert(!('pool:idle' in S.poolSources) && !B.pool.sources.has('path/pool:idle'), 'an idle unused master goes');
    assert('pool:designs_P6' in S.poolSources && 'pool:fresh' in S.poolSources, 'a used one and a recently used one stay');
  }

  // the day is counted on the pass's own clock from the first pass that finds the set committed: the set's stamp is
  // real time, and the sandbox stream plays its days faster (a sheet went at once when the stream ran a day ahead)
  {
    const w = world(), { Upkeep, sheet, order, pagesOf } = w;
    const pages = []; for (let i = 0; i < 6; i++) pages.push(sheet('gold', Date.now(), { seen: false }));
    pages.forEach((pg, i) => order('S' + i, [pg], { doneAt: null }));
    const first = await Upkeep.sweep({ at: NOW + 30 * DAY });
    assert.equal(first.pages + first.orders, 0, 'the first pass only starts the day');
    assert(pages.every(p => p.committedSeenAt === NOW + 30 * DAY), 'every committed sheet is stamped, the newest four too');
    const next = await Upkeep.sweep({ at: NOW + 31 * DAY + 60000 });
    assert.deepEqual([next.pages, next.orders], [1, 1], 'a day later on the same clock sheet 2 goes with its order');
    assert.deepEqual(pagesOf('gold').map(p => p.page), [1, 3, 4, 5, 6]);
  }

  // nothing is touched while orders are being taken in, or with no open run
  {
    const w = world(), { B, Upkeep, sheet, order } = w;
    for (let i = 0; i < 7; i++) order('Z' + i, [sheet('gold', T0)]);
    B.run.arrivalBusy = true;
    assert.equal(await Upkeep.sweep({ at: NOW }), null);
    B.run.arrivalBusy = false; B.run.status = 'complete';
    assert.equal(await Upkeep.sweep({ at: NOW }), null);
  }
  console.log('Upkeep OK: day-old committed sheets and their orders leave, open work and the card stay, the run keeps its counts, numbers stay, sets and idle masters go');
})().catch(e => { console.error(e); process.exitCode = 1; });
