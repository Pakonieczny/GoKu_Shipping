// ITEM 12 (Paul, 21 Nov 2026): "Pin" in a sheet card's piece dialog (click a charm to inspect · pin), on a staff computer (a viewer) while the cloud carries every run (mode "on").
//   Why: the pin only changed the page's memory. A viewer writes no workspace (Session frozen) and saves no sheet (persistSheet returns false), so the pin was gone after a reload
//   and the cloud's nest never held the charm. Now, on a viewer, Pin / Unpin is a person's press: sheetLayoutPatch with the pin alone (charmNestLibrary.js sheetPinPatch) puts the
//   record's own mark of a person's pin on the charm (manual { by, at }, pinned at the record's pose), which the cloud's nest holds where it is (_charmNestNestSlot.js prepare).
//   S  the library in mode "on" (offline, _cloudFakes.cjs): the pin is kept on the record (nothing moved, no file owed, no revision moved), the nest's input key changes with it
//      (a nest asked before it is stale), the cloud's next nest of the sheet (the REAL prepare of _charmNestNestSlot.js, its pieces made as squares) holds the pinned charm where it is
//      and frees the others (fresh) / lifts nothing it pinned; Unpin takes it off again; refused in words: a charm resized by hand, another screen in Manual Control, a laser-cut sheet,
//      a charm no longer placed, an old page; a patch of poses works as before
//   E  the REAL page over the fake site (_engraveRealWorld.cjs), made a viewer (CNViewer on, Session frozen, no run held, the run followed, mode "on" on the fake site): Pin in the
//      piece dialog reaches the cloud's record and says so; the sheet read again from the cloud (CharmNestCloudSheets.reload, what a reload and every other computer do) shows it
//      pinned ("Unpin") and the other charm not; Unpin there takes it off the record. Mode off: Pin is the page's own as before (no press sent, the record untouched)
//   G8 (21 Nov 2026): a pin pressed while a nest runs is never lost: the writer merges the record's pins into the nest's layout (_charmNestNest.js pinMerge: the pinned charm at its pin,
//      a piece in its way left for the next nest), and its write names the pins it holds (putSheet pinsBase): a Pin / Unpin pressed after that refuses the write, nothing written
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item12.cjs
'use strict';
const path = require('path'), Module = require('module');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  // the cloud's nest makes each piece again from its master file: here each is a 10 × 10 pt square, its record's manual mark read by the REAL manualOf (as the real rebuild reads it)
  const realPieces = require(path.join(F.fnDir, '_charmNestNestPieces.js'));
  const prevLoad = Module._load;
  Module._load = function (req, parent, ...rest) {
    if (req === './_charmNestNestPieces' && parent && /_charmNestNestSlot\.js$/.test(parent.filename || '')) return Object.assign({}, realPieces, { create: () => ({
      rebuild: async (rec, rows) => ({ missing: [], charms: (rec.charms || []).filter(rc => rc && !rc.excluded).map(rc => { const pool = rows.get(String(rc.poolId)) || {}, man = realPieces.manualOf(rc);
        return Object.assign({ id: String(rc.id), name: rc.name, sku: pool.sku || null, order: String(pool.orderId || rc.id), orderDate: +pool.orderDate || 0, arrivedAt: 0, poolId: String(rc.poolId), metal: rec.metal, excluded: false, pinned: null,
          w: 20, h: 20, scale: 0.5, bits: new Uint8Array(400).fill(1), areaPt2: 100, widthPt: 10, heightPt: 10, hash: rc.hash, orderInfo: { receiptId: pool.orderId } }, man ? { manual: man } : {}); }) }) }) });
    return prevLoad.call(this, req, parent, ...rest);
  };
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Nest = F.load('_charmNestNest.js'), Slot = F.load('_charmNestNestSlot.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-item12', SH = 'gold-s12-item12', ids = ['src1:p1', 'src1:p2', 'src1:p3'], pools = ['4242000001_42420000011_1', '4242000002_42420000021_1', '4242000003_42420000031_1'];
    const PL = [{ id: ids[0], cxPt: 40.25, cyPt: 60.5, angle: 0, scale: 0.971, wPt: 10, hPt: 10 }, { id: ids[1], cxPt: 110, cyPt: 60, angle: 90, scale: 0.971, wPt: 10, hPt: 10 }, { id: ids[2], cxPt: 180, cyPt: 60, angle: 0, scale: 1.0681, wPt: 11, hPt: 11 }];
    const seed = (mode, extra) => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['engrave', 'files'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
      pools.forEach((p, i) => F.seed('Charm_Pool/' + p, { poolId: p, runId: R, orderId: p.split('_')[0], orderDate: T0 - (3 - i) * 86400e3, state: 'placed', sheetId: SH, sku: 'TEST_SQUARE_' + i }));
      F.seed('Charm_Nest_Sheets/' + SH, Object.assign({ id: SH, runId: R, metal: 'gold', fileBase: 'GF_Item12_Sheet-1', status: 'complete', layoutRev: 3, contentRev: 5, poolIds: pools.slice(), placements: PL.map(p => Object.assign({}, p)),
        charms: ids.map((id, i) => Object.assign({ id, poolId: pools[i], hash: 'h' + i, name: `${pools[i].split('_')[0]} · TEST_SQUARE_${i}`, areaPt2: 100, widthPt: 10, heightPt: 10, excluded: false, open: false },
          i === 2 ? { manual: { by: 'Ann', at: T0 - 7200e3, size: 1.1 } } : {})) }, extra || {}));
    };
    const rec = () => F.doc('Charm_Nest_Sheets/' + SH), charmOf = id => (rec().charms || []).find(c => c.id === id), num6 = () => +rec().contentRev || 0;
    const keyOf = async (r, mode) => { const S = await Nest.readSettings(F.db), plate = Nest.plateOf(r, S, null, await Nest.readPolicy(F.db)); return Nest.inputKeyOf(r, await Nest.poolRowsOf(F.db, r), S, plate, mode); };
    // the cloud's next nest of the sheet, as it begins: the REAL prepare (slot 0), at the record's revisions and input key
    const nestOf = async mode => { const r = Object.assign({ id: SH }, rec()); const job = { id: 'nest-item12-' + mode, sheetId: SH, layoutBase: +r.layoutRev || 0, contentBase: +r.contentRev || 0, inputKey: await keyOf(r, mode), mode, seed: 7 };
      const out = await Slot.prepare({ db: F.db, bucket: F.bucket, job, log: () => {}, tick: async () => {}, stop: false });
      const pieces = out && out.solverJob ? out.solverJob.pieces : [], alt = out && out.altJob ? out.altJob.pieces : []; return { out, by: id => pieces.find(p => p.id === id) || null, alt: id => alt.find(p => p.id === id) || null }; };

    seed('on');
    const before = rec(), key0 = await keyOf(Object.assign({ id: SH }, before), 'fresh');
    let r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[0]]: { pinned: true } }, dev: PAGE, cv: CV });
    let c0 = charmOf(ids[0]), after = rec();
    ok('S1 mode on: a viewer\'s Pin is taken and kept on the sheet\'s record: the person\'s mark (by, when) and the pin at the record\'s own pose',
      r.http === 200 && r.ok && r.pinned && r.pinned[0] === ids[0] && c0.manual && c0.manual.by === 'Michelle' && c0.manual.at === T0 && c0.pinned && c0.pinned.cxPt === 40.25 && c0.pinned.cyPt === 60.5 && c0.pinned.angle === 0 && !c0.arrivalPin, { r, c0 });
    ok('S2 ... nothing moved: the same placements, no file owed, neither revision moved, the other charms untouched',
      JSON.stringify(after.placements) === JSON.stringify(before.placements) && !after.filesOwed && after.layoutRev === 3 && after.contentRev === 5 && !charmOf(ids[1]).manual && !charmOf(ids[1]).pinned && charmOf(ids[2]).manual.size === 1.1,
      { placements: after.placements, filesOwed: after.filesOwed, layoutRev: after.layoutRev, contentRev: after.contentRev, c1: charmOf(ids[1]) });
    ok('S3 ... the cloud\'s nest input key changes with the pin (a nest asked before it is stale and asked again)', key0 !== await keyOf(Object.assign({ id: SH }, after), 'fresh'));
    let n = await nestOf('fresh');
    // (ONEPIECE, Paul 21 Nov 2026: "they go on one piece at a time", and PAIRTOGETHER, 22 Nov 2026: the unit of a nest is ONE ORDER (_charmNestNestSlot.js prepare): a nest afresh places the oldest
    //  order and the others wait for the next nests (feedWait), one order at a time; NOWHOLE, Paul 21 Nov 2026: "You must remove that rule": no Gold Filled sheet is laid out whole again, so
    //  there is no second slot that appends (altJob) any more: the nest that keeps the layout IS the one that appends)
    const metaOf = (x, id) => ((x.out && x.out.input && x.out.input.meta && x.out.input.meta.charms) || []).find(c => c && String(c.id) === id) || null;
    ok('S4 the cloud\'s next nest of the sheet (afresh) holds the pinned charm exactly where it is; the others are free: not pinned, they wait for the next nests, one order at a time',
      !!n.out && !n.out.error && !n.out.stale && n.by(ids[0]) && n.by(ids[0]).hold === true && n.by(ids[0]).pinned && n.by(ids[0]).pinned.cxPt === 40.25 && n.by(ids[0]).pinned.cyPt === 60.5
        && !n.by(ids[1]) && (n.out.feedWait || []).includes(ids[1]) && (n.out.feedWait || []).includes(ids[2]) && metaOf(n, ids[1]) && metaOf(n, ids[1]).pinned === null,
      { err: n.out && (n.out.error || n.out.stale), p0: n.by(ids[0]) && { hold: n.by(ids[0]).hold, pinned: n.by(ids[0]).pinned }, p1: n.by(ids[1]) && { hold: n.by(ids[1]).hold, pinned: n.by(ids[1]).pinned }, feedWait: n.out && n.out.feedWait, m1: metaOf(n, ids[1]) });
    n = await nestOf('auto');
    // (NQ, 21 Nov 2026, then NOWHOLE the same day: a GF sheet in no set is no longer laid out whole by "Nest the rest": the pieces on it stay where they lie, the person's pin held for good,
    //  the others pinned by the run only (may be lifted), in the one nest that appends to the saved layout: cloud-nest-quality.cjs)
    ok('S5 ... and its nest that keeps the layout holds it for good (a person\'s pin); the others stay where they lie, pinned by the run only (may be lifted); no second slot (NOWHOLE)',
      n.by(ids[0]) && n.by(ids[0]).hold === true && n.by(ids[1]) && n.by(ids[1]).hold === false && !!n.by(ids[1]).pinned && n.by(ids[1]).pinned.cxPt === 110 && n.by(ids[1]).pinned.cyPt === 60 && !n.out.altJob,
      { p0: n.by(ids[0]) && n.by(ids[0]).hold, p1: n.by(ids[1]) && [n.by(ids[1]).hold, n.by(ids[1]).pinned], alt: !!(n.out && n.out.altJob) });
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[0]]: { pinned: false } }, dev: PAGE, cv: CV });
    c0 = charmOf(ids[0]); n = await nestOf('fresh');
    ok('S6 Unpin from a viewer takes the mark and the pin off the record, and the next nest frees that charm',
      r.http === 200 && r.ok && r.unpinned[0] === ids[0] && !c0.manual && !c0.pinned && n.by(ids[0]) && n.by(ids[0]).hold === false && n.by(ids[0]).pinned === null && JSON.stringify(rec().placements) === JSON.stringify(before.placements), { r, c0, p0: n.by(ids[0]) });
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[2]]: { pinned: false } }, dev: PAGE, cv: CV });
    ok('S7 ... a charm resized by hand (its size holds it) is not unpinned: refused in words, its mark kept', r.http === 409 && r.refused === 'sized' && /resized by hand/.test(r.error || '') && charmOf(ids[2]).manual && charmOf(ids[2]).manual.size === 1.1, r);
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[2]]: { pinned: true } }, dev: PAGE, cv: CV });
    ok('S8 ... Pin on it keeps its size and its first mark, and pins it with its size', r.http === 200 && charmOf(ids[2]).manual.size === 1.1 && charmOf(ids[2]).manual.by === 'Ann' && charmOf(ids[2]).pinned.size === 1.1 && charmOf(ids[2]).pinned.cxPt === 180, charmOf(ids[2]));
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { 'src1:gone': { pinned: true } }, dev: PAGE, cv: CV });
    ok('S9 ... a charm no longer placed on the sheet: refused in words, nothing written', r.http === 409 && r.refused === 'gone' && /nothing was pinned/.test(r.error || ''), r);
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[1]]: { pinned: true } }, dev: PAGE });
    ok('S10 ... from an old page (no cv) in mode on: the reload words, nothing written', r.http === 409 && r.reload === true && !charmOf(ids[1]).manual, r);
    seed('on', { manualEdit: { tab: 'tOther', by: 'Ann', at: T0 - 1000 } });
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[1]]: { pinned: true } }, dev: PAGE, cv: CV });
    ok('S11 another screen moving the sheet\'s charms by hand (Manual Control): refused in words, nothing written', r.http === 409 && r.refused === 'held' && /Ann/.test(r.error || '') && !charmOf(ids[1]).manual, r);
    seed('on', { laserDoneAt: T0 - 1000 });
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[1]]: { pinned: true } }, dev: PAGE, cv: CV });
    ok('S12 a laser-cut sheet: refused in words, nothing written', r.http === 409 && r.refused === 'cut' && !charmOf(ids[1]).manual, r);
    seed('off');
    r = await P({ op: 'sheetLayoutPatch', id: SH, tab: 't1', by: 'Ann', poses: { [ids[1]]: { cxPt: 112, cyPt: 61, angle: 90 } }, pins: { [ids[1]]: { pinned: { cxPt: 112, cyPt: 61, angle: 90 }, manual: { by: 'Ann', at: T0 } } }, mark: { at: T0, by: 'Ann', n: 1 } });
    ok('S13 mode off: a patch of poses (Manual Control) works as before: moved, pinned with its mark, a file owed', r.http === 200 && r.moved === 1 && rec().placements[1].cxPt === 112 && charmOf(ids[1]).manual.by === 'Ann' && !!rec().filesOwed, { r, c1: charmOf(ids[1]) });
    r = await P({ op: 'sheetLayoutPatch', id: SH, tab: 't1', by: 'Ann' });
    ok('S14 ... and a patch with neither poses nor pins is refused as before', /no pose to write/.test(r.error || ''), r);

    // G8 (21 Nov 2026): a pin pressed while a cloud nest of the sheet runs is never lost when the nest's layout is written
    seed('on');
    const keyBefore = Nest.pinKeyOf(rec().charms);
    // (a nest asked BEFORE the pin: its layout moved the charm and put another piece where it stood; it also turned one piece away)
    const nestLayout = [{ id: ids[0], cxPt: 150, cyPt: 20, angle: 0, scale: 0.971, wPt: 10, hPt: 10 }, { id: ids[1], cxPt: 41, cyPt: 61, angle: 0, scale: 0.971, wPt: 10, hPt: 10 }, { id: ids[2], cxPt: 180, cyPt: 60, angle: 0, scale: 1.0681, wPt: 11, hPt: 11 }];
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[0]]: { pinned: true } }, dev: PAGE, cv: CV });
    const keyAfter = Nest.pinKeyOf(rec().charms);
    ok('S15 G8 the record\'s pin key changes with a Pin (and only with a person\'s marks)', r.ok && keyBefore !== keyAfter && Nest.pinKeyOf(rec().charms) === keyAfter, { keyBefore, keyAfter });
    const sz = id => ({ w: 10, h: 10 }), has = id => ids.includes(id);
    let m = Nest.pinMerge({ placements: nestLayout, rejects: [], recCharms: rec().charms, recPlacements: rec().placements, has, sizeOf: sz, clearancePt: 1 });
    const at0 = m.placements.find(p => p.id === ids[0]);
    ok('S16 G8 the nest\'s layout merged with the record\'s pins as they are now: the pinned charm back EXACTLY where it is pinned, the piece the nest put there makes room (left unplaced, nested again), the others as the nest put them; nothing given is changed',
      m.held.length === 1 && m.held[0] === ids[0] && at0 && at0.cxPt === 40.25 && at0.cyPt === 60.5 && at0.angle === 0 && at0.scale === 0.971 && JSON.stringify(m.lifted) === JSON.stringify([ids[1]]) && !m.placements.some(p => p.id === ids[1])
        && m.placements.find(p => p.id === ids[2]).cxPt === 180 && nestLayout[0].cxPt === 150, m);
    m = Nest.pinMerge({ placements: nestLayout.filter(p => p.id !== ids[0]).map(p => p.id === ids[1] ? Object.assign({}, p, { cxPt: 110 }) : p), rejects: [ids[0]], recCharms: rec().charms, recPlacements: rec().placements, has, sizeOf: sz, clearancePt: 1 });
    ok('S17 G8 ... a pinned charm the nest turned away is put back at its pin (out of the rejects), and nothing makes room when the place is free', m.held[0] === ids[0] && !m.rejects.includes(ids[0]) && m.placements.some(p => p.id === ids[0] && p.cxPt === 40.25) && !m.lifted.length && m.placements.length === 3, m);
    m = Nest.pinMerge({ placements: PL.map(p => Object.assign({}, p)), rejects: [], recCharms: rec().charms, recPlacements: rec().placements, has, sizeOf: sz, clearancePt: 1 });
    ok('S18 G8 ... a layout that already holds the pin is written as the nest made it (nothing held again, nothing lifted)', !m.held.length && !m.lifted.length && JSON.stringify(m.placements) === JSON.stringify(PL), m);
    const C = require(path.join(F.fnDir, '_charmNestOpClass.js'));
    // (the cloud's sheet writer writes the layout it made with the pins it saw: a Pin pressed after that refuses the write, nothing written; with the pins it holds now it is written, and the pin stays)
    const before18 = rec();
    r = await lib.call({ op: 'putSheet', sheet: { id: SH, placements: nestLayout, status: 'complete' }, layoutBase: 3, contentBase: 5, pinsBase: keyBefore }, { unit: 'test|g8|stale' });
    ok('S19 G8 the cloud\'s write of a layout made before the pin (pinsBase of then): refused (pinMoved), NOTHING written: the pin and its place stand', C.isCloudOp('putSheet') && r.status === 409 && r.pinMoved === true && /pinned or unpinned/.test(r.error || '')
      && JSON.stringify(rec().placements) === JSON.stringify(before18.placements) && rec().layoutRev === 3 && charmOf(ids[0]).manual.by === 'Michelle' && charmOf(ids[0]).pinned.cxPt === 40.25, { r, layoutRev: rec().layoutRev });
    const merged = Nest.pinMerge({ placements: nestLayout, rejects: [], recCharms: rec().charms, recPlacements: rec().placements, has, sizeOf: sz, clearancePt: 1 });
    r = await lib.call({ op: 'putSheet', sheet: { id: SH, placements: merged.placements, status: 'partial', dirty: true }, layoutBase: 3, contentBase: 5, pinsBase: Nest.pinKeyOf(rec().charms) }, { unit: 'test|g8|fresh' });
    const p0 = rec().placements.find(p => p.id === ids[0]);
    ok('S20 G8 ... made again with the pins as they are (merged): written; the pinned charm at its pin, its mark kept, the piece that made room left for the next nest', r.status === 200 && r.ok !== false && rec().layoutRev === 4 && p0 && p0.cxPt === 40.25 && p0.cyPt === 60.5
      && charmOf(ids[0]).manual.by === 'Michelle' && charmOf(ids[0]).pinned.cxPt === 40.25 && !rec().placements.some(p => p.id === ids[1]), { r, placements: rec().placements });
    r = await P({ op: 'sheetLayoutPatch', id: SH, by: 'Michelle', pins: { [ids[0]]: { pinned: false } }, dev: PAGE, cv: CV });
    const kNow = Nest.pinKeyOf(rec().charms);
    r = await lib.call({ op: 'putSheet', sheet: { id: SH, placements: rec().placements, status: 'partial' }, layoutBase: 4, contentBase: num6(), pinsBase: Nest.pinKeyOf(Object.assign([], before18.charms)) }, { unit: 'test|g8|unpin' });
    ok('S21 G8 an Unpin pressed while the files were made is not written over either (refused, the charm stays unpinned)', r.status === 409 && r.pinMoved === true && !charmOf(ids[0]).manual && !charmOf(ids[0]).pinned && kNow !== keyAfter, r);
    r = await lib.call({ op: 'putSheet', sheet: { id: SH, placements: rec().placements, status: 'partial' }, layoutBase: 4, contentBase: num6() }, { unit: 'test|g8|plain' });
    ok('S22 G8 a write that names no pins (any other writer) is taken as before', r.status === 200, r);
  } finally { console.log = quiet; console.warn = qw; Module._load = prevLoad; }
}

async function partE() {
  let Rw; try { Rw = require('./_engraveRealWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const w = await Rw.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { log0('E: no playwright-core: the real page was not run'); return; }
  const page = w.page, st = w.srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 150)); } };
  const presses = []; page.on('request', q => { try { const b = q.postData() || ''; if (/charmNestLibrary/.test(q.url()) && /"op":"sheetLayoutPatch"/.test(b)) presses.push(JSON.parse(b)); } catch (_) {} });
  try {
    await Rw.build(w, ['back', 'both']);
    const R = 'run-2026-11-21-item12';
    // the sheet the pieces are on, kept in the cloud as the cloud's sheet writer keeps it (its placements and its charms)
    const sheet = await page.evaluate(() => {
      const sh = allSheets().find(s => s.sheetId && s.charms.filter(c => c.poolId && s.placements.some(p => p.id === c.id)).length >= 2); if (!sh) return null;
      sh.placements.forEach(p => { p.scale = 0.971; p.wPt = p.wPt || 12; p.hPt = p.hPt || 12; });
      const PP = window.CharmNestPoolPieces, placed = sh.charms.filter(c => c.poolId && sh.placements.some(p => p.id === c.id));
      return { id: sh.sheetId, metal: sh.metal, fileBase: sh.fileBase, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle || 0, scale: 0.971, wPt: p.wPt, hPt: p.hPt })),
        charms: placed.map(c => Object.assign({ id: c.id, poolId: c.poolId, hash: c.hash, name: c.name, sku: c.sku || (c.orderInfo && c.orderInfo.sku) || null, sourceId: c.sourceId || null, sourceName: c.sourceName || null, index: c.index || 0,
          areaPt2: +(+c.areaPt2 || 0).toFixed(1), widthPt: +(+c.widthPt || 0).toFixed(2), heightPt: +(+c.heightPt || 0).toFixed(2), excluded: false, open: false }, PP && PP.fieldsOf ? PP.fieldsOf(c) : {})) };
    });
    if (!sheet) { ok('E sheet', false, 'no sheet with two placed pieces'); return; }
    st.put('Charm_Nest_Sheets', sheet.id, Object.assign({}, st.doc('Charm_Nest_Sheets', sheet.id) || {}, { id: sheet.id, runId: R, metal: sheet.metal, fileBase: sheet.fileBase, status: 'complete', layoutRev: 2, contentRev: 2,
      poolIds: sheet.charms.map(c => c.poolId), placements: sheet.placements, charms: sheet.charms }));
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: Date.now() - 3600e3 });
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    const [A, Bc] = sheet.charms;
    // the page is a viewer now: no run held, the run followed, its workspace frozen
    const v0 = await page.evaluate(R => {
      B.run = null; const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: R });
      window.CNViewer = window.CNViewer || {}; window.__viewerOn = CNViewer.on; CNViewer.on = () => true; Session.freeze();
      window.__said = []; const t0 = window.toast; window.toast = function (t) { window.__said.push(String(t)); return t0 && t0.apply(this, arguments); };
      return { frozen: Session.frozen() };
    }, R);
    ok('E0 the page is a viewer: its workspace frozen, no run held', v0.frozen === true, v0);
    // the piece dialog, as a click on the charm opens it; its Pin pressed
    const dialog = (poolId, press) => page.evaluate(({ id, poolId, press }) => {
      const sh = allSheets().find(s => s.sheetId === id), c = sh && sh.charms.find(x => x.poolId === poolId); if (!c) return { none: true };
      openCharm(sh, c); const b = document.querySelector('#ciPin'), label = b ? b.textContent.trim() : null;
      if (press && b) b.click(); else { const d = document.querySelector('#dlgReport'); if (d && d.open) closeDlg(d); }
      return { label, pinned: !!c.pinned, manual: c.manual || null };
    }, { id: sheet.id, poolId, press });
    const d1 = await dialog(A.poolId, true);
    const kept = await until(() => { const rc = (st.doc('Charm_Nest_Sheets', sheet.id).charms || []).find(c => c.poolId === A.poolId); return rc && rc.manual ? rc : null; }, 15000);
    const said1 = await until(() => page.evaluate(() => window.__said.find(t => /pinned — saved in the cloud/.test(t)) || null), 5000);
    const recNow = st.doc('Charm_Nest_Sheets', sheet.id);
    ok('E1 Pin on the viewer: the dialog offered "Pin here"; the press reaches the cloud and the sheet\'s record keeps the person\'s pin at its own pose; the person is told it is saved in the cloud',
      d1.label === 'Pin here' && !!kept && kept.manual.by === 'Test Operator' && kept.pinned && kept.pinned.cxPt === sheet.placements.find(p => p.id === A.id).cxPt && !!said1 && presses.length === 1 && !presses[0].poses && presses[0].pins[A.id].pinned === true
        && JSON.stringify(recNow.placements) === JSON.stringify(sheet.placements) && !recNow.filesOwed,
      { d1, kept, said1, presses: presses.map(p => p.pins), filesOwed: recNow.filesOwed });
    // a reload (and every other computer): the sheet is read again from the cloud and rebuilt from the master files and its record; this page's memory of the pin is gone
    const re = await page.evaluate(async id => { const sh = allSheets().find(s => s.sheetId === id); for (const c of sh.charms) { delete c.manual; c.pinned = null; }
      try { await CharmNestCloudSheets.reload(sh, 'item 12 test: read again as after a reload'); } catch (e) { return { error: String(e && e.message || e) }; }
      return { n: sh.charms.length, placed: sh.placements.length }; }, sheet.id);
    const d2 = await dialog(A.poolId, false), d3 = await dialog(Bc.poolId, false);
    ok('E2 read again from the cloud (as after a reload, and on every other computer): the pinned charm says "Unpin", the other "Pin here"',
      !re.error && re.n >= 2 && d2.label === 'Unpin' && d2.manual && d2.manual.by === 'Test Operator' && d3.label === 'Pin here', { re, d2, d3 });
    const d4 = await dialog(A.poolId, true);
    const gone = await until(() => { const rc = (st.doc('Charm_Nest_Sheets', sheet.id).charms || []).find(c => c.poolId === A.poolId); return rc && !rc.manual && !rc.pinned ? rc : null; }, 15000);
    const d5 = await dialog(A.poolId, false);
    ok('E3 Unpin there takes the pin off the cloud\'s record, and the dialog says "Pin here" again', d4.label === 'Unpin' && !!gone && d5.label === 'Pin here' && presses.length === 2 && presses[1].pins[A.id].pinned === false, { d4, gone: !!gone, d5, presses: presses.map(p => p.pins) });
    // mode off: the page's own pin, as before (no press, the record untouched)
    await page.evaluate(() => { CNViewer.on = () => false; });
    // (as before, a charm read again from its record is held where it is: its dialog may first offer Unpin; each press turns the page's own pin over)
    const before = presses.length, recB = JSON.stringify((st.doc('Charm_Nest_Sheets', sheet.id).charms || []).find(c => c.poolId === Bc.poolId));
    const o1 = await dialog(Bc.poolId, true), o2 = await dialog(Bc.poolId, true);
    await new Promise(r => setTimeout(r, 1500));
    const o3 = await dialog(Bc.poolId, false), pinPress = o1.label === 'Pin here' ? o1 : o2, after = o1.label === 'Pin here' ? o2 : o3;
    ok('E4 mode off: Pin is the page\'s own as before: Pin here gives "Unpin" (pinned on this page, no mark of a person), each press turns it over, no press sent, the cloud\'s record untouched',
      o1.label !== o2.label && o2.label !== o3.label && pinPress.label === 'Pin here' && after.label === 'Unpin' && after.pinned === true && !after.manual && presses.length === before
        && JSON.stringify((st.doc('Charm_Nest_Sheets', sheet.id).charms || []).find(c => c.poolId === Bc.poolId)) === recB, { o1, o2, o3, sent: presses.length - before });
    await page.evaluate(() => { CNViewer.on = window.__viewerOn; });
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  // (part S in a process of its own: the fake site of part E and the offline cloud of part S each load the library with their own database, and one process keeps one copy of it)
  if (process.env.VA_PART === 'offline') await partS();
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { VA_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0(out.slice(0, 2000)); }
  }
  log0(failed ? `FAIL viewer-item12 (${failed})` : 'PASS viewer-item12');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item12: ' + (e && e.stack || e)); process.exit(1); });
