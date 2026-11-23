// SRCLIST (Paul, 22 Nov 2026, RG Sheet 1 of RG Nov 22 Set-2): in the sheet window 8 of its 29 pieces (6 designs) were drawn as plain boxes, all 29 right in the cloud's picture of it.
// The record's `sources` lists 17 designs; the 8 pieces the cloud put back on it (cmdRestoreNest) name a design (sourceId) it does not list, though each piece's own entry names its
// file (aiPath, aiUrl, sourceName, sku, hash). The window looked for a piece's design only in `sources`. The cloud's moves never listed the moving pieces' designs on the target.
//   P  charm-nest-pool-pieces.js on the live record (fixtures/rg-sheet1-window.json, read only): sourcesOf lists a design for every piece (the 17 as they are, 6 made from the pieces'
//      own entries, same ids); sourceOf as the readers matched (id or name), else the piece's own; carrySources: from a sheet that lists it, else the piece's own, never twice, its
//      short list as putSheet derives sourcesLite; nothing to add: null
//   C  the cloud's moves on the in-memory cloud of _cloudFakes.cjs (real library handler and ops): cmdMovePieces, cmdHopPieces and finishOverflow (to a sheet and to a new draft)
//      leave the target listing every moving piece's design (the source sheet's entry when it lists it, else the piece's own) with its short list; a target that lists them all
//      keeps its `sources` as it was. (cmdRestoreNest: rg-restore.cjs B10, on the live records.) No network, no Etsy.
//   NODE_PATH=<deps> node tests/charm-nest/sheet-sources.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 23, 1, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0;
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 1200))); if (!cond) failed++; };
const PP = require(path.join(ROOT, 'charm-nest-pool-pieces.js'));
const FX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/rg-sheet1-window.json'), 'utf8'));
// putSheet's own derivation of the short list (charmNestLibrary.js putSheet: sourcesLite)
const liteOf = list => list.slice(0, 500).filter(x => x && typeof x === 'object').map(x => ({ ...(x.name == null ? {} : { name: String(x.name).slice(0, 300) }), hash: x.hash ? String(x.hash).slice(0, 80) : null }));
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

(async () => {
  /* ═══ P · the shared reading, on the live record ═══ */
  {
    const rec = FX.sheet, listed = new Set(rec.sources.map(s => s.id)), placed = new Set(rec.placements.map(p => p.id));
    const lost = rec.charms.filter(c => placed.has(c.id) && !listed.has(c.sourceId)), lostIds = [...new Set(lost.map(c => c.sourceId))];
    ok('P0 · the live record: 29 placed pieces, 17 designs listed; 8 pieces (6 designs) name a design it does not list, each with its own file (aiPath, aiUrl)',
      rec.placements.length === 29 && rec.sources.length === 17 && lost.length === 8 && lostIds.length === 6 && lost.every(c => c.aiPath && c.aiUrl && c.sourceName), { placed: rec.placements.length, sources: rec.sources.length, lost: lost.length, lostIds });
    const has = ['sourceOfCharm', 'sourceOf', 'sourcesOf', 'carrySources'].every(k => typeof PP[k] === 'function');
    ok('P0 · charm-nest-pool-pieces.js reads a piece\'s design (sourceOfCharm, sourceOf, sourcesOf, carrySources)', has);
    if (has) {
    const all = PP.sourcesOf(rec), byId = new Map(all.map(s => [s.id, s]));
    ok('P1 · sourcesOf: a design for every piece of the record (17 + 6), the 17 listed as they are (the same entries)', all.length === 23 && rec.charms.every(c => byId.has(c.sourceId)) && rec.sources.every(s => byId.get(s.id) === s), all.length);
    ok('P1 · the 6 made from the pieces\' own entries: same id, its file (path = aiPath, url = aiUrl), its name, SKU and hash, a pool (master) design, not custom',
      lostIds.every(id => { const s = byId.get(id), c = lost.find(x => x.sourceId === id); return s && s.path === c.aiPath && s.url === c.aiUrl && s.name === c.sourceName && s.sku === c.sku && s.hash === String(c.hash) && s.pool === true && s.custom === false; }), lostIds.map(id => byId.get(id)));
    ok('P1 · the record itself is not changed', rec.sources.length === 17 && !rec.sources.some(s => lostIds.includes(s.id)));
    const bunny = lost.find(c => /BUNNY5/.test(c.sourceId)), shoe = rec.charms.find(c => /SPORTS9_RUNNING_SHOE/.test(c.sourceId));
    ok('P2 · sourceOf: a listed design is the record\'s own entry; an unlisted one the piece\'s own; one named only by its name is found by name, as the readers matched',
      PP.sourceOf(rec, shoe) === rec.sources.find(s => s.id === shoe.sourceId) && PP.sourceOf(rec, bunny).path === 'charmnest/sandbox/master/BUNNY5_-_BEADY.ai' && PP.sourceOf(rec, bunny).id === bunny.sourceId
        && PP.sourceOf(rec, { sourceId: 'x', sourceName: shoe.sourceName }) === rec.sources.find(s => s.id === shoe.sourceId));
    ok('P2 · a piece that names no file has no source (null), so a reader still says it could not be read', PP.sourceOf(rec, { sourceId: 'pool:nothing', sourceName: 'nothing' }) === null && PP.sourceOfCharm({ sourceId: 'pool:x' }) === null && PP.sourceOfCharm(null) === null);
    const book = rec.sources.find(s => /BOOK_23102/.test(s.id)), target = { sources: [book] }, from = { sources: rec.sources }, moving = [shoe, bunny, Object.assign({}, bunny, { id: 'twin' }), rec.charms.find(c => c.sourceId === book.id)];
    const carry = PP.carrySources(target, moving, [from]);
    ok('P3 · carrySources: a design the source sheet lists comes as that entry, one it does not as the piece\'s own, each once; the target\'s own first',
      carry && carry.sources.length === 3 && carry.sources[0] === book && carry.sources[1] === rec.sources.find(s => s.id === shoe.sourceId) && carry.sources[2].id === bunny.sourceId && carry.sources[2].path === bunny.aiPath, carry && carry.sources.map(s => s.id));
    ok('P3 · its short list is putSheet\'s (sourcesLite: name and hash of each)', carry && same(carry.sourcesLite, liteOf(carry.sources)), carry && carry.sourcesLite);
    ok('P3 · a target that lists every moving design: nothing to add (null); a target with no list takes them', PP.carrySources({ sources: rec.sources }, [shoe], [from]) === null && PP.carrySources({}, [bunny], []).sources.length === 1);
    }
  }

  /* ═══ C · the cloud's moves ═══ */
  const lib = F.load('charmNestLibrary.js');
  const R = 'run-2026-11-23-src', LEASE = 'Charm_Nest_Drive/lease', NONCE = 'nonce-src-1';
  const FILE_A = { id: 'pool:charmnest_master_ALPHA_ai', name: 'ALPHA (master)', hash: 'aaaa1111', bytes: 1000, path: 'charmnest/master/ALPHA.ai', url: 'https://x.test/ALPHA.ai', pool: true, custom: false, sku: 'ALPHA' };
  const FILE_Z = { id: 'pool:charmnest_master_ZULU_ai', name: 'ZULU (master)', hash: 'zzzz9999', bytes: 1000, path: 'charmnest/master/ZULU.ai', url: 'https://x.test/ZULU.ai', pool: true, custom: false, sku: 'ZULU' };
  const piece = (order, n, sku, extra) => { const key = sku.replace(/\W+/g, '_'), poolId = `${order}_${order}1_${n}`; return Object.assign({ id: `pool:charmnest_master_${key}_ai:${poolId}`, poolId, order: String(order), name: `${order} · ${sku}`, sku, hash: 'h' + order + n,
    sourceId: `pool:charmnest_master_${key}_ai`, sourceName: `${sku} (master)`, index: 0, aiPath: `charmnest/master/${key}.ai`, aiUrl: `https://x.test/${key}.ai`, areaPt2: 300, widthPt: 20, heightPt: 20, excluded: false, open: false }, extra || {}); };
  function world(metal) {
    F.reset(); F.clock.set(T0);
    F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'manual', cloudSteps: ['nest', 'files'], flippedAt: T0 - 3600e3 }); lib._driveForget();
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-23', sheets: {}, holds: {}, errors: [], orders: [], lines: {} });
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
    F.seed(LEASE, { nonce: NONCE, state: 'running', at: T0 });
  }
  function sheet(id, metal, page, charms, extra) {
    const placed = (extra && extra.unplaced) ? charms.filter(c => !extra.unplaced.includes(c.poolId)) : charms;
    const rec = Object.assign({ id, runId: R, metal, page, draft: true, setId: null, status: 'complete', layoutRev: 5, contentRev: 5, fileBase: id, charms, poolIds: charms.map(c => c.poolId),
      placements: placed.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 20, hPt: 20, scale: 1 })), orders: [...new Set(charms.map(c => c.order))], rejects: [] }, extra && extra.rec || {});
    F.seed('Charm_Nest_Sheets/' + id, rec);
    for (const c of charms) F.seed('Charm_Pool/' + c.poolId, { poolId: c.poolId, runId: R, orderId: c.order, state: 'nested', sheetId: id, material: metal, lineKey: c.poolId.replace(/_\d+$/, '') });
    return rec;
  }
  const S = id => F.doc('Charm_Nest_Sheets/' + id) || {};
  const call = (body, o) => lib.call(body, Object.assign({ unit: 'test|' + body.op + '|' + Math.random().toString(36).slice(2, 7) }, o || {}));
  const fence = { fence: { path: LEASE, nonce: NONCE, runId: R } };
  const lists = (rec, ids) => ids.every(id => (rec.sources || []).some(s => s && s.id === id));

  // C1 · cmdMovePieces (Merge sheets / Empty / ONEFILL's gather): draft F holds a piece of ALPHA (listed on F) and one of BRAVO (listed nowhere); target T lists ZULU only
  {
    world('gold');
    const a = piece(4100000001, 1, 'ALPHA'), b = piece(4100000002, 1, 'BRAVO'), z = piece(4100000009, 1, 'ZULU');
    sheet('gf-f', 'gold', 2, [a, b], { rec: { sources: [FILE_A] } }); sheet('gf-t', 'gold', 1, [z], { rec: { sources: [FILE_Z] } });
    const r = await call({ op: 'cmdMovePieces', runId: R, from: { sheetId: 'gf-f' }, to: { sheetId: 'gf-t' }, poolIds: [a.poolId, b.poolId], by: 'test' });
    const t = S('gf-t'), bS = (t.sources || []).find(s => s.id === b.sourceId);
    ok('C1 · cmdMovePieces moved the two pieces', r.ok && r.moved === 2, r);
    ok('C1 · the target lists both designs: ALPHA as the source sheet lists it, BRAVO from the piece\'s own entry (its file), ZULU kept first',
      lists(t, [FILE_Z.id, FILE_A.id, b.sourceId]) && t.sources.length === 3 && same(t.sources[0], FILE_Z) && same(t.sources.find(s => s.id === FILE_A.id), FILE_A) && bS && bS.path === b.aiPath && bS.url === b.aiUrl && bS.pool === true, t.sources);
    ok('C1 · its short list is the one putSheet derives', same(t.sourcesLite, liteOf(t.sources)), t.sourcesLite);
    ok('C1 · the sheet they left keeps its list', same(S('gf-f').sources, [FILE_A]));
    // a target that lists every moving design: its list is left as it was (nothing written to it)
    world('gold');
    sheet('gf-f', 'gold', 2, [a], { rec: { sources: [FILE_A] } }); sheet('gf-t', 'gold', 1, [z], { rec: { sources: [FILE_Z, FILE_A], sourcesLite: liteOf([FILE_Z, FILE_A]).concat([{ name: 'kept as it was', hash: null }]) } });
    const r2 = await call({ op: 'cmdMovePieces', runId: R, from: { sheetId: 'gf-f' }, to: { sheetId: 'gf-t' }, poolIds: [a.poolId], by: 'test' });
    ok('C1 · a target that lists the design already: moved, its sources and short list untouched', r2.ok && same(S('gf-t').sources, [FILE_Z, FILE_A]) && S('gf-t').sourcesLite.length === 3, S('gf-t').sourcesLite);
  }
  // C2 · cmdHopPieces (a charm dragged onto the next 14K sheet from a viewer): the hopped piece's design goes with it
  {
    world('gold14k');
    const a = piece(4200000001, 1, 'ALPHA'), b = piece(4200000002, 1, 'BRAVO'), z = piece(4200000009, 1, 'ZULU');
    sheet('k14-a', 'gold14k', 1, [a, b], { rec: { sources: [FILE_A] } }); sheet('k14-b', 'gold14k', 2, [z], { rec: { sources: [FILE_Z] } });
    const r = await call({ op: 'cmdHopPieces', runId: R, from: ['k14-a'], to: { sheetId: 'k14-b' }, poses: [{ poolId: b.poolId, cxPt: 120, cyPt: 60, angle: 0 }], person: 'Ann' });
    const t = S('k14-b');
    ok('C2 · cmdHopPieces moved the piece where it was put', r.ok && r.moved === 1 && t.placements.some(p => p.id === b.id && p.cxPt === 120), r);
    ok('C2 · the target lists its design (from the piece\'s own entry: the sheet it left never listed it), with its short list', lists(t, [FILE_Z.id, b.sourceId]) && t.sources.find(s => s.id === b.sourceId).path === b.aiPath && same(t.sourcesLite, liteOf(t.sources)), t.sources);
    const r2 = await call({ op: 'cmdHopPieces', runId: R, from: ['k14-a'], to: { sheetId: 'k14-b' }, poses: [{ poolId: a.poolId, cxPt: 160, cyPt: 60, angle: 0 }], person: 'Ann' });
    ok('C2 · a piece the source sheet lists: refused only by the hop\'s own rules, never by its design (a sheet is never emptied)', !r2.ok && /left empty/.test(r2.error || ''), r2);
  }
  // C3 · finishOverflow (the writer passes what a nest turned away to the next sheet, or to a new draft)
  {
    world('silver');
    const a = piece(4300000001, 1, 'ALPHA'), b = piece(4300000002, 1, 'BRAVO'), z = piece(4300000009, 1, 'ZULU');
    sheet('ss-a', 'silver', 1, [z, a, b], { unplaced: [a.poolId, b.poolId], rec: { sources: [FILE_Z, FILE_A], rejects: [a.id, b.id] } });
    sheet('ss-b', 'silver', 2, [piece(4300000008, 1, 'ZULU')], { rec: { sources: [FILE_Z] } });
    const r = await call({ op: 'finishOverflow', id: 'ss-a', runId: R, layoutBase: 5, contentBase: 5, pieceIds: [a.id, b.id], to: { sheetId: 'ss-b', layoutBase: 5, contentBase: 5 } }, fence);
    const t = S('ss-b');
    ok('C3 · finishOverflow moved the two turned-away pieces to the next sheet', r.ok && r.moved === 2 && t.poolIds.includes(a.poolId) && t.poolIds.includes(b.poolId), r);
    ok('C3 · the next sheet lists both designs (ALPHA as sheet A lists it, BRAVO from its own entry) with its short list', lists(t, [FILE_Z.id, FILE_A.id, b.sourceId]) && same(t.sources.find(s => s.id === FILE_A.id), FILE_A) && same(t.sourcesLite, liteOf(t.sources)), t.sources);
    world('silver');
    sheet('ss-a', 'silver', 1, [z, a, b], { unplaced: [a.poolId, b.poolId], rec: { sources: [FILE_Z, FILE_A], rejects: [a.id, b.id] } });
    const r2 = await call({ op: 'finishOverflow', id: 'ss-a', runId: R, layoutBase: 5, contentBase: 5, pieceIds: [a.id, b.id], to: { create: { metal: 'silver', page: 3, day: '2026-11-23' } } }, fence);
    const d = r2.ok && r2.to ? S(r2.to.sheetId) : {};
    ok('C3 · to a NEW draft: it is made listing both designs (and only those), with its short list', r2.ok && r2.to && r2.to.created && (d.sources || []).length === 2 && lists(d, [FILE_A.id, b.sourceId]) && same(d.sourcesLite, liteOf(d.sources)), { r2, sources: d.sources });
  }
  ok('rule · Etsy was never called; no network but our own functions', F.etsy.calls === 0 && !F.net.tripwire.length && !F.net.other.length);
  log0(failed ? `FAIL sheet-sources (${failed})` : 'PASS sheet-sources'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL sheet-sources: ' + (e && e.stack || e)); process.exit(1); });
