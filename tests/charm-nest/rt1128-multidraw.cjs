/* MULTIDRAW (Paul, 28 Nov 2026, GF Draft 2, custom order 4253009906): ONE dropped file drawing 8 charms (two snow globes, teacups, teapots); its 8 pieces were made each as its own charm
   (pool rows with customIndex, MULTICHARM) and nested by their own shapes, but the sheet showed all 8 as the snow globe, charm 0 of the file, over each other and their neighbours, while
   the cloud's own picture of the sheet had them right. A 12-charm "Donut1" file drew 12 copies of its first donut. The cloud put the pieces on the sheet as charm 0 (charm-nest-place.js
   charmOfRow: index 0, no customIndex) with the hash ITS drawing gives each charm, which a page never draws. Offline, Node only (the real reader and piece maker over a synthetic file of
   five charms, two of them squares of one size told apart only by where their hole is; the page's own functions read from their files; the real server modules over _cloudFakes.cjs; no
   network, no Firebase):
     R  the record: charmOfRow names the charm of its file a custom piece is (index and customIndex); a master piece and a custom piece of no charm are written as before
     K  the pick (CharmNestPoolPieces.customIndexOf / pickCharm): customIndex first and exactly, then the hash, then the one charm of its own size, then the index; a file of one charm as before
     W  the sheet window (charm-nest-sheetwin.js piecesOf, sourceGeom, fileCharm): a record the cloud nested before this (index 0, no customIndex, hashes no page draws) draws each piece as
        its own charm, from the rows this page holds, else from ONE poolGet of that file's pieces (asked once); a record written now asks nothing; without any row the sizes still tell the
        charms of different sizes apart; a file of one charm and a dropped file are drawn as before
     P  the page's rebuild of a sheet from the cloud's answer (charm-nest-bridge.js charmOfRecord): each piece its own charm from the answer's rows, never the whole file folded into one
     C  the cloud's sheet writer (_charmNestFinishHost.js makePieces, the laser files): each piece its own charm from its pool row (as before: this part passes on the old code too), and
        the .ai written from them draws the five charms' sizes, not five copies of one
     G  the cloud's green-line shapes (_charmNestRoseShapes.js charmsOf): a custom piece of a file of several charms is its own charm (it was the whole file: refused as "changed")
   node tests/charm-nest/rt1128-multidraw.cjs */
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
const F = require('./_cloudFakes.cjs')({ now: Date.UTC(2026, 10, 28, 15, 0, 0) });
const log0 = console.log.bind(console); if (!process.env.MD_LOUD) { console.log = () => {}; console.warn = () => {}; }
let n = 0, failed = 0; const ok = (m, c, d) => { n++; log0((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };
const part = async (name, fn) => { try { await fn(); } catch (e) { ok(name + ' ran to its end', false, String((e && e.stack) || e).slice(0, 900)); } };
global.self = global;
const Pdf = require(path.join(ROOT, 'netlify/functions/_charmNestPdf.js')).withCanvas().CharmNestPDF;
const PP = require(path.join(ROOT, 'charm-nest-pool-pieces.js'));
const Place = require(path.join(ROOT, 'charm-nest-place.js'));
const PoolMake = require(path.join(ROOT, 'charm-nest-pool-make.js'));
const O = require(path.join(ROOT, 'charm-nest-orders.js'));
const Pair = require(path.join(ROOT, 'charm-nest-pair.js'));

const RID = '4260112801', TX = '42601128011', KEY = `${RID}_${TX}`, FILE = `charmnest/custom/${RID}/d349ab27875e-${RID}.pdf`, SRC = 'pool:' + FILE.replace(/[^\w]+/g, '_');
const near = (b, want, tol = 1.5) => Array.isArray(b) && Array.isArray(want) && b.length === 4 && b.every((v, i) => Math.abs(v - want[i]) <= tol);

/* ── the file: five charms. 0 a 40 pt square with its hole at the left (the "snow globe": charm 0), 1 the same square with its hole at the right, 2 a disc, 3 a wide bar, 4 a tall bar ── */
async function fiveCharms() {
  const { PDFDocument, rgb } = require(path.join(ROOT, 'vendor/pdf-lib-1.17.1.min.js'));
  const doc = await PDFDocument.create(), page = doc.addPage([380, 120]);
  const rect = (x, y, w, h) => page.drawSvgPath(`M 0 0 L ${w} 0 L ${w} ${h} L 0 ${h} Z`, { x, y: y + h, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  const circ = (cx, cy, r) => page.drawCircle({ x: cx, y: cy, size: r, borderColor: rgb(0, 0, 0), borderWidth: 0.25 });
  rect(20, 30, 40, 40); circ(30, 50, 3);
  rect(80, 30, 40, 40); circ(110, 50, 3);
  circ(160, 50, 18);
  rect(200, 40, 60, 24);
  rect(290, 25, 24, 60);
  return new Uint8Array(await doc.save());
}

/* ── the page's piece maker in node (a page's host: not the cloud) ── */
function pageMaker(bytes, rows) {
  const SS = { settings: { minPt: 6, silhouetteRes: 6 }, poolSources: {} };
  const BB = { pool: { rows: rows || new Map(), sources: new Map() }, master: { entries: new Map() }, run: null, orders: { byKey: new Map() } };
  const reads = [];
  const maker = PoolMake.create({
    window: { CharmNestPair: Pair }, S: SS, B: BB, O, P: Pdf, MM: 25.4 / 72, PoolPieces: PP,
    Master: { entryFor: () => null, fetchEntry: async () => null, keepOutOf: () => null, skuRegex: () => Pdf.SKU_PATTERN_DEFAULT },
    agent: () => {}, api: async (fn, body) => { if (fn === 'charmNestOutput' && body && body.op === 'url') return { ok: true, url: 'mem:' + body.path }; throw new Error('no call ' + fn); },
    stockFor: () => ({ wPt: 1e9, hPt: 1e9 }), labelOf: m => m, now: () => Date.now(),
    CharmNestAssets: () => ({ bytes: async url => { reads.push(url); if (String(url) !== 'mem:' + FILE && String(url) !== FILE) throw new Error('not the file: ' + url); return bytes; } }),
    keptMaster: async () => null, keepMaster: () => {}, charmOf: () => null, pinPooled: () => {}, greenHeld: () => false, cancelled: () => false,
    Orders: () => null, Review: () => null, CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => undefined, cloud: false
  });
  return { maker, B: BB, S: SS, reads };
}

/* ── a slice of a page file, from a line that starts it to a line that ends it (the end line not included) ── */
function slice(file, from, to) {
  const src = fs.readFileSync(path.join(ROOT, file), 'utf8'), a = src.indexOf(from), b = src.indexOf(to, a + from.length);
  if (a < 0 || b < 0) throw new Error(`${file}: "${from.trim().slice(0, 60)}" .. "${to.trim().slice(0, 60)}" not found`);
  return src.slice(a, b);
}
function blockOf(file, head) {   // a function from its head line to its closing "  }" at the same depth
  const src = fs.readFileSync(path.join(ROOT, file), 'utf8'), a = src.indexOf(head); if (a < 0) throw new Error(`${file}: ${head.trim()} not found`);
  const b = src.indexOf('\n  }\n', a); return src.slice(a, b + 4);
}

(async () => {
  const bytes = await fiveCharms();
  // the cloud's own reading of the file (the piece maker of a custom send: _charmNestCustomMake.js), the charms its pool rows are counted in
  const Make = require(path.join(ROOT, 'netlify/functions/_charmNestCustomMake.js'));
  const cloudRead = await Make.create({ sends: new Map(), pdf: () => Pdf, bytes: async () => bytes, settings: { minPt: 6, silhouetteRes: 6 } }).read({ id: 'f1', name: 'five.pdf', cloud: { path: FILE } });
  const CH = cloudRead.charms.map(c => ({ bbox: c.bbox.slice(), widthPt: +c.widthPt.toFixed(2), heightPt: +c.heightPt.toFixed(2), areaPt2: +(+c.areaPt2).toFixed(1) }));
  ok('0 the file reads as five charms, the first two one size', CH.length === 5 && Math.abs(CH[0].widthPt - CH[1].widthPt) < 0.01 && Math.abs(CH[0].heightPt - CH[1].heightPt) < 0.01 && CH[0].bbox[0] < CH[1].bbox[0], CH);
  const WANT = [0, 1, 2, 3, 4, 1];   // (six pieces: each charm once, the second square twice)
  const rowOf = k => ({ poolId: `${KEY}_${k + 1}`, orderId: RID, transactionId: TX, lineKey: KEY, sku: 'CUSTOM', material: 'gold', copy: k + 1, quantity: WANT.length, custom: true, customFile: 'five.pdf', aiPath: FILE,
    state: 'nested', sheetId: 'gold-md', charmHash: 'n0de' + WANT[k], customIndex: WANT[k], widthPt: CH[WANT[k]].widthPt, heightPt: CH[WANT[k]].heightPt, areaPt2: CH[WANT[k]].areaPt2 });
  const rows = WANT.map((_, k) => rowOf(k));
  // a record as the cloud nested it BEFORE this fix: every piece charm 0 of the file, no customIndex, the hash the cloud's drawing gave (no page draws it), its own size
  const legacyCharm = r => { const c = Place.charmOfRow(r, { widthPt: r.widthPt, heightPt: r.heightPt, areaPt2: r.areaPt2 }, Date.now()); c.index = 0; delete c.customIndex; return c; };
  const at = k => ({ id: `${SRC}:${KEY}_${k + 1}`, cxPt: 30 + 45 * k, cyPt: 40, angle: 0, scale: 0.971, wPt: 40, hPt: 40 });
  const recOf = charms => ({ id: 'gold-md', metal: 'gold', poolIds: rows.map(r => r.poolId), charms, placements: charms.map((_, k) => at(k)), sources: [] });
  const legacy = recOf(rows.map(legacyCharm));

  await part('R', async () => {
    const c = rows.map(r => Place.charmOfRow(r, { widthPt: r.widthPt, heightPt: r.heightPt, areaPt2: r.areaPt2 }, 1));
    ok('R1 a custom piece\'s sheet charm names the charm of its file it is (index and customIndex)', c.every((x, k) => x.index === WANT[k] && x.customIndex === WANT[k]), c.map(x => [x.index, x.customIndex]));
    const m = Place.charmOfRow({ poolId: '4260112802_1_1', orderId: '4260112802', sku: 'OCTOPUS 4', aiPath: 'charmnest/master/OCTOPUS_4.ai', customIndex: 3, quantity: 1 }, { widthPt: 30, heightPt: 30, areaPt2: 500 }, 1);
    const o = Place.charmOfRow(Object.assign({}, rows[2], { customIndex: undefined }), {}, 1);
    ok('R2 a master piece is written as before (index 0, no customIndex, whatever its row says); a custom piece that names no charm too', m.index === 0 && !('customIndex' in m) && o.index === 0 && !('customIndex' in o), { m: [m.index, m.customIndex], o: [o.index, o.customIndex] });
  });

  await part('K', async () => {
    ok('K0 the pick is shared (CharmNestPoolPieces.customIndexOf, pickCharm)', typeof PP.customIndexOf === 'function' && typeof PP.pickCharm === 'function');
    const cs = [{ hash: 'a', widthPt: 40, heightPt: 40 }, { hash: 'b', widthPt: 40, heightPt: 40 }, { hash: 'c', widthPt: 36, heightPt: 36 }, { hash: 'd', widthPt: 60, heightPt: 24 }];
    const pick = (x, ...more) => { const r = PP.pickCharm(cs, x, ...more); return r ? [cs.indexOf(r.charm), r.by] : null; };
    ok('K1 customIndex first, exactly (its record\'s, else its row\'s), whatever the hash and the index say', JSON.stringify(pick({ customIndex: 1, hash: 'a', index: 0 })) === '[1,"customIndex"]' && JSON.stringify(pick({ hash: 'zz', index: 0 }, { customIndex: 3 })) === '[3,"customIndex"]');
    ok('K2 a customIndex the file does not have is no charm (never another)', pick({ customIndex: 7, index: 0 }) === null);
    ok('K3 else the hash (and the drawing under a Charm Editor tag)', JSON.stringify(pick({ hash: 'c', index: 0 })) === '[2,"hash"]' && JSON.stringify(pick({ hash: 'd~ae12', index: 0 })) === '[3,"hash"]');
    const cust = { hash: 'n0de', index: 0, custom: true, poolId: 'x_1_1' };
    ok('K4 else, for an order\'s custom piece, the one charm of its own size (within 3 %)', JSON.stringify(pick(Object.assign({ widthPt: 36.4, heightPt: 35.8 }, cust))) === '[2,"size"]' && JSON.stringify(pick(Object.assign({ widthPt: 60, heightPt: 24 }, cust))) === '[3,"size"]');
    const sq = i => JSON.stringify(pick(Object.assign({ widthPt: 40, heightPt: 40 }, cust, { index: i })));
    ok('K5 two charms of one size: the one its index names among them, else the nearest of that size (never a charm of another size)', sq(1) === '[1,"size"]' && sq(0) === '[0,"size"]' && sq(3) === '[0,"size"]', [sq(0), sq(1), sq(3)]);
    const tw = [{ hash: 'a' }, { hash: 'a' }, { hash: 'c' }], tie = i => cs.indexOf.call(tw, (PP.pickCharm(tw, { hash: 'a', index: i }) || {}).charm);
    ok('K5b two charms of one outline share its hash: the one its index names, else the first', tie(1) === 1 && tie(0) === 0 && tie(2) === 0, [tie(0), tie(1), tie(2)]);
    ok('K6 a piece that is not an order\'s custom piece (a dropped file) keeps the index; a file of one charm its only charm', JSON.stringify(pick({ hash: 'zz', index: 2, widthPt: 60, heightPt: 24 })) === '[2,"index"]' && (PP.pickCharm([cs[0]], { customIndex: 3, hash: 'zz', index: 5 }) || {}).by === 'only');
    ok('K7 customIndexOf: the first that says it; nothing else counts', PP.customIndexOf(null, {}, { customIndex: '2' }, { customIndex: 4 }) === 2 && PP.customIndexOf({ customIndex: -1 }, { customIndex: 1.5 }, { customIndex: '' }) === null);
  });

  // the sheet window's own functions, run as they stand (piecesOf, sourceGeom, fileCharm and what they call)
  const W_FILE = 'charm-nest-sheetwin.js';
  function sheetWindow(env) {
    const code = blockOf(W_FILE, '  function piecesOf(rec) {') + '\n' + slice(W_FILE, '  const customOf = c => {', '\n  /** A live sheet as a record') + '\n' + blockOf(W_FILE, '  async function sourceGeom(s) {') + '\n'
      + slice(W_FILE, '  const hashCore = h =>', '  async function attachGeom(') + '\nreturn { piecesOf, sourceGeom, fileCharm };';
    const names = ['window', 'api', 'poolRowOf', 'withOwnArt', 'CharmNestPair', 'keptGeom', 'Master', 'Pool', 'CharmNestAssets', 'CharmNestPDF', 'S', 'fileGeoms', 'inPlaceFitOf'];
    return new Function(...names, code)(...names.map(k => env[k]));
  }
  function windowEnv(o = {}) {
    const pm = pageMaker(bytes, o.held), calls = [];
    const win = { CharmNestPoolPieces: PP, CharmNestPair: Pair, Pool: { masterCharm: pm.maker.masterCharm, ensureBase: pm.maker.ensureBase, cloneCharm: pm.maker.cloneCharm }, CNSheetLive: o.live || null };
    const env = {
      window: win, CharmNestPair: Pair, Pool: win.Pool, Master: { entryFor: () => null, fetchEntry: async () => null }, keptGeom: async () => null, fileGeoms: new Map(), inPlaceFitOf: () => 0,
      CharmNestAssets: { bytes: async url => { if (String(url) !== 'mem:' + FILE) throw new Error('not the file ' + url); return bytes; } }, CharmNestPDF: Pdf, S: { settings: { minPt: 6, silhouetteRes: 6 } },
      withOwnArt: async c => c, poolRowOf: id => (o.held && o.held.get(id)) || null,
      api: async (fn, body) => { calls.push([fn, body && body.op, body && body.poolIds ? body.poolIds.slice() : null]); if (fn === 'charmNestOutput' && body.op === 'url') return { url: 'mem:' + body.path };
        if (fn === 'charmNestLibrary' && body.op === 'poolGet') { if (o.noRows) throw new Error('offline'); return { pools: Object.fromEntries(rows.filter(r => body.poolIds.includes(r.poolId)).map(r => [r.poolId, r])) }; } throw new Error('no call ' + fn); }
    };
    return { env, calls, pm };
  }
  async function drawWith(rec, o) {
    const { env, calls } = windowEnv(o), SW = sheetWindow(env), pieces = SW.piecesOf(rec), srcs = PP.sourcesOf(rec), got = [];
    for (const x of pieces) { const g = await SW.sourceGeom(srcs.find(s => s.id === x.sourceId)); const c = await SW.fileCharm(g, x, rec); got.push(c ? c.bbox : null); }
    return { got, calls, SW, env, pieces };
  }
  const right = got => got.every((b, k) => near(b, CH[WANT[k]].bbox));

  await part('W', async () => {
    const held = new Map(rows.map(r => [r.poolId, r]));
    const a = await drawWith(legacy, { held });
    ok('W1 a record the cloud nested before (index 0, no customIndex, hashes no page draws), its rows held here: each piece is its own charm, nothing asked', right(a.got) && !a.calls.some(c => c[1] === 'poolGet'), { got: a.got, want: WANT.map(i => CH[i].bbox), calls: a.calls });
    const b = await drawWith(legacy, {});
    const asks = b.calls.filter(c => c[1] === 'poolGet');
    ok('W2 the same record on a page that holds no row: ONE poolGet of that file\'s pieces, and each piece its own charm (the two squares told apart)', right(b.got) && asks.length === 1 && JSON.stringify(asks[0][2]) === JSON.stringify(rows.map(r => r.poolId).sort()), { got: b.got, asks });
    const again = []; for (const x of b.pieces) { const g = await b.SW.sourceGeom(PP.sourcesOf(legacy).find(s => s.id === x.sourceId)); again.push((await b.SW.fileCharm(g, x, legacy) || {}).bbox); }
    ok('W3 drawn again in the same window: asked no more', right(again) && b.calls.filter(c => c[1] === 'poolGet').length === 1, b.calls.filter(c => c[1] === 'poolGet').length);
    const now = recOf(rows.map(r => Place.charmOfRow(r, { widthPt: r.widthPt, heightPt: r.heightPt, areaPt2: r.areaPt2 }, 1)));
    const c = await drawWith(now, {});
    ok('W4 a record written now (its charms name their charm): each its own charm, nothing asked', right(c.got) && !c.calls.some(x => x[1] === 'poolGet'), { got: c.got, calls: c.calls });
    const live = { pools: id => (id === 'gold-md' ? Object.fromEntries(rows.map(r => [r.poolId, r])) : null) };
    const d = await drawWith(legacy, { live });
    ok('W5 the rows the one copy\'s answer brought are read before anything is asked', right(d.got) && !d.calls.some(x => x[1] === 'poolGet'), { got: d.got, calls: d.calls });
    const e = await drawWith(legacy, { noRows: true });
    ok('W6 no row to be had: the charms of a size of their own are still told by their size (disc, bars); never all charm 0', [2, 3, 4].every(k => near(e.got[k], CH[WANT[k]].bbox)) && near(e.got[0], CH[0].bbox), { got: e.got });
    // a file of ONE charm, and a file of several dropped on a page (no order's piece): drawn exactly as before
    const oneRec = { id: 'gold-one', metal: 'gold', charms: [Object.assign(legacyCharm(rows[2]), { id: 'one:1', poolId: `${KEY}_9`, hash: 'zz' })], placements: [Object.assign(at(0), { id: 'one:1' })], sources: [] };
    const { env: e1 } = windowEnv({}), S1 = sheetWindow(e1);
    const onlyG = { pool: false, charms: [cloudRead.charms[2]].map(x => Object.assign({}, x, { custom: true })), file: { path: FILE } };
    const one = await S1.fileCharm(onlyG, S1.piecesOf(oneRec)[0], oneRec);
    const dropRec = { id: 'gold-drop', metal: 'gold', charms: [{ id: 'drop:3', sourceId: 'drop', sourceName: 'five.ai', index: 3, hash: 'zz', aiPath: FILE, widthPt: 40, heightPt: 40 }], placements: [Object.assign(at(0), { id: 'drop:3' })], sources: [{ id: 'drop', name: 'five.ai', path: FILE }] };
    const dg = await S1.sourceGeom(dropRec.sources[0]), drop = await S1.fileCharm(dg, S1.piecesOf(dropRec)[0], dropRec);
    ok('W7 a file of one charm is its only charm; a dropped file of several keeps the charm at its index (as before)', near(one && one.bbox, CH[2].bbox) && near(drop && drop.bbox, CH[3].bbox), { one: one && one.bbox, drop: drop && drop.bbox });
    // a record a page wrote before (its pieces carry the hash the page's own reading gives them, no customIndex): drawn from its hash as before, nothing asked
    const plain = (await S1.sourceGeom(PP.sourcesOf(legacy)[0])).charms, hashOf = i => (plain.find(c => near(c.bbox, CH[i].bbox)) || {}).hash;
    const paged = recOf(rows.map((r, k) => Object.assign(legacyCharm(r), { hash: hashOf(WANT[k]), index: WANT[k] })));
    const f = await drawWith(paged, {});
    ok('W8 a record a page wrote (its own hashes, no customIndex): each piece by its hash, nothing asked', right(f.got) && !f.calls.some(x => x[1] === 'poolGet') && WANT.every(i => !!hashOf(i)), { got: f.got, calls: f.calls });
  });

  await part('P', async () => {
    const code = slice('charm-nest-bridge.js', '  async function charmOfRecord(d, rc, row) {', '  async function rebuildOnce(pg, opts) {') + '\nreturn charmOfRecord;';
    const pm = pageMaker(bytes, new Map());   // (NOSTORE: the page holds no row of these pieces; the cloud's answer brings them)
    const Pool = { masterCharm: pm.maker.masterCharm, ensureBase: pm.maker.ensureBase, cloneCharm: pm.maker.cloneCharm, sizeEntry: pm.maker.sizeEntry };
    const charmOfRecord = new Function('Review', 'Master', 'CharmNestPoolPieces', 'Pool', code)({ skuNowOf: () => null }, { entryFor: () => null }, PP, Pool);
    const d = Object.assign({}, legacy, { pools: Object.fromEntries(rows.map(r => [r.poolId, r])) });
    const got = [];
    for (const rc of d.charms) { const c = await charmOfRecord(d, rc, d.pools[rc.poolId]); got.push(c ? c.bbox : null); }
    ok('P1 a sheet built on the page from the cloud\'s answer: each piece its own charm (it was the whole file folded into one, for every piece)', right(got), { got, want: WANT.map(i => CH[i].bbox) });
  });

  await part('C', async () => {
    F.blobs.set(FILE, { buf: Buffer.from(bytes), meta: { contentType: 'application/pdf', size: bytes.length, metadata: {} } });
    for (const r of rows) F.seed('Charm_Pool/' + r.poolId, r);
    const Host = require(path.join(ROOT, 'netlify/functions/_charmNestFinishHost.js'));
    const made = await Host.makePieces({ db: F.db, bucket: F.bucket, rec: legacy, S: { settings: Object.assign({}, Host.DEFAULTS, { minPt: 6, silhouetteRes: 6 }) }, read: async () => ({ ok: true, records: {}, edits: {} }), log: () => {} });
    const byPool = new Map(made.charms.map(c => [c.poolId, c]));
    ok('C1 the cloud\'s sheet writer makes each piece from its own charm (the laser files), from the pool rows', !made.missing.length && rows.every((r, k) => near(byPool.get(r.poolId) && byPool.get(r.poolId).bbox, CH[WANT[k]].bbox)), { missing: made.missing, got: rows.map(r => byPool.get(r.poolId) && byPool.get(r.poolId).bbox) });
    // the .ai written from them (the writer's sheetSpec → buildSheet), read back: the pieces' own sizes, not six of one
    const items = rows.map((r, k) => byPool.get(r.poolId)), sources = new Map();
    for (const c of items) if (!sources.has(c.sourceId)) sources.set(c.sourceId, made.sources[c.sourceId].parsed);
    const ai = await Pdf.buildSheet({ sheet: { wPt: 300, hPt: 150, strokeRGB: [1, 0, 0], strokePt: 0.5 }, placements: items.map((c, k) => ({ charm: c, angle: 0, cxPt: 30 + 48 * (k % 6), cyPt: k < 3 ? 40 : 100, scale: 1 })), sources, title: 'test', meta: { sheetId: 'gold-md', metal: 'gold', seed: 1, clearancePt: 0, charmScale: 1 } });
    const back = await Pdf.parseSource(ai, 'sheet.ai'), g = Pdf.groupCharms(back, { minPt: 6 });
    const sizes = g.charms.map(c => [Math.round(c.bbox[2] - c.bbox[0]), Math.round(c.bbox[3] - c.bbox[1])]).filter(([w, h]) => w < 100 && h < 100).map(String).sort();
    const want = WANT.map(i => [Math.round(CH[i].bbox[2] - CH[i].bbox[0]), Math.round(CH[i].bbox[3] - CH[i].bbox[1])]).map(String).sort();
    ok('C2 the written .ai draws the six pieces at their own charms\' sizes', JSON.stringify(sizes) === JSON.stringify(want), { sizes, want });
  });

  await part('G', async () => {
    const Shapes = require(path.join(ROOT, 'netlify/functions/_charmNestRoseShapes.js'));
    const rose = Object.assign({}, legacy, { id: 'rose-md', metal: 'rose', charms: legacy.charms.map(c => Object.assign({}, c, { metal: 'rose' })) });
    let map = null, err = null; try { map = await Shapes.charmsOf({ db: F.db, bucket: F.bucket, read: async () => ({ ok: true, records: {}, edits: {} }) }, rose); } catch (e) { err = (e && e.message) || String(e); }
    const got = rose.charms.map(c => map && map.get(c.id) && map.get(c.id).bbox);
    ok('G1 the green line\'s shapes: each custom piece its own charm (it was the whole file, refused as "changed since it was placed")', !err && right(got), { err, got });
  });

  log0(`\n${n - failed}/${n} passed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL the test ran to its end :: ' + ((e && e.stack) || e)); process.exit(1); });
