// CLOUD SILHOUETTE PARITY (cloud plan v2, build step 3): pieces made in node, from the same code the page uses, are the page's pieces.
//   The page draws a charm's silhouette on its canvas (charm-nest-pdf.js buildSilhouettes: the outline stroked and filled, every member on top, the pixels read
//   back and flooded from the border); the silhouette's hash is the piece's charmHash, its area what the nest and the plate check count. In node the same code
//   draws on netlify/functions/_charmNestCanvas.js (_charmNestPdf.js withCanvas()). The golden beside this file was recorded ONCE from the page's own canvas
//   (Chrome, OffscreenCanvas): `node tests/charm-nest/cloud-silhouette-parity.cjs --record` (PW_DIR, CHROMIUM). Without --record nothing but node runs.
//   1  every charm of the fixture masters (all of tests/charm-nest/fixtures/*.ai: 225 charms) traced in node: the same canvas size, every pixel that differs
//      lies on the edge of the page's silhouette (one pixel from a pixel of the other value), the same hash (charm-nest-pdf.js buildSilhouettes), the area within 1 %
//   2  lines made up into pieces in node by charm-nest-pool-make.js (makePool: a plain charm, two copies, earring pairs with the Right turned over, a master
//      that split in two folded back): the pool rows byte for byte the page's, every piece's silhouette as in 1
//   3  a node host (cloud: true): each row says which body it is cut from (bodyIndex, oneBody) and is otherwise the page's row; a 429 or an Etsy stop never
//      passes by itself there, while the page's rule is unchanged; the pace is asked between the pieces of a line
//   4  Etsy: the piece maker and the canvas carry no SKU-table or inventory code and require no Etsy module; while node made every piece above, no Etsy module
//      was loaded, fetch was never called (a tripwire), and the host's api saw no call at all (so no listing SKU table without cacheOnly, no inventory call)
//   Each drawing way of the canvas that is here is checked: "js" always, "resvg" when @resvg/resvg-js is installed (it says so when it is not).
//   node tests/charm-nest/cloud-silhouette-parity.cjs        Offline: fixture files only, no network, no Etsy.
'use strict';
const fs = require('fs'), path = require('path'), zlib = require('zlib'), assert = require('assert');
const root = path.join(__dirname, '../..');
const GOLDEN = path.join(__dirname, 'cloud-silhouette-parity.golden.json');
const FIX = path.join(__dirname, 'fixtures');
const DESIGNS = fs.readdirSync(FIX).filter(f => /\.ai$/i.test(f)).sort();
const PIECE_FILES = DESIGNS.filter(f => fs.statSync(path.join(FIX, f)).size < 100000);   // (the single designs and the two-charm master: the big multi-SKU masters are traced in 1)
const SCENARIOS = [
  { id: 'plain-tinybar', sku: 'TINY_BAR_7427', form: 'necklace', quantity: 1 },
  { id: 'plain-calendar-x2', sku: 'CALENDAR_86188', form: 'necklace', quantity: 2 },
  { id: 'plain-perched', sku: 'PERCHED_43564', form: 'pendant', quantity: 1 },
  { id: 'plain-schro', sku: 'SCHRODINGER_CAT', form: 'necklace', quantity: 1 },
  { id: 'plain-schro-1', sku: 'SCHRODINGER_CAT_1', form: 'necklace', quantity: 1 },
  { id: 'plain-schro-2', sku: 'SCHRODINGER_CAT_2', form: 'necklace', quantity: 1 },
  { id: 'plain-tag', sku: 'TINY_INITIAL_TAG_1', form: 'necklace', quantity: 1 },
  { id: 'plain-celestial', sku: 'CELESTIAL3-COMET', form: 'necklace', quantity: 1 },
  { id: 'plain-5x8', sku: 'MASTER_5X8_5', form: 'necklace', quantity: 1 },
  { id: 'folded-broken-heart', sku: 'SKUs_CORRECTED_Broken_Heart', form: 'necklace', quantity: 1 },
  { id: 'earrings-comet', sku: 'COMET_9427', form: 'earrings', quantity: 1 },
  { id: 'earrings-perched-x2', sku: 'PERCHED_43564', form: 'earrings', quantity: 2 },
  { id: 'earrings-tennis-ball', sku: 'TENNIS_BALL_HUGGIE_', form: 'earrings', quantity: 1 },
  { id: 'earrings-tennis-racket', sku: 'TENNIS_RACKET_HUGGIE_', form: 'earrings', quantity: 1 },
  { id: 'cloud-earrings-comet', sku: 'COMET_9427', form: 'earrings', quantity: 1, cloud: true },
  { id: 'cloud-plain-calendar-x2', sku: 'CALENDAR_86188', form: 'necklace', quantity: 2, cloud: true },
];

/* The one flow, run in the page (its globals) and in node (arg.mods): every charm of each master traced, then each scenario's line made up by the piece maker.
   Self-contained: the page receives it as text. */
async function flow(arg) {
  const g = arg.mods || globalThis;
  const P = g.CharmNestPDF, O = g.CharmNestOrders, PP = g.CharmNestPoolPieces, PM = g.CharmNestPoolMake, win = arg.mods ? { CharmNestPair: g.CharmNestPair } : window;
  const fromB64 = s => { if (typeof Buffer !== 'undefined') return new Uint8Array(Buffer.from(s, 'base64')); const bin = atob(s), u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i); return u; };
  const toB64 = u => { if (typeof Buffer !== 'undefined') return Buffer.from(u).toString('base64'); let s = ''; for (let i = 0; i < u.length; i += 0x8000) s += String.fromCharCode.apply(null, u.subarray(i, i + 0x8000)); return btoa(s); };
  const pack = bits => { const out = new Uint8Array(Math.ceil(bits.length / 8)); for (let i = 0; i < bits.length; i++) if (bits[i]) out[i >> 3] |= 1 << (i & 7); return toB64(out); };
  const sil = c => ({ id: c.id, w: c.w, h: c.h, hash: c.hash, areaPt2: c.areaPt2, widthPt: c.widthPt, heightPt: c.heightPt, open: c.open, bits: pack(c.bits) });
  const files = arg.files, out = { designs: {}, pieces: {} };
  for (const name of arg.designs) {
    const parsed = await P.parseSource(fromB64(files[name]), name), gr = P.groupCharms(parsed, { minPt: 6 });
    const rings = gr.charms.map(c => { try { const r = P.integrateRings(c); return `welded ${r.welded}, left ${r.left.length}`; } catch (e) { return 'error: ' + e.message; } });   // (said, so a page or node without its vector library shows)
    await P.buildSilhouettes(parsed, gr.charms, 6);
    out.designs[name] = gr.charms.map((c, i) => Object.assign(sil(c), { rings: rings[i] }));
  }
  for (const sc of arg.scenarios) {
    const entries = {};
    for (const name of arg.pieceFiles) { const sku = name.replace(/\.ai$/i, ''); entries[sku] = { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'fx:' + name, masterHash: 'mh-' + sku, upAngle: 90 }; }
    const S = { settings: { silhouetteRes: 6, minPt: 6, insetPt: 0, maxFill: 0.8 }, poolSources: {}, cloud: { ok: true } };
    const B = { pool: { rows: new Map(), sources: new Map() }, master: { entries: new Map(Object.entries(entries)) }, run: null, maps: {} };
    const calls = [], logs = [], paced = [];
    const M = PM.create({ window: win, S, B, O, P, MM: 25.4 / 72, PoolPieces: PP,
      Master: { entryFor: k => entries[k] || null, fetchEntry: async () => null, keepOutOf: () => [], skuRegex: () => /^$/ },
      agent: (...a) => { logs.push(a.slice(1).join(' ')); }, api: async (fn, body) => { calls.push([fn, body && body.op, body && body.cacheOnly]); throw new Error('no function is called here'); },
      stockFor: () => ({ wPt: 1400, hPt: 700 }), labelOf: m => m, now: () => 0,
      CharmNestAssets: () => ({ bytes: async url => fromB64(files[String(url).replace(/^fx:/, '')]) }),
      Orders: () => ({ releaseAway: () => '' }), Review: () => ({ problemText: p => p.kind }), CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => undefined,
      keptMaster: async () => null, keepMaster() {}, pinPooled() {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false,
      cloud: !!sc.cloud, ...(sc.cloud ? { pace: async (row, copy, count) => { paced.push(copy + '/' + count); } } : {}) });
    const row = { key: '4190000101_5000000101', state: 'pulled', order: { receiptId: '4190000101', createTs: 1791500000, updateTs: 1791500001 }, line: { transactionId: '5000000101', listingId: '1', sku: sc.sku, title: '' },
      spec: { designSku: sc.sku, material: 'gold', quantity: sc.quantity, size: null, form: sc.form, chain: null }, problems: [], poolIds: [], engrave: null, arrivedAt: 0 };
    const prep = await M.makePool(row, null);
    out.pieces[sc.id] = { state: row.state, reason: row.reason || null, pools: prep ? JSON.stringify(prep.pools) : null, charms: prep ? prep.charms.map(sil) : [], calls, paced };
  }
  return out;
}

const b64 = f => fs.readFileSync(path.join(FIX, f)).toString('base64');
const argOf = mods => ({ mods, files: Object.fromEntries(DESIGNS.map(f => [f, b64(f)])), designs: DESIGNS, pieceFiles: PIECE_FILES, scenarios: SCENARIOS });
const deflate = s => zlib.deflateRawSync(Buffer.from(s, 'base64'), { level: 9 }).toString('base64');
const inflate = s => zlib.inflateRawSync(Buffer.from(s, 'base64'));
const unpack = (buf, n) => { const out = new Uint8Array(n); for (let i = 0; i < n; i++) out[i] = (buf[i >> 3] >> (i & 7)) & 1; return out; };

/* ── --record: the page's own canvas (Chrome), once ── */
async function record() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || undefined, args: ['--no-sandbox'] });
  try {
    const page = await browser.newPage();
    await page.route('**/*', r => r.abort());   // (nothing leaves the page: every script is given as text below)
    await page.setContent('<!doctype html><meta charset="utf-8"><title>silhouette parity</title><body></body>');
    for (const f of ['vendor/opentype-1.3.4.min.js', 'vendor/pdf-lib-1.17.1.min.js', 'vendor/clipper-6.4.2.js', 'charm-nest-vector.js', 'charm-nest-geom.js', 'charm-nest-pdf.js', 'charm-nest-pair.js', 'charm-nest-pool-pieces.js', 'charm-nest-orders.js', 'charm-nest-pool-make.js'])
      await page.addScriptTag({ content: fs.readFileSync(path.join(root, f), 'utf8') + '\n//# sourceURL=' + f });
    const ok = await page.evaluate(() => typeof OffscreenCanvas !== 'undefined' && !!(window.opentype && window.CharmNestVector && window.CharmNestPDF && window.CharmNestPoolMake && window.CharmNestPair && window.CharmNestOrders));
    assert(ok, 'the page has its canvas and the modules');
    const out = await page.evaluate(flow, argOf(null));
    for (const list of Object.values(out.designs)) for (const c of list) c.bits = deflate(c.bits);
    for (const p of Object.values(out.pieces)) for (const c of p.charms) c.bits = deflate(c.bits);
    const golden = { about: 'Recorded once from the page\'s own canvas (Chrome OffscreenCanvas) by tests/charm-nest/cloud-silhouette-parity.cjs --record: the silhouettes charm-nest-pdf.js buildSilhouettes draws for every charm of tests/charm-nest/fixtures/*.ai, and the pieces charm-nest-pool-make.js makes of the scenarios. bits: the silhouette, one bit a pixel (row by row, low bit first), raw-deflated, base64.',
      chrome: await browser.version(), designs: out.designs, pieces: out.pieces };
    fs.writeFileSync(GOLDEN, JSON.stringify(golden) + '\n');
    console.log(`recorded ${Object.values(out.designs).reduce((n, l) => n + l.length, 0)} design charms and ${SCENARIOS.length} lines from Chrome ${golden.chrome} -> ${path.relative(root, GOLDEN)} (${Math.round(fs.statSync(GOLDEN).size / 1024)} KB)`);
  } finally { await browser.close(); }
}

/* ── node: the same flow on the node canvas, held to the golden ── */
let fails = 0;
const say = (ok, name, detail) => { if (!ok) fails++; console.log(`${ok ? '  ok  ' : '  FAIL'} ${name}${detail ? ' :: ' + detail : ''}`); };
function compareCharm(gold, mine) {
  if (!mine) return { ok: false, why: 'missing' };
  if (gold.w !== mine.w || gold.h !== mine.h) return { ok: false, why: `canvas ${mine.w}x${mine.h}, the page's ${gold.w}x${gold.h}` };
  const n = gold.w * gold.h, gb = unpack(inflate(gold.bits), n), mb = unpack(Buffer.from(mine.bits, 'base64'), n), W = gold.w, H = gold.h;
  let diff = 0, off = 0;
  for (let i = 0; i < n; i++) {
    if (gb[i] === mb[i]) continue; diff++;
    const x = i % W, y = (i - x) / W; let edge = false;
    for (let dy = -1; dy <= 1 && !edge; dy++) for (let dx = -1; dx <= 1 && !edge; dx++) { const xx = x + dx, yy = y + dy; if ((dx || dy) && xx >= 0 && yy >= 0 && xx < W && yy < H && gb[yy * W + xx] !== gb[i]) edge = true; }
    if (!edge) off++;
  }
  const dA = Math.abs(mine.areaPt2 - gold.areaPt2) / Math.max(1e-9, gold.areaPt2);
  const why = [gold.rings !== mine.rings ? `hoops ${mine.rings}, the page's ${gold.rings}` : '', off ? `${off} differing pixel(s) off the edge` : '', dA > 0.01 ? `area ${mine.areaPt2.toFixed(2)}, the page's ${gold.areaPt2.toFixed(2)}` : '', mine.id !== gold.id ? `id ${mine.id}, the page's ${gold.id}` : ''].filter(Boolean).join('; ');
  return { ok: !why, why, diff, dA, n, sameHash: mine.hash === gold.hash };
}
async function nodeRun(backend, golden) {
  // the tripwires: no fetch, no Etsy module
  const realFetch = globalThis.fetch, fetched = [];
  globalThis.fetch = async u => { fetched.push(String(u)); throw new Error('no network in the parity test: ' + u); };
  try {
    const pdf = require(path.join(root, 'netlify/functions/_charmNestPdf.js')).withCanvas({ backend });
    const canvasMod = require(path.join(root, 'netlify/functions/_charmNestCanvas.js'));
    assert.strictEqual(canvasMod.backend(), backend, 'the canvas draws with ' + backend);
    const mods = { CharmNestPDF: pdf.CharmNestPDF, CharmNestOrders: require(path.join(root, 'charm-nest-orders.js')), CharmNestPair: require(path.join(root, 'charm-nest-pair.js')), CharmNestPoolPieces: require(path.join(root, 'charm-nest-pool-pieces.js')), CharmNestPoolMake: require(path.join(root, 'charm-nest-pool-make.js')) };
    const t0 = Date.now(), out = await flow(argOf(mods)), ms = Date.now() - t0;
    console.log(`── canvas "${backend}": ${Object.values(out.designs).reduce((n, l) => n + l.length, 0)} design charms and ${SCENARIOS.length} lines made in ${ms} ms`);
    // 1 · every charm of every master
    let charms = 0, bad = [], diffPx = 0, px = 0, worstA = 0, hashes = [];
    for (const name of DESIGNS) {
      const gl = golden.designs[name] || [], ml = out.designs[name] || [];
      if (gl.length !== ml.length) { bad.push(`${name}: ${ml.length} charms, the page's ${gl.length}`); continue; }
      gl.forEach((gc, i) => { charms++; const r = compareCharm(gc, ml[i]); diffPx += r.diff || 0; px += r.n || 0; worstA = Math.max(worstA, r.dA || 0); if (!r.ok) bad.push(`${name} #${i} (${gc.hash}): ${r.why}`); else if (!r.sameHash) hashes.push(`${name} #${i} ${ml[i].hash}/${gc.hash}`); });
    }
    say(!bad.length && charms > 0, `1 · ${charms} charms of ${DESIGNS.length} masters: the page's canvas size, every differing pixel on the edge of the page's silhouette, the area within 1 %`, bad.length ? bad.slice(0, 6).join(' | ') : `${diffPx} edge pixel(s) of ${px} differ; the largest area difference ${(worstA * 100).toFixed(3)} %`);
    say(!hashes.length && charms > 0, `1h · the page's hash (the plan's criterion): ${charms - hashes.length} of ${charms} charms`, hashes.length ? `${hashes.length} differ (node/page): ${hashes.slice(0, 5).join(', ')}${hashes.length > 5 ? ' ...' : ''}` : '');
    // 2 · the pieces
    bad = []; hashes = []; let pieces = 0; const noHash = j => j && j.replace(/"charmHash":"[^"]*"/g, '"charmHash":""');
    for (const sc of SCENARIOS) {
      const gp = golden.pieces[sc.id], mp = out.pieces[sc.id];
      if (!gp) { bad.push(`${sc.id}: not in the golden (record again)`); continue; }
      if (gp.state !== mp.state || gp.reason !== mp.reason) { bad.push(`${sc.id}: ${mp.state} (${mp.reason}), the page's ${gp.state} (${gp.reason})`); continue; }
      if (noHash(gp.pools) !== noHash(mp.pools)) { bad.push(`${sc.id}: the pool rows differ: ${mp.pools && mp.pools.slice(0, 200)} | the page's ${gp.pools && gp.pools.slice(0, 200)}`); continue; }
      if (gp.charms.length !== mp.charms.length) { bad.push(`${sc.id}: ${mp.charms.length} pieces, the page's ${gp.charms.length}`); continue; }
      gp.charms.forEach((gc, i) => { pieces++; const r = compareCharm(gc, mp.charms[i]); if (!r.ok) bad.push(`${sc.id} piece ${i + 1}: ${r.why}`); else if (!r.sameHash) hashes.push(`${sc.id} piece ${i + 1}`); });
      if (gp.pools !== mp.pools && !hashes.some(h => h.startsWith(sc.id + ' '))) bad.push(`${sc.id}: the pool rows differ beyond the charm hash`);
      if (JSON.stringify(gp.paced) !== JSON.stringify(mp.paced)) bad.push(`${sc.id}: paced ${mp.paced}, the page's ${gp.paced}`);
    }
    say(!bad.length && pieces > 0, `2 · ${SCENARIOS.length} lines made up in node by the piece maker: ${pieces} pieces, the same pieces and pool rows as the page (byte for byte but for charmHash), every piece's silhouette within 1 above`, bad.slice(0, 6).join(' | '));
    say(!hashes.length && pieces > 0, `2h · each piece's charmHash is the page's (the plan's criterion): ${pieces - hashes.length} of ${pieces}`, hashes.length ? `${hashes.length} differ: ${hashes.slice(0, 6).join(', ')}` : '');
    // 3 · the node host
    {
      const rowsOf = id => JSON.parse(out.pieces[id].pools || '[]');
      const c = rowsOf('cloud-earrings-comet'), p = rowsOf('earrings-comet'), c2 = rowsOf('cloud-plain-calendar-x2'), p2 = rowsOf('plain-calendar-x2');
      const strip = r => { const x = Object.assign({}, r); delete x.oneBody; if (!x.side) delete x.bodyIndex; return x; };
      const sameElse = c.length === 2 && JSON.stringify(c.map(strip)) === JSON.stringify(p) && c2.length === 2 && JSON.stringify(c2.map(strip)) === JSON.stringify(p2);
      const said = c.every(r => r.oneBody === false && r.bodyIndex === 0) && c2.every(r => r.oneBody === false && r.bodyIndex === 0) && !p.some(r => 'oneBody' in r) && !p2.some(r => 'oneBody' in r || 'bodyIndex' in r);
      say(sameElse && said, '3a · a node host\'s rows say the body each piece is cut from (bodyIndex, oneBody); otherwise they are the page\'s rows, and the page\'s rows carry neither', JSON.stringify({ c: c.map(r => [r.bodyIndex, r.oneBody]), c2: c2.map(r => [r.bodyIndex, r.oneBody]) }));
      const PMk = mods.CharmNestPoolMake, cloud = PMk.create({ cloud: true }), page = PMk.create({});
      const e429 = Object.assign(new Error('HTTP 429'), { status: 429 }), stop = new Error('{"error":"etsy_stopped"} (429)'), hdr = Object.assign(new Error('refused'), { status: 409, headers: { get: k => (k === 'x-etsy-stopped' ? '1' : null) } }), e503 = Object.assign(new Error('HTTP 503'), { status: 503 }), slow = new Error('The cloud did not answer in time');
      const ok3 = page.passing(e429) && page.passing(stop) && !cloud.passing(e429) && !cloud.passing(stop) && !cloud.passing(hdr) && cloud.passing(e503) && cloud.passing(slow) && page.passing(e503) && PMk.passing(e429) && !cloud.passing(Object.assign(new Error('x'), { status: 429, transient: true }));
      say(ok3, '3b · in node a 429 and every Etsy stop answer are held (never passing); a 5xx or a timeout passes; the page\'s rule is unchanged (a 429 passes there)');
      say(JSON.stringify(out.pieces['cloud-plain-calendar-x2'].paced) === JSON.stringify(['2/2']) && JSON.stringify(out.pieces['cloud-earrings-comet'].paced) === JSON.stringify(['2/2']) && out.pieces['plain-calendar-x2'].paced.length === 0, '3c · the pace is asked between the pieces of a line (before piece 2 of 2), only where the host gives one');
      let threw = null; const pace = PMk.pace({ budgetMs: 1000 }); await pace(); const t1 = Date.now(); while (Date.now() - t1 < 1100) { /* the budget passes */ } try { await pace(); } catch (e) { threw = e; }
      say(!!(threw && threw.budget), '3d · past its budget the pace ends the unit ({ budget: true })');
    }
    // 4 · Etsy
    const loaded = Object.keys(require.cache).filter(k => /_etsyMailEtsy|_etsyImageCache|_charmNestEtsySku|_charmNestEtsyLink|_charmNestEtsyZodiac|_charmNestSandboxPull|_charmNestListingSkus/.test(k));
    const calls = Object.values(out.pieces).flatMap(p => p.calls);
    say(!fetched.length && !calls.length && !loaded.length, '4b · while node made every piece: fetch never called, the host\'s api never called (no listing SKU table, no inventory), no Etsy module loaded', JSON.stringify({ fetched, calls, loaded }));
    return out;
  } finally { globalThis.fetch = realFetch; }
}
function staticChecks() {
  const texts = { 'charm-nest-pool-make.js': fs.readFileSync(path.join(root, 'charm-nest-pool-make.js'), 'utf8'), 'netlify/functions/_charmNestCanvas.js': fs.readFileSync(path.join(root, 'netlify/functions/_charmNestCanvas.js'), 'utf8') };
  const banned = ['Lib.ops', 'wantTables', 'askTables', 'askTable', 'tablesLoad', 'getListingInventory', 'listingSkus', '_etsyMailEtsy', '_etsyImageCache', '_charmNestEtsySku', '_charmNestEtsyLink', '_charmNestEtsyZodiac', '_charmNestSandboxPull', 'fetch(', 'XMLHttpRequest', 'require("http', "require('http", 'etsy.com'];
  const found = []; for (const [f, t] of Object.entries(texts)) for (const b of banned) if (t.includes(b)) found.push(f + ': ' + b);
  say(!found.length, '4a · the piece maker and the node canvas carry no SKU-table, inventory, network or Etsy code', found.join(', '));
  // the page loads the piece maker before the bridge, with a ?v= token, and the public site carries it
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), at = html.indexOf('<script src="charm-nest-pool-make.js?v='), br = html.indexOf('<script src="charm-nest-bridge.js?v=');
  const pub = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
  say(at > 0 && br > at && /"charm-nest-pool-make\.js"/.test(pub), '4c · the page loads charm-nest-pool-make.js (with its ?v= token) before the bridge, and the public site publishes it');
}

(async () => {
  if (process.argv.includes('--record')) { await record(); return; }
  assert(fs.existsSync(GOLDEN), 'the golden is missing: record it once with --record (PW_DIR, CHROMIUM)');
  const golden = JSON.parse(fs.readFileSync(GOLDEN, 'utf8'));
  console.log(`golden: Chrome ${golden.chrome}, ${Object.values(golden.designs).reduce((n, l) => n + l.length, 0)} design charms, ${Object.keys(golden.pieces).length} lines`);
  staticChecks();
  await nodeRun('js', golden);
  let hasResvg = false; try { require.resolve('@resvg/resvg-js'); hasResvg = true; } catch (_) { /* not installed here */ }
  if (hasResvg) await nodeRun('resvg', golden);
  else console.log('  --   the canvas\'s "resvg" way: @resvg/resvg-js is not installed here, so only the "js" way was checked against the page');
  console.log(fails ? `FAIL cloud-silhouette-parity · ${fails} check(s)` : 'PASS cloud-silhouette-parity');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error('ERROR', e && e.stack || e); process.exit(2); });
