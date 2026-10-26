// RU-S1 (Firestore cost, 18 Oct 2026): the three learned maps (Charm_Sku_Aliases, Charm_Option_Map, Charm_Sku_NoDesign: about 3,000 + 2,000 +
// 300 documents) were read WHOLE at every page load: Orders.loadMaps (charm-nest-bridge.js) kept the last answer of each in page memory only, and
// asked for it again with its signature (ifSig / since) from then on. charm-nest-master-cache.js now keeps the last answer on this computer
// (IndexedDB cn-map-copy), so a page just opened asks with the signature of the copy and is answered `unchanged` (a handful of reads) or only
// what was written after it.
//   A · the real handlers over the cost meter's in-memory Firestore: what the whole read of the three maps costs, and what the signature ask costs
//   B · the real Sorter page over the real functions: a reload with nothing changed makes NO whole read of any map and shows the same maps; a write
//       on another computer, an option answer, a no-design row and its removal all show on the next load, as a whole read would show them (the page
//       is compared, map by map, with a page that kept nothing); a corrupt copy, one of another revision and a browser without IndexedDB read the
//       maps whole, as before
//   node tests/cost/ru-S1-map-copy.cjs [playwright-core dir]        (B is skipped, and said so, where there is no browser)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }; const say = s => process.stdout.write(s + '\n');
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── A · the cost of the maps at the server, measured ── */
async function partA() {
  const meter = require('./meter.cjs');
  const m = meter.create(); m.install();
  const realLog = console.log, realWarn = console.warn; console.log = () => {}; console.warn = () => {};
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
  const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body || '{}'); };
  const TS = meter.Timestamp.fromMillis(1790000000000), seed = {};
  for (let i = 0; i < 3000; i++) seed['Charm_Sku_Aliases/' + (4000000000 + i)] = { listingId: String(4000000000 + i), by: 'operator', title: 'Listing title number ' + i, sku: 'SKU-' + i, v: 2, bySku: { ['V' + i]: 'SKU-' + i }, updatedAt: TS };
  for (let i = 0; i < 2000; i++) seed['Charm_Option_Map/' + (5000000000 + i)] = { listingId: String(5000000000 + i), map: { size: { small: { field: 'size', value: 'S', by: 'x', at: 1 } } }, updatedAt: TS };
  for (let i = 0; i < 300; i++) seed['Charm_Sku_NoDesign/nd' + i] = { sku: 'ND-' + i, by: 'operator', createdAt: TS };
  m.db.seed(seed);
  const rows = [['al', 'aliasGet'], ['om', 'optionMapGet'], ['nd', 'noDesignGet']];
  const cost = async (name, body) => { const a = m.snapshot(); const r = await m.op(name, () => call(body)); const d = m.since(a); return { r, reads: d.reads + d.aggs, bytes: d.bytes }; };
  let whole = 0, wholeBytes = 0, held = 0; const sig = {};
  for (const [k, op] of rows) { const x = await cost(op + ' whole', { op }); whole += x.reads; wholeBytes += x.bytes; sig[k] = { sig: x.r.sig, n: x.r.n }; ok(x.r.sig && Array.isArray(x.r.n), op + ': a whole answer carries its signature and its count'); }
  for (const [k, op] of rows) { const x = await cost(op + ' with the kept signature', { op, ifSig: sig[k].sig, since: sig[k] }); held += x.reads; ok(x.r.unchanged === true && !x.r.aliases && !x.r.maps && !x.r.list, op + ': the signature of the copy kept answers unchanged'); }
  console.log = realLog; console.warn = realWarn;
  say(`A · the three maps, a page just opened: whole read ${whole.toLocaleString()} reads, ${Math.round(wholeBytes / 1024)} KB  ->  with the copy kept on the computer ${held} reads, ~0 KB (every page load)`);
  ok(whole >= 5000 && held <= 12, `the whole read is ${whole} reads, the signature ask ${held}`);
  m.uninstall();
}

/* ── B · the real page ── */
let chromium = null;
for (const d of [process.argv[2], process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].filter(Boolean)) { try { ({ chromium } = require(path.join(d, 'playwright-core'))); break; } catch (_) { /* next */ } }
const CHROME = process.env.CHROMIUM || (() => { try { const d = '/opt/pw-browsers'; const c = fs.readdirSync(d).filter(x => /^chromium-/.test(x)).sort().pop(); return c ? path.join(d, c, 'chrome-linux/chrome') : '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; } catch (_) { return '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'; } })();
const MAP_OPS = { aliasGet: 'al', optionMapGet: 'om', noDesignGet: 'nd' };

async function partB() {
  if (!chromium || !fs.existsSync(CHROME)) { say('B · skipped: no playwright-core or chrome'); return; }
  const { start, Timestamp } = require('../charm-nest/bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const mkCtx = async noIdb => {
      const ctx = await browser.newContext({ viewport: { width: 1300, height: 900 } });
      await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' }));
      await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
      await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
      await ctx.addInitScript(noIdb => { window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} if (noIdb) Object.defineProperty(window, 'indexedDB', { value: undefined, configurable: true }); }, !!noIdb);
      const page = await ctx.newPage(), errs = [], calls = [];
      page.on('pageerror', e => errs.push(e.message));
      page.on('request', r => { if (r.method() === 'POST' && /charmNestLibrary/.test(r.url())) { try { const b = JSON.parse(r.postData() || '{}'); if (MAP_OPS[b.op]) calls.push(b); } catch (_) { /* not json */ } } });
      return { ctx, page, errs, calls };
    };
    const boot = async h => { h.calls.length = 0; await h.page.goto(sorterOrigin + '/charm-nest-1.html'); await h.page.waitForFunction(() => window.B && B.maps && B.maps.loadedAt > 0, null, { timeout: 60000 }); await sleep(400); };
    // (the maps as the page holds them, in one canonical text: object keys and the no-design lists sorted, since a delta puts a new row last where a whole read has it in document order)
    const maps = h => h.page.evaluate(() => { const canon = v => Array.isArray(v) ? v.map(canon) : v && typeof v === 'object' ? Object.fromEntries(Object.keys(v).sort().map(k => [k, canon(v[k])])) : v, nd = B.maps.noDesign || {}, ls = a => (a || []).map(canon).map(x => JSON.stringify(x)).sort();
      return JSON.stringify([canon(B.maps.optionMaps), canon(B.maps.aliases), { patterns: ls(nd.patterns), skus: ls(nd.skus), rows: ls(nd.rows) }]); });
    const whole = h => h.calls.filter(c => !c.ifSig), asked = h => h.calls.filter(c => c.ifSig);
    const api = (h, body) => h.page.evaluate(b => window.api('charmNestLibrary', b, { quiet: true }), body);
    const kept = h => h.page.evaluate(async () => { const out = {}; for (const k of ['om', 'al', 'nd']) { const r = await CharmNestMasterCache.readMap(k); out[k] = r ? { sig: r.sig, n: r.n } : null; } return out; });
    const waitKept = async (h, want) => { for (let i = 0; i < 150; i++) { const k = await kept(h); if (['om', 'al', 'nd'].every(x => k[x] && (!want || want[x] == null || k[x].sig === want[x]))) return k; await sleep(100); } throw new Error('the maps were not kept'); };
    // (how: what is done to the kept row of each map: { al: 'corrupt', om: 'badcount', nd: 'oldrev' })
    const store = (h, how) => h.page.evaluate(async how => {
      const db = await new Promise((res, rej) => { const r = indexedDB.open('cn-map-copy', 1); r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });
      return new Promise((res, rej) => { const t = db.transaction('copy', 'readwrite'), s = t.objectStore('copy'), g = s.getAll(); g.onsuccess = () => { const rows = g.result; for (const r of rows) { const key = r.k.split('|')[2], d = how[key]; if (d === 'corrupt') { r.json = r.json.slice(0, 200); s.put(r); } else if (d === 'oldrev') { s.delete(r.k); r.v = 99; r.k = r.k.replace(/^map\|\d+\|/, 'map|99|'); s.put(r); } else if (d === 'badcount') { r.n = [r.n[0] + 1]; s.put(r); } } res(rows.length); }; t.onerror = () => rej(t.error); });
    }, how);

    const h = await mkCtx(false);
    await h.page.goto(sorterOrigin + '/charm-nest-1.html'); await h.page.waitForFunction(() => window.CN && CN.S && CN.S.cloud.ok !== null && window.B && B.maps);
    // the learned maps, written through the real ops: 8 listings' aliases, 5 option answers, 3 no-design rows
    st.put('Charm_Master_Index', 'NEW-1', { sku: 'NEW-1' });
    for (let i = 0; i < 8; i++) ok((await api(h, { op: 'aliasPut', listingId: String(4100000000 + i), sku: 'NEW-1', by: 'Paul', title: 'Listing ' + i })).ok, 'alias ' + i + ' written');
    for (let i = 0; i < 5; i++) ok((await api(h, { op: 'optionMapPut', listingId: String(5100000000 + i), optionName: 'Size', optionValue: 'Big' + i, map: { field: 'size', value: 'L' }, by: 'Paul' })).ok, 'option answer ' + i + ' written');
    const nds = []; for (let i = 0; i < 3; i++) { const r = await api(h, { op: 'noDesignPut', sku: 'ND-' + i, by: 'Paul' }); ok(r.ok, 'no-design row ' + i); nds.push(r.id); }
    // (the fake sorts a document without the field as a tie: every document gets an early stamp, as a map that has been patched before has)
    for (const [k, d] of st.docs) if (/^Charm_(Sku_Aliases|Option_Map)\//.test(k)) d.updatedAt = Timestamp.fromMillis(1700000000000 + st.docs.size);
    await h.page.evaluate(async () => { for (const k of ['om', 'al', 'nd']) await CharmNestMasterCache.forgetMap(k); });

    // 1 · no copy: the three maps are read whole (as before), and kept
    await boot(h);
    ok(whole(h).length === 3 && !asked(h).length && ['aliasGet', 'optionMapGet', 'noDesignGet'].every(op => whole(h).some(c => c.op === op)), 'a page with no copy reads all three maps whole (' + h.calls.map(c => c.op + (c.ifSig ? '+ifSig' : '')).join(',') + ')');
    const M0 = await maps(h); ok(M0.includes('"NEW-1"') && M0.includes('ND-2'), 'and the maps are there');
    const k0 = await waitKept(h); ok(k0.al.n[0] === 8 && k0.om.n[0] === 5 && k0.nd.n[0] === 3, 'the answers are kept on this computer (' + JSON.stringify([k0.al.n, k0.om.n, k0.nd.n]) + ')');

    // 2 · a reload with nothing changed: the signatures only, no whole read, the same maps
    await boot(h);
    ok(!whole(h).length && asked(h).length >= 3 && Object.keys(MAP_OPS).every(op => asked(h).some(c => c.op === op && c.ifSig && c.since)), 'a reload asks for the maps with the signature (and the count) of the copy, never without it (' + h.calls.map(c => c.op + (c.ifSig ? '+ifSig' : '')).join(',') + ')');
    ok((await maps(h)) === M0, 'and the maps are the same as the ones read whole');

    // 3 · a write made elsewhere shows at the next load, as a whole read would show it (compared with a page that kept nothing)
    const ref = await mkCtx(true), refMaps = async () => { await boot(ref); return maps(ref); };   // (a page that keeps nothing: every load reads the maps whole)
    const same = async why => { const whole = await refMaps(); ok((await maps(h)) === whole, why); };
    ok((await api(h, { op: 'aliasPut', listingId: '4199999999', sku: 'NEW-1', by: 'Ann', title: 'Elsewhere' })).ok, 'an alias answer is saved (another computer)');
    await boot(h);
    ok(!whole(h).length && asked(h).some(c => c.op === 'aliasGet' && c.since && c.since.n[0] === 8), 'the next load asks for the aliases with the copy\'s signature: no whole read');
    ok((await maps(h)).includes('4199999999'), 'and the new alias shows'); await same('the aliases are the whole read\'s, listing by listing');
    await waitKept(h);
    ok((await api(h, { op: 'optionMapPut', listingId: '5199999999', optionName: 'Size', optionValue: 'Huge', map: { field: 'size', value: 'XL' }, by: 'Ann' })).ok, 'an option answer is saved');
    await boot(h); ok(!whole(h).length && (await maps(h)).includes('5199999999'), 'the option answer shows, with no whole read'); await same('the option maps are the whole read\'s');
    await waitKept(h);
    const put = await api(h, { op: 'noDesignPut', sku: 'ND-NEW', by: 'Ann' }); ok(put.ok, 'a no-design row is saved');
    await boot(h); ok((await maps(h)).includes('ND-NEW'), 'the no-design row shows'); await same('the no-design rows are the whole read\'s');
    await waitKept(h);
    // a removal is no write a since-read can see: the count says it, and the map is read whole again
    ok((await api(h, { op: 'noDesignDelete', id: put.id })).ok, 'the no-design row is removed');
    await boot(h);
    ok(!(await maps(h)).includes('ND-NEW') && asked(h).some(c => c.op === 'noDesignGet'), 'a removal shows at the next load (the copy is not believed)'); await same('the maps are the whole read\'s after a removal');
    await waitKept(h);

    // 4 · a copy that cannot be believed is ignored, map by map: cut short (aliases), a count that is not its map's (option maps), another revision (no-design rows)
    ok((await store(h, { al: 'corrupt', om: 'badcount', nd: 'oldrev' })) === 3, 'three maps are kept'); await boot(h);
    ok(whole(h).length === 3 && Object.keys(MAP_OPS).every(op => whole(h).some(c => c.op === op)), 'a corrupt copy, a copy whose count is off and a copy of another revision are not used: each map is read whole (' + h.calls.map(c => c.op + (c.ifSig ? '+ifSig' : '')).join(',') + ')');
    ok((await maps(h)) === (await refMaps()), 'and the page shows the same maps as one that kept nothing');
    await waitKept(h); await boot(h); ok(!whole(h).length, 'and the good copies that replaced them are used at the next load');
    ok(!h.errs.length, 'no page error: ' + h.errs.slice(0, 3).join(' | '));
    await h.ctx.close(); await ref.ctx.close();

    // 5 · no IndexedDB (a private window, blocked site data): every load reads the maps whole, as it always did
    const p = await mkCtx(true);
    await boot(p); const P0 = await maps(p);
    ok(whole(p).length === 3, 'no IndexedDB: the maps are read whole');
    ok(!p.errs.length, 'no page error without IndexedDB: ' + p.errs.slice(0, 3).join(' | '));
    await p.ctx.close();
  } finally { await browser.close(); try { srv.close(); } catch (_) { /* done */ } }
}

(async () => {
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok(/MC\.readMap\(key\)/.test(bridge) && /MC\.saveMap\(key, \{ sig: c\.sig, n: c\.n, r: c\.r \}\)/.test(bridge) && /!WORKSPACE_SANDBOX && MC && typeof MC\.readMap/.test(bridge), 'readMap starts from the copy kept on the computer, in production only');
  ok(/charm-nest-master-cache\.js\?v=[^"]*-mp1[-"]/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-mp1[-"]/.test(html), 'the page addresses of the two files carry the new token (later releases append their own after it)');
  const MC = require('../../charm-nest-master-cache.js');
  ok((await MC.readMap('al')) === null && (await MC.saveMap('al', { sig: 'a', n: [1], r: { aliases: { x: {} }, sig: 'a' } })) === false && (await MC.forgetMap('al')) === false, 'with no IndexedDB (Node) readMap, saveMap and forgetMap answer nothing and throw nothing');
  await partA();
  await partB();
  say(`ru-S1-map-copy OK · ${n} checks`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
