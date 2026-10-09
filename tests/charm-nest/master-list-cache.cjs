// The charm library kept on this computer (Paul, 9 Oct 2026: "on every refresh, I have to go through the downloading of all the
// thumbnails ... especially if you have weak Internet"). Master.load (charm-nest-bridge.js) read the WHOLE index again at every
// refresh (about 6,960 Firestore documents in 3 parts); charm-nest-master-cache.js keeps the last whole answer in IndexedDB and the
// page asks for the signatures only (masterListFiles + ifFilesSig).
//   A · files: built into the public site, loaded before the bridge, the bridge uses it, the key names the code, the signature
//       carries the shape of an entry
//   B · the real Sorter page over the real functions on the in-memory Firestore: a refresh with the signatures unchanged makes NO
//       masterList call and shows the same library; every writer of the index (masterPatch facing, masterPutIndex, masterRemoveSku)
//       changes the signature so the copy is dropped and the new value shows; a new master file record alone is picked up without
//       reading the entries; a corrupt copy and one of another code revision are ignored; no IndexedDB still works
//   node tests/charm-nest/master-list-cache.cjs      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome> for B; skipped without)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ── A · files ── */
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
ok(/"charm-nest-master-cache\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'the module is in the public build list');
const at = s => html.indexOf(s);
ok(at('<script src="charm-nest-master-cache.js?v=') > at('<script src="charm-nest-thumbs.js') && at('<script src="charm-nest-master-cache.js?v=') < at('<script src="charm-nest-bridge.js'), 'the page loads the module after the thumbnail module and before the bridge');
ok(/CharmNestMasterCache/.test(bridge) && /MC\.read\(\)/.test(bridge) && /MC\.save\(\{ index: ix\.index/.test(bridge) && /MC\.saveFiles\(/.test(bridge), 'Master.load reads, saves and updates the kept copy');
ok(/!B\.master\.index && !o\.force/.test(bridge), 'only a page that holds nothing yet, and not load(true), starts from the copy');
ok(/shape: Master\.SHAPE/.test(fs.readFileSync(path.join(root, 'netlify/functions/charmNestLibrary.js'), 'utf8')), 'the index signature carries the shape of an entry');
const MC = require('../../charm-nest-master-cache.js');
const pageWith = srcs => ({ document: { querySelectorAll: () => srcs.map(s => ({ getAttribute: () => s })) } });
const code = fs.readFileSync(path.join(root, 'charm-nest-master-cache.js'), 'utf8'), revOf = srcs => { const mod = { exports: {} }; new Function('module', 'self', code)(mod, pageWith(srcs)); return mod.exports.revision(); };
ok(revOf(['charm-nest-bridge.js?v=1']) === revOf(['charm-nest-bridge.js?v=1']) && revOf(['charm-nest-bridge.js?v=1']) !== revOf(['charm-nest-bridge.js?v=2']) && revOf(['charm-nest-bridge.js?v=1', 'charm-nest-master-cache.js?v=a']) !== revOf(['charm-nest-bridge.js?v=1', 'charm-nest-master-cache.js?v=b']), 'the key follows the addresses of the bridge and of this module');

let chromium = null;
for (const d of [process.env.PW_DIR, path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].filter(Boolean)) { try { ({ chromium } = require(path.join(d, 'playwright-core'))); break; } catch (_) { /* next */ } }
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!chromium || !fs.existsSync(CHROME)) { console.log(`master-list-cache: ${n} file checks OK · no playwright-core or chrome: part B was not run`); process.exit(0); }

/* ── B · the real page ── */
const COUNT = 60;
async function partB() {
  const { start, Timestamp } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const mkCtx = async (noIdb) => {
      const ctx = await browser.newContext({ viewport: { width: 1300, height: 900 } });
      await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' }));
      await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
      await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
      await ctx.addInitScript(noIdb => { window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} if (noIdb) Object.defineProperty(window, 'indexedDB', { value: undefined, configurable: true }); }, !!noIdb);
      const page = await ctx.newPage(), errs = [], calls = [];
      page.on('pageerror', e => errs.push(e.message));
      page.on('request', r => { if (r.method() === 'POST' && /charmNestLibrary/.test(r.url())) { try { const b = JSON.parse(r.postData() || '{}'); if (/^master/.test(b.op || '')) calls.push(b); } catch (_) { /* not json */ } } });
      return { ctx, page, errs, calls };
    };
    const boot = async (h) => { h.calls.length = 0; await h.page.goto(sorterOrigin + '/charm-nest-1.html'); await h.page.waitForFunction(() => window.B && B.master && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 }); await sleep(400); };
    const ops = (h, op) => h.calls.filter(c => c.op === op);
    const lib = h => h.page.evaluate(() => ({ n: B.master.entries.size, files: B.master.files.length, e: Object.fromEntries([...B.master.entries].filter(([k]) => /^MC00[1-5]$/.test(k)).map(([k, v]) => [k, { w: v.widthPt, f: v.facing || null, name: v.masterName }])) }));
    const api = (h, body) => h.page.evaluate(b => window.api('charmNestLibrary', b, { quiet: true }), body);
    const idb = (h, how) => h.page.evaluate(async how => {
      const db = await new Promise((res, rej) => { const r = indexedDB.open('cn-master-list', 1); r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });
      return new Promise((res, rej) => { const t = db.transaction('copy', 'readwrite'), s = t.objectStore('copy'), g = s.getAll(); g.onsuccess = () => { const rows = g.result; for (const r of rows) { if (how === 'corrupt') { r.entriesJson = r.entriesJson.slice(0, 400); s.put(r); } else if (how === 'oldrev') { s.delete(r.k); r.k = 'copy|zzzzzz'; r.rev = 'zzzzzz'; s.put(r); } } res(rows.length); }; t.onerror = () => rej(t.error); });
    }, how);
    const kept = h => h.page.evaluate(async () => { const s = await CharmNestMasterCache.stats(), r = await CharmNestMasterCache.read(); return { records: s.records || 0, n: r ? r.entries.length : -1, files: r ? r.files.length : -1 }; });
    const waitKept = async h => { for (let i = 0; i < 100; i++) { const k = await kept(h); if (k.n >= 0) return k; await sleep(100); } throw new Error('the copy was not kept'); };

    const h = await mkCtx(false);
    // the index and one master file record, written through the real ops (the writers whose stamps the signature rests on)
    await h.page.goto(sorterOrigin + '/charm-nest-1.html'); await h.page.waitForFunction(() => window.CN && CN.S && CN.S.cloud.ok !== null && window.B);
    const entries = Array.from({ length: COUNT }, (_, i) => ({ sku: 'MC' + String(i + 1).padStart(3, '0'), masterHash: 'aa11bb22', widthPt: 40 + i, heightPt: 30, areaPt2: 900, members: 1, holes: 1, aiPath: `charmnest/master/MC${i + 1}.ai`, thumbPath: `charmnest/master/MC${i + 1}.png`, charmHash: 'h' + i }));
    ok((await api(h, { op: 'masterPutIndex', entries, masterHash: 'aa11bb22', masterPath: 'charmnest/master/m.ai', masterName: 'MASTER A', hashSource: 'browser' })).ok, 'the fake index is written by masterPutIndex');
    ok((await api(h, { op: 'masterPutFile', file: { masterHash: 'aa11bb22', name: 'MASTER A', charms: COUNT, skus: entries.map(e => e.sku) } })).ok, 'and its file record by masterPutFile');
    // (the fake sorts a document that lacks the field as a tie, where Firestore leaves it out of an orderBy: every document gets an early edit stamp, as an index that has been patched before has)
    for (const [k, d] of st.docs) if (k.startsWith('Charm_Master_Index/')) d.updatedAt = Timestamp.fromMillis(1700000000000);
    await h.page.evaluate(() => CharmNestMasterCache.forget());

    // 1 · the first load with no copy behaves as before: the whole index (masterList) and the files (no signature sent), then keeps it
    await boot(h);
    ok(ops(h, 'masterList').length >= 1 && ops(h, 'masterListFiles').length === 1 && !ops(h, 'masterListFiles')[0].ifFilesSig, 'first load, no copy: the whole index is read and the files list asked for with no signature');
    const L0 = await lib(h); ok(L0.n === COUNT && L0.files === 1, 'and the library is there (' + L0.n + ' charms, ' + L0.files + ' file)');
    const k0 = await waitKept(h); ok(k0.n === COUNT && k0.files === 1 && k0.records === 1, 'the whole answer is kept on this computer (' + k0.n + ' charms, one record)');

    // 2 · a refresh with nothing changed: the signatures only, no list call, the same library
    await boot(h);
    ok(ops(h, 'masterList').length === 0, 'refresh, nothing changed: no masterList call (' + h.calls.map(c => c.op).join(',') + ')');
    ok(ops(h, 'masterListFiles').length === 1 && !!ops(h, 'masterListFiles')[0].ifFilesSig, 'only the signature check goes out (masterListFiles with ifFilesSig)');
    const L1 = await lib(h); ok(JSON.stringify(L1) === JSON.stringify(L0), 'and the library is the same as the one read from the cloud');
    await boot(h); ok(ops(h, 'masterList').length === 0, 'and again');

    // 3 · every writer of the index changes the signature: the copy is dropped and the new value shows (never a stale charm)
    ok((await api(h, { op: 'masterPatch', sku: 'MC003', patch: { facing: 'L' } })).ok, 'a person sets a facing (masterPatch)');
    await boot(h);
    ok(ops(h, 'masterList').length >= 1 && (await lib(h)).e.MC003.f === 'L', 'masterPatch: the next load reads the index again and shows the facing');
    await waitKept(h); await boot(h); ok(ops(h, 'masterList').length === 0, 'and the new copy is used after that');
    entries[1].widthPt = 99; entries[1].charmHash = 'hNEW';
    await api(h, { op: 'masterPutIndex', entries: [entries[1]], masterHash: 'aa11bb22', masterPath: 'charmnest/master/m.ai', masterName: 'MASTER A', hashSource: 'browser' });
    await boot(h);
    ok(ops(h, 'masterList').length >= 1 && (await lib(h)).e.MC002.w === 99, 'masterPutIndex (a re-index): the next load shows the new drawing size');
    await waitKept(h);
    ok((await api(h, { op: 'masterRemoveSku', sku: 'MC004' })).ok, 'a prune (masterRemoveSku)');
    await boot(h);
    ok(ops(h, 'masterList').length >= 1 && (await lib(h)).n === COUNT - 1, 'masterRemoveSku: the next load has one charm fewer');
    await waitKept(h);

    // 4 · a new master file record alone: the entries are not read again, the files are, and the copy follows
    await api(h, { op: 'masterPutFile', file: { masterHash: 'cc33dd44', name: 'MASTER B', charms: 1, skus: ['MC001'] } });
    await boot(h);
    ok(ops(h, 'masterList').length === 0 && (await lib(h)).files === 2, 'a new file record only: no masterList call, the files list is read again (2 files)');
    for (let i = 0; i < 50; i++) { if ((await kept(h)).files === 2) break; await sleep(100); }
    await boot(h); ok(ops(h, 'masterList').length === 0 && (await lib(h)).files === 2 && (await kept(h)).files === 2, 'and the kept copy has the new files (the next load asks for nothing more)');

    // 5 · a copy that cannot be believed is ignored: corrupt, or made by another code revision
    await idb(h, 'corrupt'); await boot(h);
    ok(ops(h, 'masterList').length >= 1 && (await lib(h)).n === COUNT - 1, 'a corrupt copy is ignored: the index is read again and the library is whole');
    const k1 = await waitKept(h); ok(k1.n === COUNT - 1 && k1.records === 1, 'and a good copy replaces it');
    await idb(h, 'oldrev'); await boot(h);
    ok(ops(h, 'masterList').length >= 1 && (await lib(h)).n === COUNT - 1, 'a copy kept under another code revision is not used');
    const k2 = await waitKept(h); ok(k2.records === 1, 'and the new copy replaces the old record (one record kept)');
    ok(!h.errs.length, 'no page error: ' + h.errs.slice(0, 3).join(' | '));
    await h.ctx.close();

    // 6 · no IndexedDB (a private window, blocked site data): it works as before, every load reads the index
    const p = await mkCtx(true);
    await boot(p); const P0 = await lib(p);
    ok(ops(p, 'masterList').length >= 1 && P0.n === COUNT - 1, 'no IndexedDB: the library loads (' + P0.n + ' charms)');
    await boot(p); ok(ops(p, 'masterList').length >= 1 && (await lib(p)).n === COUNT - 1, 'and a refresh reads it again, with no error');
    ok(!p.errs.length, 'no page error without IndexedDB: ' + p.errs.slice(0, 3).join(' | '));
    await p.ctx.close();
  } finally { await browser.close(); try { srv.close(); } catch (_) { /* done */ } }
}

(async () => {
  ok((await MC.read()) === null && (await MC.save({ index: {}, entries: [{ sku: 'A' }] })) === false && (await MC.saveFiles([], null)) === false, 'with no IndexedDB (Node) read, save and saveFiles answer nothing and throw nothing');
  await partB();
  console.log(`master-list-cache OK · ${n} checks`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
