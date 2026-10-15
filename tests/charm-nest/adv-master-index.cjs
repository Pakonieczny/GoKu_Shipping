// ADV-MASTER (Paul, 15 Oct 2026: "a final test before I push everything to production ... resilient against lost internet connections, interruptions and unexpected user behaviour").
// The Master tab's indexing, attacked like a clumsy offline user would. Headless Chromium over the in-memory stand-in of the site (tests/charm-nest/bridge-server.cjs); nothing leaves this machine.
// Each scenario answers "what is lost, duplicated, stuck or wrong?":
//   1 · two master files dropped TOGETHER that share a SKU: the second finds it held (left alone, said), nothing is blocked as a clash with a file that was only a second ago being indexed;
//   2 · a charm with two SKU lines, one held by another master: rebuilt whole, the other master superseded, nothing blocked, that master's other SKUs untouched;
//   3 · the internet drops in the middle of the design-file uploads: the run says failed, NOTHING is in the index (no SKU without its file), no page error, the same file dropped again builds all of it once;
//   4 · the page is closed / reloaded while the index write is on its way (the server stored it, the answer never arrived): the SKUs are in, the upload-history record is not; dropping the file again writes the record and builds nothing twice;
//   5 · the charm library cannot be read (Master tab): "Could not load the charm library" with Try again, never "No charms indexed yet".
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/adv-master-index.cjs
'use strict';
const assert = require('assert'), fs = require('fs'), os = require('os'), path = require('path');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const A = '0.2 0.35 0.85', B = '0.85 0.2 0.2', C = '0.2 0.6 0.3';
const libSkus = st => st.list('Charm_Master_Index').map(e => e._id).sort();
const sleep = ms => new Promise(r => setTimeout(r, ms));
const body = route => { try { return JSON.parse(route.request().postData() || '{}'); } catch (_) { return {}; } };
// twelve charms in a grid, one label under each
const names12 = Array.from({ length: 12 }, (_, i) => 'GRID_' + String(i + 1).padStart(2, '0'));
const grid12 = () => scene({ page: [420, 300], bodies: names12.map((_, i) => ({ shape: ['house', 'notch', 'blob'][i % 3], x: 30 + (i % 4) * 95, y: 200 - Math.floor(i / 4) * 80, w: 40, h: 40, ink: [A, B, C][i % 3] })), labels: names12.map((nm, i) => ({ text: nm, cx: 50 + (i % 4) * 95, top: 199 - Math.floor(i / 4) * 80 })) });

(async () => {
  const br = await R.browser(), st = br.st, page = br.page, ctx = page.context();
  const job = (k = -1) => page.evaluate(i => { const js = [...B.master.jobs.values()], j = js.at(i); return { state: j.state, error: j.error || null, written: j.written, held: j.held, name: j.name, supersede: j.supersede || [], heldList: (j.heldList || []).map(x => [x.sku, x.from]), log: j.log }; }, k);
  const wipe = async () => { for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => { B.master.jobs.clear(); return Master.load(true); }); };
  const ready = async () => {
    await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(({ station }) => { const s = CN.S.settings; s.dsOrigin = station; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; }, { station: br.srv.stationOrigin });
    await page.evaluate(() => CN.setMode('master')); await page.waitForSelector('#mFile', { state: 'attached' });
  };
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'adv-master-'));
  const put = (bytes, name) => { const f = path.join(tmp, name); fs.writeFileSync(f, bytes); return f; };
  const settled = (count) => page.waitForFunction(c => { const js = [...B.master.jobs.values()]; return js.length >= c && js.every(x => ['done', 'error'].includes(x.state)); }, count, { timeout: 150000 });
  try {
    // 1 · two files dropped together, one SKU in both
    const fA = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'blob', x: 200, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'SHARED_1', cx: 82.5, top: 149 }, { text: 'A_ONLY_1', cx: 222.5, top: 149 }] });
    const fB = await scene({ page: [420, 300], bodies: [{ shape: 'notch', x: 60, y: 150, w: 55, h: 50, ink: B }, { shape: 'house', x: 200, y: 150, w: 45, h: 45, ink: B }], labels: [{ text: 'SHARED_1', cx: 87.5, top: 149 }, { text: 'B_ONLY_1', cx: 222.5, top: 149 }] });
    await page.evaluate(() => B.master.jobs.clear());
    await page.evaluate(async fs => {   // (the picker takes one file; several arrive only by dropping them on the tab)
      const dt = new DataTransfer();
      for (const [name, b64] of fs) { const bin = atob(b64), u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i); dt.items.add(new File([u], name, { type: 'application/pdf' })); }
      document.getElementById('masterView').dispatchEvent(new DragEvent('drop', { dataTransfer: dt, bubbles: true, cancelable: true }));
    }, [['A.ai', fA.toString('base64')], ['B.ai', fB.toString('base64')]]);
    await settled(2);
    const shared = st.doc('Charm_Master_Index', 'SHARED_1'), jb = await job(-1);
    ok(libSkus(st).join() === 'A_ONLY_1,B_ONLY_1,SHARED_1', 'both files are indexed: ' + libSkus(st));
    ok(shared && !shared.blocked && !shared.conflict && shared.masterName === 'A.ai', 'the SKU both files carry is the first file\'s and is NOT blocked as a clash: ' + JSON.stringify(shared && { m: shared.masterName, b: shared.blocked, c: shared.conflict }));
    ok(jb.name === 'B.ai' && jb.held === 1 && jb.written === 1 && jb.heldList.join() === 'SHARED_1,A.ai', 'the second file found it held, says whose it is, and wrote only its own: ' + JSON.stringify(jb));
    ok(st.list('Charm_Master_Index').every(e => !e.blocked), 'nothing in the library is blocked');

    // 2 · a charm with two SKU lines, one of them held by another master
    const fG = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ text: 'SHARED_1', cx: 82.5, top: 149 }, { text: 'NEW_2', cx: 82.5, top: 141 }] });
    await br.upload(fG, 'G.ai'); const jg = await job();
    const s1 = st.doc('Charm_Master_Index', 'SHARED_1'), s2 = st.doc('Charm_Master_Index', 'NEW_2'), a1 = st.doc('Charm_Master_Index', 'A_ONLY_1');
    ok(jg.state === 'done' && jg.written === 2 && jg.supersede.length === 1, 'a charm with one held SKU and one new one is rebuilt whole and supersedes the other master: ' + JSON.stringify({ w: jg.written, s: jg.supersede }));
    ok(s1.masterName === 'G.ai' && s2.masterName === 'G.ai' && s1.aiPath === s2.aiPath && !s1.blocked && !s2.blocked, 'both lines are on one design file of the new master and neither is blocked');
    ok(a1.masterName === 'A.ai' && !a1.blocked && st.blobs.has(a1.aiPath), 'the other master\'s own SKU is untouched');

    // 3 · the internet drops in the middle of the design-file uploads
    await wipe();
    const g12 = await grid12(); let signs = 0;
    const cut = async route => { if (body(route).op === 'sign' && ++signs === 6) { await ctx.setOffline(true); return route.abort('internetdisconnected'); } return route.fallback(); };
    await page.route(/charmNestOutput/, cut);
    await br.upload(g12, 'GRID.ai', { timeout: 150000 }); let j = await job();
    ok(j.state === 'error' && /./.test(j.error || ''), 'the run says failed and why: ' + j.error);
    ok(await page.evaluate(() => /failed/.test((document.querySelector('#mJobs') || {}).innerText || '')), 'and the card says failed');
    await ctx.setOffline(false); await page.unroute(/charmNestOutput/, cut); await sleep(1500);   // (the workers that were still going have given up or finished)
    ok(libSkus(st).length === 0 && st.list('Charm_Master_Files').length === 0, 'NOTHING is in the index (no SKU without its design file), no upload-history record');
    await br.upload(g12, 'GRID.ai'); j = await job();
    const aiFiles = [...st.blobs.keys()].filter(k => /^charmnest\/master\/GRID_\d+\.ai$/.test(k));
    ok(j.state === 'done' && j.written === 12 && libSkus(st).join() === names12.join() && aiFiles.length === 12 && st.list('Charm_Master_Files').length === 1, 'dropping the file again builds all 12 once, one design file each, one upload-history record: ' + JSON.stringify({ w: j.written, files: aiFiles.length }));
    const bad3 = br.errors.filter(e => !/status of 500|ERR_INTERNET_DISCONNECTED|Failed to load resource|net::ERR/.test(e));
    ok(bad3.length === 0, 'no page error from the dropped connection: ' + bad3.join(' | '));

    // 4 · the page is reloaded while the index write is on its way: the server stored it, the answer never arrived
    await wipe();
    let lost = null; const lose = async route => { if (body(route).op === 'masterPutIndex' && !lost) { await route.fetch(); lost = true; return new Promise(() => {}); } return route.fallback(); };   // (route.fetch(): the stand-in site did the whole write; the page never hears)
    await page.route(/charmNestLibrary/, lose);
    await page.setInputFiles('#mFile', put(g12, 'GRID.ai'));
    for (let i = 0; i < 600 && !lost; i++) await sleep(250);
    ok(lost === true, 'the index write reached the site');
    await page.unroute(/charmNestLibrary/, lose); await page.reload(); await ready();
    ok(libSkus(st).join() === names12.join() && st.list('Charm_Master_Files').length === 0, 'after the reload the 12 SKUs are in the library and the upload-history record is not');
    const putsBefore = st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length, blobsBefore = st.blobs.size;
    await br.upload(g12, 'GRID.ai'); j = await job();
    ok(j.state === 'done' && j.written === 0 && j.held === 12 && j.log.some(l => /record was missing/.test(l)) && st.list('Charm_Master_Files').length === 1 && st.list('Charm_Master_Files')[0].skus.length === 12, 'dropping the file again writes the missing record and builds nothing: ' + JSON.stringify({ w: j.written, h: j.held, rec: st.list('Charm_Master_Files').length }));
    ok(st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length === putsBefore && st.blobs.size === blobsBefore, 'no design file was uploaded and no index write was made a second time');

    // 5 · the library cannot be read: the tab says so, with a way to try again
    await br.upload(g12, 'GRID.ai');   // (a held file: the library is whole)
    await page.evaluate(() => { B.master.entries = new Map(); B.master.index = null; B.master.files = []; B.master.loadedAt = 0; });
    const down = async route => { if (/^master(List|ListFiles)$/.test(body(route).op || '')) return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'firestore unavailable' }) }); return route.fallback(); };
    await page.route(/charmNestLibrary/, down);
    await page.evaluate(() => Master.load(true).catch(() => {})); await page.evaluate(() => Master.render());
    let txt = await page.evaluate(() => document.querySelector('#mGrid').innerText);
    ok(/Could not load the charm library/.test(txt) && /firestore unavailable/.test(txt) && /Try again/.test(txt) && !/No charms indexed yet/.test(txt), 'an unreadable library is "Could not load the charm library", never an empty one: ' + txt.replace(/\s+/g, ' ').slice(0, 120));
    await page.unroute(/charmNestLibrary/, down); await page.click('#mRetry');
    await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length === 12, null, { timeout: 30000 });
    ok(true, 'Try again reads the library: 12 tiles');
    const bad = br.errors.filter(e => !/status of 500|ERR_INTERNET_DISCONNECTED|Failed to load resource|net::ERR/.test(e));
    ok(bad.length === 0, 'no page error in the whole run: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`adv-master-index: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
