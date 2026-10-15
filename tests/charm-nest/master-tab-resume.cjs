// What a person sees when a master file is dropped in the Master tab and something goes wrong, and what the next drop of the same file does (MASTER2, Paul 14 Oct 2026).
// Headless Chromium over the in-memory stand-in of the site (tests/charm-nest/bridge-server.cjs); nothing leaves this machine.
//   · a library that holds a pair design as ONE body is said, and "re-index SKUs already held" builds it from both bodies;
//   · a drop where everything is held keeps the leftovers on the card and writes back an upload-history record the library lost;
//   · the first write fails: the job says failed, nothing is half-built, and dropping the file again builds all of it;
//   · the library answers a write without having stored one SKU: the index is read back and that SKU is written again;
//   · a SKU the library refuses (a name it cannot hold) is named, never dropped without a word;
//   · a sheet with no readable label does not claim that all its charms are already held.
//   PW_DIR=/opt/node-tools/node_modules node tests/charm-nest/master-tab-resume.cjs
'use strict';
const assert = require('assert');
const { scene } = require('./_masterScene.cjs'), R = require('./_masterRoutes.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const A = '0.2 0.35 0.85', B = '0.85 0.2 0.2', C = '0.2 0.6 0.3';
const pairAndLone = () => scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 107, y: 150, w: 45, h: 45, ink: B }, { shape: 'house', x: 300, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'Spider / Web_1', cx: 106, top: 149 }, { text: 'LONE_9002', cx: 322.5, top: 146 }] });
const libSkus = st => st.list('Charm_Master_Index').map(e => e._id).sort();

(async () => {
  const br = await R.browser(), st = br.st, page = br.page;
  const job = () => page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return { state: j.state, error: j.error || null, written: j.written, held: j.held, stale: (j.stale || []).map(x => x.sku), rejected: (j.rejected || []).map(x => x.sku), missing: j.missing || [], unlabelled: (j.lab && j.lab.unlabelled) || [], log: j.log }; });
  const sheet = await pairAndLone();
  try {
    // 1 · a library that holds the pair as ONE body
    let r = await br.upload(sheet, 'PAIR.ai'); ok(r.state === 'done' && libSkus(st).join() === 'LONE_9002,SPIDER - WEB_1' && st.doc('Charm_Master_Index', 'SPIDER - WEB_1').pair.bodies === 2, 'the first drop builds the pair and the lone charm');
    delete st.doc('Charm_Master_Index', 'SPIDER - WEB_1').pair; await page.evaluate(() => Master.load(true));
    r = await br.upload(sheet, 'PAIR.ai'); let j = await job();
    ok(j.state === 'done' && j.written === 0 && j.stale.join() === 'SPIDER - WEB_1', 'a design held as one body of a pair is said: ' + JSON.stringify(j.stale));
    ok(r.unlabelled.length === 1, 'and its other body stays listed, it is not claimed by a design that does not hold it');
    ok(await page.evaluate(() => { const d = document.querySelector('#mJobs details'); return /to look at/.test(d.querySelector('summary').innerText); }), 'the line of the finished run says that something is left to look at');
    r = await br.upload(sheet, 'PAIR.ai', { reindexAll: true }); j = await job();
    ok(j.state === 'done' && st.doc('Charm_Master_Index', 'SPIDER - WEB_1').pair.bodies === 2 && j.stale.length === 0 && j.unlabelled.length === 0, '"re-index SKUs already held" builds it from both bodies');
    // 2 · everything held, the upload-history record lost
    st.docs.delete('Charm_Master_Files/' + [...st.docs.keys()].find(k => k.startsWith('Charm_Master_Files/')).split('/')[1]); await page.evaluate(() => Master.load(true));
    ok(st.list('Charm_Master_Files').length === 0, 'the record is gone');
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(j.state === 'done' && j.written === 0 && st.list('Charm_Master_Files').length === 1 && j.log.some(l => /record was missing/.test(l)), 'the next drop writes the missing upload-history record back and says so ' + JSON.stringify({ s: j.state, w: j.written, n: st.list('Charm_Master_Files').length, e: j.error, log: j.log }));
    ok(st.list('Charm_Master_Files')[0].skus.length === 2, 'with the SKUs of the file');
    r = await br.upload(sheet, 'PAIR.ai'); j = await job(); ok(!j.log.some(l => /record was missing/.test(l)) && st.list('Charm_Master_Files').length === 1, 'a record that exists is left alone');

    // 3 · the first write fails
    for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => Master.load(true));
    let failOnce = true; const handler = async route => { const req = route.request(); let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {} if (b.op === 'masterPutIndex' && failOnce) { failOnce = false; return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'forced failure' }) }); } return route.fallback(); };
    await page.route(/charmNestLibrary/, handler);
    let failed = null; try { await br.upload(sheet, 'PAIR.ai', { timeout: 60000 }); } catch (e) { failed = e; }
    j = await job(); ok(j.state === 'error' && /forced failure|500|failed/i.test(j.error || ''), 'the failed write says failed: ' + j.error);
    ok(libSkus(st).length === 0, 'nothing is half-built in the library');
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(j.state === 'done' && libSkus(st).join() === 'LONE_9002,SPIDER - WEB_1' && st.doc('Charm_Master_Index', 'SPIDER - WEB_1').pair.bodies === 2, 'dropping the file again builds all of it, the pair included');
    await page.unroute(/charmNestLibrary/, handler);

    // 4 · the library answers a write without storing one SKU
    for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => Master.load(true));
    let dropped = 0; const lossy = async route => { const req = route.request(); let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (_) {} if (b.op === 'masterPutIndex' && !dropped && (b.entries || []).length > 1) { dropped = 1; b.entries = b.entries.filter(e => e.sku !== 'LONE_9002'); return route.fallback({ postData: JSON.stringify(b) }); } return route.fallback(); };
    await page.route(/charmNestLibrary/, lossy);
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(dropped === 1 && j.state === 'done' && libSkus(st).join() === 'LONE_9002,SPIDER - WEB_1' && j.missing.length === 0, 'a SKU the library did not store is read back as missing and written again: ' + libSkus(st).join());
    ok(st.calls.filter(c => c.body && c.body.op === 'masterGetMany').length >= 1, 'by reading the index back');
    await page.unroute(/charmNestLibrary/, lossy);

    // 5 · a SKU the library refuses
    for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => { Master.load(true); CN.S.settings.skuPattern = "^[A-Z0-9#][A-Z0-9 #_.,'&()+\\-]{1,60}$"; });
    const hash = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'house', x: 200, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'HASH#1_X', cx: 82.5, top: 149 }, { text: 'FINE_ONE_1', cx: 222.5, top: 149 }] });
    r = await br.upload(hash, 'HASH.ai'); j = await job();
    ok(j.state === 'done' && j.rejected.join() === 'HASH#1_X' && libSkus(st).join() === 'FINE_ONE_1', 'a name the library refuses is named and is not in it: ' + JSON.stringify({ rej: j.rejected, lib: libSkus(st) }));
    ok(j.missing.length === 0, 'and it is not chased as "missing" three times');
    ok(/refused by the library/.test(r.card + (await page.evaluate(() => (document.querySelector('#mJobs') || {}).innerHTML || ''))), 'the card says so');
    await page.evaluate(() => { CN.S.settings.skuPattern = ''; });

    // 6 · no readable label
    for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => Master.load(true));
    const bare = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 200, y: 150, w: 45, h: 45, ink: B }] });
    r = await br.upload(bare, 'BARE.ai'); j = await job();
    ok(j.state === 'done' && j.written === 0 && !j.log.some(l => /all 0 charm/.test(l)) && j.log.some(l => /no SKU label was read/.test(l)), 'a sheet with no label does not claim that all its charms are held: ' + j.log.join(' | '));
    ok(j.unlabelled.length === 2, 'both charms are listed without a SKU');

    const wipe = async () => { for (const k of [...st.docs.keys()]) if (/^Charm_Master_(Index|Files)\//.test(k)) st.docs.delete(k); await page.evaluate(() => Master.load(true)); };
    // 7 · the file's record keeps the whole file when a repair drop rebuilds one SKU
    await wipe(); await br.upload(sheet, 'PAIR.ai');
    const rec0 = st.list('Charm_Master_Files')[0]; ok(rec0.skus.length === 2, 'the first drop records both SKUs: ' + rec0.skus);
    st.docs.delete('Charm_Master_Index/LONE_9002'); await page.evaluate(() => Master.load(true));
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(j.written === 1 && st.list('Charm_Master_Files')[0].skus.slice().sort().join() === 'LONE_9002,SPIDER - WEB_1', 'a repair drop that builds one SKU still records the whole file: ' + st.list('Charm_Master_Files')[0].skus);

    // 8 · a library that cannot be read stops the run before anything is written
    await page.route(/charmNestLibrary/, async route => { let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {} if (/^master(List|ListFiles)$/.test(b.op || '')) return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'firestore unavailable' }) }); return route.fallback(); }, { times: 40 });
    const callsBefore = st.calls.length; await wipe().catch(() => {});
    await page.unroute(/charmNestLibrary/); await page.route(/charmNestLibrary/, async route => { let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {} if (/^master(List|ListFiles)$/.test(b.op || '')) return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'firestore unavailable' }) }); return route.fallback(); });
    const before = JSON.stringify(libSkus(st)), putsBefore = st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length;
    r = await br.upload(sheet, 'PAIR2.ai'); j = await job();
    ok(j.state === 'error' && /firestore unavailable|500|failed/i.test(j.error || ''), 'a library that cannot be read ends the run: ' + j.error);
    ok(st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length === putsBefore && JSON.stringify(libSkus(st)) === before, 'and nothing was written');
    await page.unroute(/charmNestLibrary/);

    // 9 · "re-index held" over a corrected copy under a new name: the old file is superseded, nothing is blocked as a clash
    await wipe(); await br.upload(sheet, 'A.ai');
    const corrected = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'notch', x: 107, y: 150, w: 45, h: 45, ink: B }, { shape: 'house', x: 300, y: 150, w: 45, h: 45, ink: C }, { shape: 'house', x: 300, y: 60, w: 40, h: 40, ink: A }], labels: [{ text: 'Spider / Web_1', cx: 106, top: 149 }, { text: 'LONE_9002', cx: 322.5, top: 146 }, { text: 'THIRD_9003', cx: 320, top: 59 }] });
    r = await br.upload(corrected, 'A (corrected).ai', { reindexAll: true }); j = await job();
    ok(j.state === 'done' && libSkus(st).length === 3 && st.list('Charm_Master_Index').every(e => !e.blocked && !e.conflict), 'a corrected copy under a new name with "re-index held" blocks nothing: ' + JSON.stringify(st.list('Charm_Master_Index').map(e => [e._id, e.blocked])));
    await page.evaluate(() => { const cb = document.querySelector('#mAllSkus'); cb.checked = false; cb.onchange && cb.onchange(); });

    // 10 · a refused name does not replace the file of the SKU its name is cleaned to
    await wipe(); await page.evaluate(() => { CN.S.settings.skuPattern = "^[A-Z0-9#][A-Z0-9 #_.,'&()+\\-]{1,60}$"; });
    const good = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ text: 'HASH_1', cx: 82.5, top: 149 }] });
    await br.upload(good, 'GOOD.ai'); const goodAi = st.doc('Charm_Master_Index', 'HASH_1').aiPath, goodBytes = Buffer.from(st.blobs.get(goodAi).buf);
    const clash = await scene({ page: [420, 300], bodies: [{ shape: 'notch', x: 60, y: 150, w: 60, h: 45, ink: B }], labels: [{ text: 'HASH#1', cx: 90, top: 149 }] });
    r = await br.upload(clash, 'CLASH.ai'); j = await job();
    ok(j.rejected.join() === 'HASH#1' && Buffer.compare(Buffer.from(st.blobs.get(goodAi).buf), goodBytes) === 0, 'the refused name uploaded nothing: the file of HASH_1 is what it was');
    ok(j.written === 0, 'and the run does not count it as written: ' + j.written);
    await page.evaluate(() => { CN.S.settings.skuPattern = ''; });

    // 11 · held is judged per size
    await wipe();
    const sizeS = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ text: 'ALPHA_1 · S', cx: 82.5, top: 149 }] });
    await br.upload(sizeS, 'SIZES.ai');
    const sizeSM = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'house', x: 200, y: 150, w: 60, h: 60, ink: A }], labels: [{ text: 'ALPHA_1 · S', cx: 82.5, top: 149 }, { text: 'ALPHA_1 · M', cx: 230, top: 149 }] });
    r = await br.upload(sizeSM, 'SIZES.ai'); j = await job();
    ok(j.state === 'done' && j.written === 1 && j.held === 1 && Object.keys(st.doc('Charm_Master_Index', 'ALPHA_1').sizes || {}).includes('M'), 'a new size of a held SKU is built, not skipped: ' + JSON.stringify({ w: j.written, h: j.held, sizes: Object.keys(st.doc('Charm_Master_Index', 'ALPHA_1').sizes || {}) }));   // (the stand-in merges a document one level deep: the real library keeps size S beside M)

    // 12 · the same file dropped twice at once starts one run
    await wipe(); { const file = require('path').join(require('os').tmpdir(), 'twice-' + Date.now() + '.ai'); require('fs').writeFileSync(file, sheet);
      await page.evaluate(() => B.master.jobs.clear());
      await Promise.all([page.setInputFiles('#mFile', file), page.waitForTimeout(50).then(() => page.setInputFiles('#mFile', file))]);
      await page.waitForFunction(() => { const js = [...B.master.jobs.values()]; return js.length && js.every(x => ['done', 'error'].includes(x.state)); }, null, { timeout: 120000 });
      ok((await page.evaluate(() => B.master.jobs.size)) === 1 && libSkus(st).length === 2 && st.calls.filter(c => c.body && c.body.op === 'masterPutIndex').length >= 1, 'two drops of one file are one run'); }

    // 13 · a label read by Claude from outlined text: confirming it writes it (the charm was never traced before)
    await wipe();
    const outlined = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }, { shape: 'house', x: 200, y: 150, w: 45, h: 45, ink: C }], labels: [{ text: 'LABELLED_1', cx: 82.5, top: 149 }],
      extraOps: Array.from({ length: 9 }, (_, i) => `q 0 0 0 RG 0.4 w ${205 + i * 4.2} 119 m ${207 + i * 4.2} 129 l ${209 + i * 4.2} 119 l S Q`) });   // (outlined lettering: a row of small strokes under the charm, read by no text operator)
    st.agentResults.labelRead = () => ({ reads: Array.from({ length: 6 }, (_, i) => ({ index: i, sku: 'VIS-ONE-' + i, size: null, confidence: 0.99 })) });   // (the model is a stub: it reads a SKU named by the charm's number under whichever charm it is shown)
    r = await br.upload(outlined, 'OUTLINED.ai'); j = await job();
    const pending = await page.evaluate(() => { const j = [...B.master.jobs.values()].pop(); return (j.vision || []).filter(v => !v.confirmed).map(v => v.sku); });
    ok(pending.join() === 'VIS-ONE-1', 'the read waits for a person: ' + JSON.stringify(pending) + ' log ' + j.log.join(' | '));
    await page.evaluate(() => { const b = [...document.querySelectorAll('#mJobs button')].find(x => /Confirm checked/.test(x.textContent)); b.click(); });
    await page.waitForFunction(() => { const j = [...B.master.jobs.values()].pop(); return j && (j.vision || []).every(v => v.confirmed) && j.state === 'done'; }, null, { timeout: 60000 });
    ok(libSkus(st).join() === 'LABELLED_1,VIS-ONE-1' && st.doc('Charm_Master_Index', 'VIS-ONE-1').labelSource === 'vision', 'the confirmed read is in the library: ' + libSkus(st));
    ok(st.list('Charm_Master_Files')[0].skus.slice().sort().join() === 'LABELLED_1,VIS-ONE-1', 'and the file records both');
    
    // 14 · SKUs the server cleans to one file name keep a file each
    await wipe();
    const clash2 = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 30, y: 150, w: 40, h: 40, ink: A }, { shape: 'blob', x: 110, y: 150, w: 40, h: 40, ink: B }, { shape: 'notch', x: 190, y: 150, w: 40, h: 40, ink: C }], labels: [{ text: 'A+B_1', cx: 50, top: 149 }, { text: 'A B_1', cx: 130, top: 149 }, { text: 'A&B_1', cx: 210, top: 149 }] });
    r = await br.upload(clash2, 'CLASH2.ai'); j = await job();
    const paths = st.list('Charm_Master_Index').map(e => e.aiPath);
    ok(j.state === 'done' && libSkus(st).length === 3 && new Set(paths).size === 3 && paths.every(pth => st.blobs.has(pth)), 'three SKUs the server cleans to one name keep a design file each: ' + JSON.stringify(paths));
    r = await br.upload(clash2, 'CLASH2.ai'); j = await job(); ok(j.written === 0 && j.held === 3, 'and the same file again builds nothing');

    // 15 · a file with more pages says that only the first is read
    await wipe();
    const pages = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ text: 'PAGE_ONE_1', cx: 82.5, top: 149 }], extraPages: 2 });
    r = await br.upload(pages, 'PAGES.ai'); j = await job();
    ok(j.state === 'done' && j.log.some(l => /3 pages and only the FIRST is read/.test(l)), 'a file with three pages says that only the first is read: ' + j.log.join(' | '));

    // 16 · a second drop with everything held traces nothing
    await wipe(); await br.upload(sheet, 'PAIR.ai');
    await page.evaluate(() => { window.__traced = 0; const o = CharmNestPDF.buildSilhouettes; CharmNestPDF.buildSilhouettes = function (p, cs, ...rest) { window.__traced += cs.length; return o.call(this, p, cs, ...rest); }; });
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(j.state === 'done' && j.written === 0 && (await page.evaluate(() => window.__traced)) === 0, 'a sheet added again with everything held traces no charm: ' + (await page.evaluate(() => window.__traced)));
    await page.evaluate(() => { delete window.__traced; });

    // 17 · Claude's grouping review: a labelled charm it merges or excludes does not take its SKU with it unseen, and a sheet with an unlabelled charm does not crash it
    await wipe(); await page.evaluate(() => { CN.S.settings.review = 'on'; });
    const three = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 30, y: 150, w: 40, h: 40, ink: A }, { shape: 'blob', x: 110, y: 150, w: 40, h: 40, ink: B }, { shape: 'notch', x: 190, y: 150, w: 40, h: 40, ink: C }], labels: [{ text: 'FIRST_1', cx: 50, top: 149 }, { text: 'SECOND_2', cx: 130, top: 149 }, { text: 'THIRD_3', cx: 210, top: 149 }] });
    st.agentResults.grouping = () => ({ summary: 'test', realCharmCount: 2, verdicts: [{ index: 0, verdict: 'complete', note: '' }, { index: 1, verdict: 'complete', note: '' }, { index: 2, verdict: 'fragment', mergeInto: 1, note: 'half of the second' }] });
    r = await br.upload(three, 'THREE.ai'); j = await job();
    ok(j.state === 'done' && !j.log.some(l => /Cannot read properties/.test(l)), 'the review does not crash: ' + j.log.join(' | '));
    ok(libSkus(st).join() === 'FIRST_1,SECOND_2' && j.log.some(l => /THIRD_3 was labelled under charm #2/.test(l)) && r.orphans.includes('THIRD_3'), 'THIRD_3 merged into a charm that has a SKU is named, with no charm above it: ' + JSON.stringify({ lib: libSkus(st), orphans: r.orphans }));
    await wipe();
    const half = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 30, y: 150, w: 40, h: 40, ink: A }, { shape: 'blob', x: 110, y: 150, w: 40, h: 40, ink: B }], labels: [{ text: 'FIRST_1', cx: 50, top: 149 }] });
    st.agentResults.grouping = () => ({ summary: 'test', realCharmCount: 1, verdicts: [{ index: 0, verdict: 'fragment', mergeInto: 1, note: 'the labelled one is a fragment of the unlabelled one' }, { index: 1, verdict: 'complete', note: '' }] });
    r = await br.upload(half, 'HALF.ai'); j = await job();
    ok(j.state === 'done' && libSkus(st).join() === 'FIRST_1' && j.log.some(l => /merged the charm labelled FIRST_1 into #1/.test(l)), 'a labelled fragment merged into an unlabelled charm: the SKU goes with it: ' + j.log.join(' | '));
    await page.evaluate(() => { CN.S.settings.review = 'off'; });

    // 18 · text left under a labelled charm that is no part of its SKU is said
    await wipe();
    const stacked = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 60, y: 150, w: 45, h: 45, ink: A }], labels: [{ text: 'Spider /', cx: 82.5, top: 149 }, { text: 'Web_1', cx: 82.5, top: 141 }] });
    r = await br.upload(stacked, 'STACKED.ai'); j = await job();
    ok(libSkus(st).join() === 'WEB_1' && j.log.some(l => /left out: “Spider \/” \(under WEB_1\)/.test(l)), 'a half label that could not be joined is said, not dropped in silence: ' + j.log.join(' | '));
    
    // 19 · the loose disc in a ring's window is drawn into the design, which stays one body, hoop-up (CIRCLE_4432's plug, Paul 15 Oct)
    await wipe();
    const kk = 0.5523, circ = (cx, cy, r) => `${cx + r} ${cy} m ${cx + r} ${cy + kk * r} ${cx + kk * r} ${cy + r} ${cx} ${cy + r} c ${cx - kk * r} ${cy + r} ${cx - r} ${cy + kk * r} ${cx - r} ${cy} c ${cx - r} ${cy - kk * r} ${cx - kk * r} ${cy - r} ${cx} ${cy - r} c ${cx + kk * r} ${cy - r} ${cx + r} ${cy - kk * r} ${cx + r} ${cy} c h`;
    const ring = [`q 0.05 0.05 0.05 RG 0.5 w ${circ(100, 170, 22)} ${circ(100, 170, 17)} S Q`, `q 0.05 0.05 0.05 RG 0.5 w ${circ(96, 170, 8)} 93 167 6 6 re S Q`];
    const ringSheet = await scene({ page: [420, 300], bodies: [{ shape: 'house', x: 300, y: 150, w: 45, h: 45, ink: C }], extraOps: ring, labels: [{ text: 'CIRCLE_T_1', cx: 100, top: 141 }, { text: 'CONTROL_1', cx: 322.5, top: 146 }] });
    r = await br.upload(ringSheet, 'RING.ai'); j = await job();
    const ce = st.doc('Charm_Master_Index', 'CIRCLE_T_1');
    ok(j.state === 'done' && ce && ce.members === 2 && !ce.pair && Math.round(ce.upAngle) === 90, 'the disc in the ring\'s window is drawn into the design, which is one body, hoop-up: ' + JSON.stringify(ce && { m: ce.members, up: ce.upAngle, pair: ce.pair }) + ' ' + j.log.join(' | '));
    ok(j.unlabelled.length === 0, 'and it is not a charm left without a SKU');
    { const { CharmNestPDF: PDF } = require('../../netlify/functions/_charmNestPdf.js'); const pr = await PDF.parseSource(new Uint8Array(st.blobs.get(ce.aiPath).buf), 'x.ai'); ok(PDF.groupCharms(pr, { minPt: 6 }).charms.length === 2, 'the design file holds the plate and its disc'); }

    // 20 · designs this file's earlier indexing wrote with an older reader are built again; another master's are left alone
    await wipe(); await br.upload(sheet, 'PAIR.ai');
    for (const e of st.list('Charm_Master_Index')) { const d = st.doc('Charm_Master_Index', e._id); delete d.readerV; }
    await page.evaluate(() => Master.load(true));
    r = await br.upload(sheet, 'PAIR.ai'); j = await job();
    ok(j.state === 'done' && j.written === 2 && j.log.some(l => /older reader and are built again/.test(l)) && st.list('Charm_Master_Index').every(e => e.readerV >= 2), 'both SKUs an older reader wrote are rebuilt by the next drop of their file: ' + JSON.stringify({ w: j.written, v: st.list('Charm_Master_Index').map(e => e.readerV) }));
    r = await br.upload(sheet, 'PAIR.ai'); j = await job(); ok(j.written === 0 && j.held === 2, 'and the drop after that builds nothing');
    for (const e of st.list('Charm_Master_Index')) { const d = st.doc('Charm_Master_Index', e._id); delete d.readerV; d.masterHash = 'ffffffffeeeeeeee'; d.masterName = 'OTHER.ai'; }
    await page.evaluate(() => Master.load(true));
    r = await br.upload(sheet, 'PAIR.ai'); j = await job(); ok(j.written === 0 && j.held === 2, 'a design another master file wrote with an older reader is left as it is: ' + JSON.stringify({ w: j.written, h: j.held }));
    
    // 21 · a page that is out of date reads and writes nothing (it would use the old reader), and says why
    await wipe();
    await page.evaluate(() => { PageVersion._set('"v1"'); });
    const headRoute = async route => { if (route.request().method() === 'HEAD') return route.fulfill({ status: 200, headers: { etag: '"v2"' }, body: '' }); return route.fallback(); };
    await page.route(/charm-nest-1\.html/, headRoute);
    const jobsBefore = await page.evaluate(() => B.master.jobs.size), putsOf2 = () => st.calls.filter(c => c.body && /^master(PutIndex|PutFile)$/.test(c.body.op || '')).length, putsBefore2 = putsOf2();
    { const file = require('path').join(require('os').tmpdir(), 'stale-' + Date.now() + '.ai'); require('fs').writeFileSync(file, sheet); await page.setInputFiles('#mFile', file); }
    await page.waitForFunction(() => PageVersion.isStale(), null, { timeout: 15000 });
    ok((await page.evaluate(() => B.master.jobs.size)) === jobsBefore && putsOf2() === putsBefore2 && libSkus(st).length === 0, 'an out-of-date page reads and writes nothing');
    ok(await page.evaluate(() => /reload the page/i.test(document.body.innerText)), 'and tells the person to reload');
    await page.unroute(/charm-nest-1\.html/, headRoute);
    await page.evaluate(() => { PageVersion._set(null); });
        const bad = br.errors.filter(e => !/status of 500/.test(e));   // (the one failure this test forces is logged by the browser)
    ok(bad.length === 0, 'no page error: ' + bad.join(' | '));
  } finally { await br.close(); }
  console.log(`master-tab-resume: ${n} checks passed`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
